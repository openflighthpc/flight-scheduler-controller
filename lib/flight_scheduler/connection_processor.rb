#==============================================================================
# Copyright (C) 2020-present Alces Flight Ltd.
#
# This file is part of FlightSchedulerController.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License 2.0 which is available at
# <https://www.eclipse.org/legal/epl-2.0>, or alternative license
# terms made available by Alces Flight Ltd - please direct inquiries
# about licensing to licensing@alces-flight.com.
#
# FlightSchedulerController is distributed in the hope that it will be useful, but
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR
# IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR CONDITIONS
# OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A
# PARTICULAR PURPOSE. See the Eclipse Public License 2.0 for more
# details.
#
# You should have received a copy of the Eclipse Public License 2.0
# along with FlightSchedulerController. If not, see:
#
#  https://opensource.org/licenses/EPL-2.0
#
# For more information on FlightSchedulerController, please visit:
# https://github.com/openflighthpc/flight-scheduler-controller
#==============================================================================

module FlightScheduler::ConnectionProcessor
  module Helper
    def process(message)
      Async.logger.error("Unrecognised message #{message[:command]}: #{tag_line}")
    end

    def tag_line
      'unknown'
    end

    def event_processor
      FlightScheduler.app.processors.event
    end
  end

  def self.process(connection)
    message = connection.read
    unless message.is_a? Hash
      Async.logger.error "Received connection with non-hash body"
      return
    end

    node_name = FlightScheduler::Auth.node_from_token(message[:auth_token])

    processor =
      case message[:command]
      when 'CONNECTED'
        DaemonProcessor.new(connection, node_name)

      when 'JOBD_CONNECTED'
        unless message[:job_id]
          Async.logger.error("JOBD_CONNECTED from #{node_name} missing 'job_id'")
          return
        end
        JobProcessor.new(connection, node_name, message[:job_id])

      when 'STEPD_CONNECTED'
        unless message[:job_id]
          Async.logger.error("STEPD_CONNECTED from #{node_name} missing 'job_id'")
          return
        end
        unless message[:step_id]
          Async.logger.error("STEPD_CONNECTED from #{node_name} missing 'step_id'")
          return
        end
        StepProcessor.new(connection, node_name, message[:job_id], message[:step_id])

      else
        Async.logger.error("Unrecognised connection message: #{message[:command]}")
        return
      end

    Async do |task|
      task.yield # Yield to allow the older processor to be cleared on reconnects
      FlightScheduler.app.processors.add(processor)

      # Start processing messages
      begin
        Async.logger.info("#{processor.tag_line} -> #{message[:command]}")
        processor.process(message)
        while message = connection.read
          Async.logger.info("#{processor.tag_line} -> #{message[:command]}")
          processor.process(message)
          Async.logger.debug("#{processor.tag_line} processed #{message[:command]}")
        end
      ensure
        FlightScheduler.app.processors.remove(processor)
        Async.logger.info("#{processor.tag_line} disconnected")
      end
    end.wait
  rescue FlightScheduler::Auth::AuthenticationError
    Async.logger.info("Could not authenticate connection: #{$!.message}")
  end

  DaemonProcessor = Struct.new(:connection, :node_name) do
    include Helper

    def tag_line
      "[daemon:#{node_name}]"
    end

    def process(message)
      msg = message[:command]
      case msg
      when 'CONNECTED'
        connected(message)
      when 'NODE_DEALLOCATED'
        dispatch_event(:resource_deallocated, message[:job_id])
      else
        super
      end
    end

    def send_job_allocated(job_id, environment:, username:, time_limit:)
      connection.write(
        command: 'JOB_ALLOCATED',
        job_id: job_id,
        environment: environment,
        username: username,
        time_limit: time_limit
      )
      connection.flush
      Async.logger.info("#{tag_line} <- JOB_ALLOCATED:#{job_id}")
      Async.logger.debug { {time_limit: time_limit, username: username} }
    end

    # NOTE: This maybe better suited on JobProcessor but this would require changing
    # the relationship between daemon-jobd/batchd to:
    # 1. Send message to jobd/batchd to trigger a graceful shutdown,
    # 2. The daemon detects the shutdown and responds NODE_DEALLOCATED implicitly
    # X. What happens if it is in a state that can't be shutdown? (e.g. steps running)
    def send_job_deallocated(job_id)
      connection.write(
        command: 'JOB_DEALLOCATED',
        job_id: job_id
      )
      connection.flush
      Async.logger.info("#{tag_line} <- JOB_DEALLOCATED:#{job_id}")
    end

    private

    def dispatch_event(event, *args)
      event_processor.send(event, node_name, *args)
    end

    def connected(message)
      node = FlightScheduler.app.nodes[node_name] ||
        FlightScheduler.app.nodes.register_node(node_name)
      event_processor.daemon_connected(node, message)
    end
  end

  JobProcessor = Struct.new(:connection, :node_name, :job_id) do
    include Helper

    def process(message)
      case message[:command]
      when 'JOBD_CONNECTED'
        jobd_connected unless message[:reconnect]
      when 'NODE_COMPLETED_JOB'
        dispatch_event(:node_completed_job)
      when 'NODE_FAILED_JOB'
        dispatch_event(:node_failed_job)
      when 'JOB_TIMED_OUT'
        dispatch_event(:job_timed_out)
      else
        super
      end
    end

    def tag_line
      "[jobd:#{node_name}:#{job_id}]"
    end

    def send_run_script(script, stdout_path, stderr_path)
      connection.write({
        command: 'RUN_SCRIPT',
        arguments: script.arguments,
        script: script.content,
        stdout_path: stdout_path,
        stderr_path: stderr_path,
      })
      connection.flush
      Async.logger.info("#{tag_line} <- RUN_SCRIPT")
    end

    def send_run_step(arguments:, path:, pty:, step_id:, environment:)
      connection.write({
        command: 'RUN_STEP',
        arguments: arguments,
        path: path,
        pty: pty,
        step_id: step_id,
        environment: environment
      })
      connection.flush
      Async.logger.info("#{tag_line} <- RUN_STEP:#{step_id}")
      Async.logger.debug { {path: path, arguments: arguments, pty: pty} }
    end

    def send_job_cancelled
      connection.write({ command: 'JOB_CANCELLED' })
      connection.flush
      Async.logger.info("#{tag_line} <- JOB_CANCELLED")
    end

    private

    def dispatch_event(event, *args)
      job = FlightScheduler.app.job_registry.lookup(job_id)
      if job.nil?
        # XXX: Should there be a termination protocol for unknown jobs?
        Async.logger.error("#{processor.tag_line} unable to find job:#{job_id}")
        return
      end
      event_processor.send(event, job, node_name, *args)
    end

    def jobd_connected
      job = FlightScheduler.app.job_registry.lookup(job_id)
      if job.nil?
        # XXX: Should there be a termination protocol for unknown jobs?
        Async.logger.error("#{processor.tag_line} unable to find job:#{job_id}")
        return
      end
      event_processor.jobd_connected(job, node_name)
    end
  end

  StepProcessor = Struct.new(:connection, :node_name, :job_id, :step_id) do
    include Helper

    def tag_line
      "[stepd:#{node_name}:#{job_id}.#{step_id}]"
    end

    def process(message)
      case message[:command]
      when 'STEPD_CONNECTED'
        # NOOP
      when 'RUN_STEP_STARTED'
        dispatch_event(:job_step_started, message[:port])
      when 'RUN_STEP_COMPLETED'
        dispatch_event(:job_step_completed)
      when 'RUN_STEP_FAILED'
        dispatch_event(:job_step_failed)
      else
        super
      end
    end

    private

    def dispatch_event(event, *args)
      job = FlightScheduler.app.job_registry.lookup(job_id)
      if job.nil?
        # XXX: Should there be a termination protocol for unknown jobs?
        Async.logger.error("#{processor.tag_line} unable to find job:#{job_id}")
        return
      end
      job_step = job.job_steps.detect { |step| step.id == step_id }
      if job_step.nil?
        # XXX: Should there be a termination protocol for unknown job steps?
        Async.logger.error("#{processor.tag_line} unable to find job_step:#{step_id}")
        return
      end
      event_processor.send(event, job, job_step, node_name, *args)
    end
  end
end
