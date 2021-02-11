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
  end

  def self.process(connection)
    message = connection.read
    unless message.is_a? Hash
      Async.logger.error "Received a websocket connection with a non-hash body"
      return
    end

    node_name = FlightScheduler::Auth.node_from_token(message[:auth_token])

    processor = case message[:command]
    when 'CONNECTED'
      DaemonProcessor.new(connection, node_name)

    when 'JOBD_CONNECTED'
      unless message[:job_id]
        Async.logger.error("JOBD_CONNECTED from #{node_name} is missing its 'job_id'")
        return
      end
      JobProcessor.new(connection, node_name, message[:job_id])

    when 'STEPD_CONNECTED'
      unless message[:job_id]
        Async.logger.error("STEPD_CONNECTED from #{node_name} is missing its 'job_id'")
        return
      end
      unless message[:step_id]
        Async.logger.error("STEPD_CONNECTED from #{node_name} is missing its 'step_id'")
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
        Async.logger.debug("CONNECTING: #{processor.tag_line}")
        processor.process(message)
        Async.logger.info("CONNECTED: #{processor.tag_line}")
        while message = connection.read
          processor.process(message)
          Async.logger.info("Processed #{message[:command]}: #{processor.tag_line}")
        end
      ensure
        FlightScheduler.app.processors.remove(processor)
        Async.logger.info("DISCONNECTED: #{processor.tag_line}")
      end
    end.wait
  rescue FlightScheduler::Auth::AuthenticationError
    Async.logger.info("Could not authenticate connection: #{$!.message}")
  end

  DaemonProcessor = Struct.new(:connection, :node_name) do
    include Helper

    def tag_line
      "#{node_name} daemon"
    end

    def process(message)
      msg = message[:command]
      case msg
      when 'CONNECTED'
        connected(message)
      when 'NODE_DEALLOCATED'
        node_deallocated(message[:job_id])
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
      Async.logger.info("Sent JOB_ALLOCATED: #{tag_line}")
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
      Async.logger.info("Sent JOB_DEALLOCATED: #{tag_line}")
    end

    private

    def connected(message)
      node = FlightScheduler.app.nodes[node_name] || FlightScheduler.app.nodes.register_node(node_name)

      # Update the nodes attributes
      attributes = node.attributes.dup
      attributes.cpus   = message[:cpus]    if message.key?(:cpus)
      attributes.gpus   = message[:gpus]    if message.key?(:gpus)
      attributes.memory = message[:memory]  if message.key?(:memory)
      attributes.type   = message[:type]    if message.key?(:type)

      if attributes == node.attributes
        Async.logger.debug("Unchanged '#{node_name}' attributes:") {
          attributes.to_h.map { |k, v| "#{k}: #{v}" }.join("\n")
        }
      elsif attributes.valid?
        Async.logger.info("Updating '#{node_name}' attributes:") {
          attributes.to_h.map { |k, v| "#{k}: #{v}" }.join("\n")
        }

        node.attributes = attributes
        FlightScheduler.app.nodes.update_partition_cache(node)
      else
        Async.logger.error <<~ERROR
          Invalid node attributes for #{node.name}:
          #{attributes.errors.messages}
        ERROR
      end

      Async.logger.debug("Connected nodes: #{FlightScheduler.app.processors.connected_nodes}")

      # Ensure existing terminal allocations are removed
      stale_jobs = FlightScheduler.app.job_registry.jobs.select do |job|
        next false unless job.terminal_state?
        alloc = FlightScheduler.app.allocations.for_job(job.id)
        next false unless alloc
        alloc.nodes.map(&:name).include?(node_name)
      end
      stale_jobs.each { |j| send_job_deallocated(j.id) }

      # Start the allocation loop
      FlightScheduler.app.processors.event.allocate_resources_and_run_jobs
    end

    def node_deallocated(job_id)
      # Remove the node from the job
      # The job's allocation will be remove implicitly if this was the last node
      job = FlightScheduler.app.job_registry[job_id]
      allocation = FlightScheduler.app.allocations
                                  .deallocate_node_from_job(job_id, node_name)
      return unless allocation

      if allocation.nodes.empty? && !job.terminal_state?
        # If the job is not in a terminal state, it has not been updated
        # following a `NODE_{COMPLETED,FAILED}_JOB` command.  It might have been
        # created without a batch script, i.e., created with the `alloc`
        # command.  Or the message might not have been sent or received.
        # Ideally, we'd capture the exit code of some command somewhere to be
        # able to set the FAILED state if appropriate.
        if job.state == 'CANCELLING'
          job.state = 'CANCELLED'
        elsif job.state == 'TIMINGOUT'
          job.state = 'TIMEOUT'
        else
          job.state = 'COMPLETED'
        end
      end
      FlightScheduler.app.processors.event.allocate_resources_and_run_jobs
    end
  end

  JobProcessor = Struct.new(:connection, :node_name, :job_id) do
    include Helper

    def process(message)
      case message[:command]
      when 'JOBD_CONNECTED'
        jobd_connected unless message[:reconnect]
      when 'NODE_COMPLETED_JOB'
        node_completed_job
      when 'NODE_FAILED_JOB'
        node_failed_job
      when 'JOB_TIMED_OUT'
        job_timed_out
      else
        super
      end
    end

    def tag_line
      "#{node_name} job (job: #{job_id})"
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
      Async.logger.info("Sent RUN_STEP: #{tag_line}")
    end

    def send_job_cancelled
      connection.write({ command: 'JOB_CANCELLED' })
      connection.flush
      Async.logger.info("Sent JOB_CANCELLED: #{tag_line}")
    end

    private

    def jobd_connected
      job = FlightScheduler.app.job_registry.lookup(job_id)
      # XXX: Should there be a termination protocol for unknown jobs?
      #      This likely effects all the request handlers
      return if job.nil?

      # Send the batch script to the primary node
      primary_name = FlightScheduler.app.allocations.for_job(job.id)&.nodes&.first&.name
      if job.has_batch_script? && primary_name == node_name
        Async.logger.debug("Sending batch script for job #{job.display_id} to #{node_name}")
        node = FlightScheduler.app.nodes[node_name]
        pg = FlightScheduler::PathGenerator.build(node, job)
        script = job.batch_script
        connection.write({
          command: 'RUN_SCRIPT',
          arguments: script.arguments,
          script: script.content,
          stdout_path: pg.render(script.stdout_path),
          stderr_path: pg.render(script.stderr_path)
        })
        connection.flush
        Async.logger.info("Sent RUN_SCRIPT: #{tag_line}")
      end

      # Flag the job as running
      unless job.terminal_state?
        job.state == 'RUNNING'
        FlightScheduler.app.persist_scheduler_state
      end
    end

    def node_completed_job
      job = FlightScheduler.app.job_registry.lookup(job_id)
      return if job.nil?

      Async.logger.info("Node #{node_name} completed job #{job.display_id}")
      job.state = 'COMPLETED'
      FlightScheduler::Deallocation::Job.new(job).call
    end

    def node_failed_job
      job = FlightScheduler.app.job_registry.lookup(job_id)
      return if job.nil?

      Async.logger.info("Node #{node_name} failed job #{job.display_id}")
      if job.state == 'CANCELLING'
        job.state = 'CANCELLED'
      elsif job.state == 'TIMINGOUT'
        job.state = 'TIMEOUT'
      else
        job.state = 'FAILED'
      end
      FlightScheduler::Deallocation::Job.new(job).call
    end

    def job_timed_out
      job = FlightScheduler.app.job_registry.lookup(job_id)
      return unless job
      job.state = job.terminal_state? ? 'TIMEOUT' : 'TIMINGOUT'
    end
  end

  StepProcessor = Struct.new(:connection, :node_name, :job_id, :step_id) do
    include Helper

    def tag_line
      "#{node_name} job (job: #{job_id} - step: #{step_id})"
    end

    def process(message)
      case message[:command]
      when 'STEPD_CONNECTED'
        # NOOP
      when 'RUN_STEP_STARTED'
        job_step_started(message[:port])
      when 'RUN_STEP_COMPLETED'
        job_step_completed
      when 'RUN_STEP_FAILED'
        job_step_failed
      else
        super
      end
    end

    private

    def job_step_started(port)
      job = FlightScheduler.app.job_registry.lookup(job_id)
      job_step = job.job_steps.detect { |step| step.id == step_id }
      Async.logger.info("Node #{node_name} started step #{job_step.display_id}")
      execution = job_step.execution_for(node_name)
      execution.state = 'RUNNING'
      execution.port = port
      FlightScheduler.app.persist_scheduler_state
    end

    def job_step_completed
      job = FlightScheduler.app.job_registry.lookup(job_id)
      job_step = job.job_steps.detect { |step| step.id == step_id }
      Async.logger.info("Node #{node_name} completed step #{job_step.display_id}")
      execution = job_step.execution_for(node_name)
      execution.state = 'COMPLETED'
      FlightScheduler.app.persist_scheduler_state
    end

    def job_step_failed(node_name, job_id, step_id)
      job = FlightScheduler.app.job_registry.lookup(job_id)
      job_step = job.job_steps.detect { |step| step.id == step_id }
      Async.logger.info("Node #{node_name} failed step #{job_step.display_id}")
      execution = job_step.execution_for(node_name)
      execution.state = 'FAILED'
      FlightScheduler.app.persist_scheduler_state
    end
  end
end
