#==============================================================================
# Copyright (C) 2021-present Alces Flight Ltd.
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

# Plugin providing core functionality.
#
# * Maintains the domain model.
# * Orchestrates communication with daemon/jobd/stepd.
#
# It is expected that this plugin will not have alternatives, although it may
# change overtime.
class Core
  def self.plugin_name
    'core/builtin'
  end

  class EventProcessor
    def daemon_connected(node, _)
      # Ensure existing terminal allocations are removed
      stale_jobs = FlightScheduler.app.job_registry.jobs.select do |job|
        next false unless job.terminal_state?
        alloc = FlightScheduler.app.allocations.for_job(job.id)
        next false unless alloc
        alloc.nodes.map(&:name).include?(node.name)
      end
      daemon_processor = FlightScheduler.app.connection_registry.daemon_processor_for(node.name)
      stale_jobs.each do |j|
        daemon_processor.send_job_deallocated(j.id)
      end
    end

    def job_created(job)
      FlightScheduler.app.job_registry.add(job)
    end

    def job_cancelled(job)
      case job.job_type
      when 'JOB'
        batch_job_cancelled(job)
      when 'ARRAY_JOB'
        array_job_cancelled(job)
      else
        Async.logger.error("[core] cannot cancel #{job.job_type} #{job.display_id}")
      end
    end

    def job_timed_out(job)
      job.state = job.terminal_state? ? 'TIMEOUT' : 'TIMINGOUT'
    end

    def resources_allocated(new_allocations)
      new_allocations.each do |allocation|
        FlightScheduler::Submission::Job.new(allocation).call
      end
    end

    def resource_deallocated(job, node_name)
      allocation = FlightScheduler.app.allocations
        .deallocate_node_from_job(job.id, node_name)
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
    end

    # The primary node allocated to the job has completed the job.
    def node_completed_job(job, node_name)
      job.state = 'COMPLETED'
      FlightScheduler::Deallocation::Job.new(job).call
    end

    def node_failed_job(job, node_name)
      if job.state == 'CANCELLING'
        job.state = 'CANCELLED'
      elsif job.state == 'TIMINGOUT'
        job.state = 'TIMEOUT'
      else
        job.state = 'FAILED'
      end
      FlightScheduler::Deallocation::Job.new(job).call
    end

    def jobd_connected(job, node_name)
      if should_send_batch_script?(job, node_name)
        Async.logger.debug(
          "[core] sending batch script for job #{job.display_id} to #{node_name}"
        )
        send_batch_script(job, node_name)
      end
      unless job.terminal_state?
        job.state == 'RUNNING'
      end
    end

    def job_step_created(job_step)
      FlightScheduler::Submission::JobStep.new(job_step).call
    end

    def job_step_started(job, job_step, node_name, port)
      execution = job_step.execution_for(node_name)
      execution.state = 'RUNNING'
      execution.port = port
    end

    def job_step_completed(job, job_step, node_name)
      execution = job_step.execution_for(node_name)
      execution.state = 'COMPLETED'
    end

    def job_step_failed(job, job_step, node_name)
      execution = job_step.execution_for(node_name)
      execution.state = 'FAILED'
    end

    private

    def batch_job_cancelled(job)
      case job.state
      when 'PENDING'
        Async.logger.info("[core] cancelling pending job #{job.display_id}")
        job.state = 'CANCELLED'
      when 'CONFIGURING', 'RUNNING', 'CANCELLING'
        Async.logger.info("[core] cancelling running job #{job.display_id}")
        FlightScheduler::Cancellation::Job.new(job).call
      else
        Async.logger.info("[core] not cancelling #{job.state} job #{job.display_id}")
      end
    end

    def array_job_cancelled(job)
      if job.running_tasks.empty?
        Async.logger.info("[core] cancelling pending job #{job.display_id}")
        job.state = 'CANCELLED'
      else
        Async.logger.info("[core] cancelling running array tasks for #{job.display_id}")
        FlightScheduler::Cancellation::ArrayJob.new(job).call
      end
    end

    # Return true if the job's batch script should be sent to node_name.
    def should_send_batch_script?(job, node_name)
      return false unless job.has_batch_script?
      primary_node = FlightScheduler.app.allocations.for_job(job.id)&.nodes&.first
      primary_node.name == node_name
    end

    # Send the batch script to the given node
    def send_batch_script(job, node_name)
      node = FlightScheduler.app.nodes[node_name]
      pg = FlightScheduler::PathGenerator.build(node, job)
      script = job.batch_script
      jobd_processor = FlightScheduler.app.connection_registry.job_processor_for(node.name, job.id)
      jobd_processor.send_run_script(
        script,
        pg.render(script.stdout_path),
        pg.render(script.stderr_path),
      )
    end
  end

  def event_processor
    @event_processor ||= EventProcessor.new
  end
end
