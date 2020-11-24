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

module FlightScheduler::EventProcessor
  def job_created(job)
    FlightScheduler.app.job_registry.add(job)
    allocate_resources_and_run_jobs
  end
  module_function :job_created

  def job_step_created(job_step)
    Async.logger.info("Created step for job #{job_step.job.id}")
    FlightScheduler::Submission::JobStep.new(job_step).call
  end
  module_function :job_step_created

  def node_connected
    allocate_resources_and_run_jobs
  end
  module_function :node_connected

  def allocate_resources_and_run_jobs
    Async.logger.info("Attempting to allocate rescources to jobs")
    Async.logger.debug("Queued jobs #{FlightScheduler.app.job_registry.jobs.map(&:id)}")
    Async.logger.debug(
      "Allocated jobs #{FlightScheduler.app.allocations.each.map{|a| a.job.id}}"
    )
    Async.logger.debug(
      "Connected nodes #{FlightScheduler.app.daemon_connections.connected_nodes}"
    )
    Async.logger.debug(
      "Allocated nodes #{FlightScheduler.app.allocations.each.map{|a| a.nodes.map(&:name)}.flatten.sort.uniq}"
    )
    new_allocations = FlightScheduler.app.scheduler.allocate_jobs
    new_allocations.each do |allocation|
      allocated_node_names = allocation.nodes.map(&:name).join(',')
      Async.logger.info("Allocated #{allocated_node_names} to job #{allocation.job.id}")

      case allocation.job.job_type
      when 'ARRAY_TASK'
        FlightScheduler::Submission::ArrayTask.new(allocation).call
      when 'JOB'
        FlightScheduler::Submission::BatchJob.new(allocation).call
      else
        # The ARRAY_JOB can not be started, this condition should never be reached
      end
    end
  end
  module_function :allocate_resources_and_run_jobs

  def node_completed_job(node_name, job_id)
    Async.logger.info("Node #{node_name} completed job #{job_id}")
    job = FlightScheduler.app.job_registry.lookup(job_id)
    return unless job
    job.state = 'COMPLETED'
    FlightScheduler::Deallocation::Job.new(job).call
  end
  module_function :node_completed_job

  def node_failed_job(node_name, job_id)
    Async.logger.info("Node #{node_name} failed job #{job_id}")
    job = FlightScheduler.app.job_registry.lookup(job_id)
    return if job.nil?

    if job.state == 'CANCELLING'
      job.state = 'CANCELLED'
    else
      job.state = 'FAILED'
    end
    FlightScheduler::Deallocation::Job.new(job).call
  end
  module_function :node_failed_job

  def node_completed_task(node_name, task_id, job_id)
    task = FlightScheduler.app.job_registry.lookup(task_id)
    return unless task

    Async.logger.info("Node #{node_name} completed task #{task.array_index} for job #{job_id}")
    task.state = 'COMPLETED'
    task.array_job.update_array_job_state
    FlightScheduler::Deallocation::Job.new(task).call
  end
  module_function :node_completed_task

  def node_failed_task(node_name, task_id, job_id)
    task = FlightScheduler.app.job_registry.lookup(task_id)
    return if task.nil?

    Async.logger.info("Node #{node_name} failed task #{task.array_index} for job #{job_id}")
    task.state = task.cancelling? ? 'CANCELLED' : 'FAILED'
    task.array_job.update_array_job_state
    FlightScheduler::Deallocation::Job.new(task).call
  end
  module_function :node_failed_task

  def node_deallocated(node_name, job_id)
    # NOTE: There maybe duplicate deallocation requests so the allocation
    #       may not exist.
    allocation = FlightScheduler.app.allocations.for_job(job_id)
    return unless allocation

    allocation.nodes.delete_if { |n| n.name == node_name }
    FlightScheduler.app.allocations.delete(allocation) if allocation.nodes.empty?
    allocate_resources_and_run_jobs
  end
  module_function :node_deallocated

  def job_step_started(node_name, job_id, step_id, port)
    Async.logger.info("Node #{node_name} started step:#{step_id} for job #{job_id}")
    job = FlightScheduler.app.job_registry.lookup(job_id)
    job_step = job.job_steps.detect { |step| step.id == step_id }
    execution = job_step.execution_for(node_name)
    execution.state = 'STARTED'
    execution.port = port
  end
  module_function :job_step_started

  def job_step_completed(node_name, job_id, step_id)
    Async.logger.info("Node #{node_name} completed step:#{step_id} for job #{job_id}")
    job = FlightScheduler.app.job_registry.lookup(job_id)
    job_step = job.job_steps.detect { |step| step.id == step_id }
    execution = job_step.execution_for(node_name)
    execution.state = 'COMPLETED'
  end
  module_function :job_step_completed

  def job_step_failed(node_name, job_id, step_id)
    Async.logger.info("Node #{node_name} failed step:#{step_id} for job #{job_id}")
    job = FlightScheduler.app.job_registry.lookup(job_id)
    job_step = job.job_steps.detect { |step| step.id == step_id }
    execution = job_step.execution_for(node_name)
    execution.state = 'FAILED'
  end
  module_function :job_step_failed

  def cancel_job(job)
    case job.job_type
    when 'JOB'
      cancel_batch_job(job)
    when 'ARRAY_JOB'
      cancel_array_job(job)
    else
      Async.logger.error("Cannot cancel #{job.job_type} #{job.id}")
    end
  end
  module_function :cancel_job

  # TODO: Which one is required for module_function?
  private
  private_class_method

  def cancel_batch_job(job)
    case job.state
    when 'PENDING'
      Async.logger.info("Cancelling pending job #{job.id}")
      job.state = 'CANCELLED'
    when 'RUNNING'
      Async.logger.info("Cancelling running job #{job.id}")
      FlightScheduler::Cancellation::BatchJob.new(job).call
    else
      Async.logger.info("Not cancelling #{job.state} job #{job.id}")
    end
  end
  module_function :cancel_batch_job

  def cancel_array_job(job)
    Async.logger.info("Cancelling running array jobs for #{job.id}")
    FlightScheduler::Cancellation::ArrayJob.new(job).call
  end
  module_function :cancel_array_job
end
