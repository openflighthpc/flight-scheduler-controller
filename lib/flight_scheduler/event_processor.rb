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
  class << self
    attr_reader :env_var_prefix
    attr_reader :cluster_name
  end

  def batch_job_created(job)
    FlightScheduler.app.scheduler.add_job(job)
    allocate_resources_and_run_jobs
  end
  module_function :batch_job_created

  def node_connected
    allocate_resources_and_run_jobs
  end
  module_function :node_connected

  def cancel_job(job)
    case job.state

    when 'PENDING'
      Async.logger.info("Cancelling pending job #{job.id}")
      job.state = 'CANCELLED'

    when 'RUNNING'
      # For running jobs we still need to kill any processes on any nodes.
      case job.job_type
      when 'ARRAY_JOB'
        Async.logger.info("Cancelling running array job #{job.id}")
        FlightScheduler::Cancellation::ArrayJob.new(job).call
      when 'ARRAY_TASK'
        # XXX Do we really want to cancel the entire array job when cancelling
        # an array task?
        Async.logger.info("Cancelling running array job #{job.array_job.id}")
        FlightScheduler::Cancellation::ArrayJob.new(job.array_job).call
      else
        Async.logger.info("Cancelling running job #{job.id}")
        FlightScheduler::Cancellation::BatchJob.new(job).call
      end

    else
      Async.logger.info("Not cancelling #{job.state} job #{job.id}")

    end
  ensure
    FlightScheduler.app.scheduler.remove_job(job)
  end
  module_function :cancel_job

  def allocate_resources_and_run_jobs
    Async.logger.info("Attempting to allocate rescources to jobs")
    Async.logger.debug("Queued jobs #{FlightScheduler.app.scheduler.queue.map(&:id)}")
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
      if allocation.job.job_type == 'ARRAY_JOB'
        FlightScheduler::Submission::ArrayTask.new(allocation).call
      else
        FlightScheduler::Submission::BatchJob.new(allocation).call
      end
    end
  end
  module_function :allocate_resources_and_run_jobs

  def node_completed_job(node_name, job_id)
    Async.logger.info("Node #{node_name} completed job #{job_id}")
    allocation = FlightScheduler.app.allocations.for_job(job_id)
    allocation.job.state = 'COMPLETED'
    FlightScheduler.app.scheduler.remove_job(allocation.job)
    FlightScheduler.app.allocations.delete(allocation)
    allocate_resources_and_run_jobs
  end
  module_function :node_completed_job

  def node_failed_job(node_name, job_id)
    Async.logger.info("Node #{node_name} failed job #{job_id}")
    allocation = FlightScheduler.app.allocations.for_job(job_id)
    if allocation.job.state == 'CANCELLING'
      allocation.job.state = 'CANCELLED'
    else
      allocation.job.state = 'FAILED'
    end
    FlightScheduler.app.scheduler.remove_job(allocation.job)
    FlightScheduler.app.allocations.delete(allocation)
    allocate_resources_and_run_jobs
  end
  module_function :node_failed_job

  def node_completed_task(node_name, task_id, job_id)
    allocation = FlightScheduler.app.allocations.for_job(job_id)
    job = allocation.job
    task = job.array_tasks.detect { |task| task.id == task_id }
    Async.logger.info("Node #{node_name} completed task #{task.array_index} for job #{job_id}")
    task.state = 'COMPLETED'
    if job.array_tasks.any?(&:pending?) && !(job.cancelled? || job.cancelling?)
      Async.logger.info("Running next task in array")
      FlightScheduler::Submission::ArrayTask.new(allocation).call
    else
      job.state =
        if job.cancelled? || job.cancelling?
          'CANCELLED'
        elsif job.array_tasks.any?(&:failed?)
          'FAILED'
        else
          'COMPLETED'
        end
      FlightScheduler.app.scheduler.remove_job(job)
      FlightScheduler.app.allocations.delete(allocation)
      allocate_resources_and_run_jobs
    end
  end
  module_function :node_completed_task

  def node_failed_task(node_name, task_id, job_id)
    allocation = FlightScheduler.app.allocations.for_job(job_id)
    job = allocation.job
    task = job.array_tasks.detect { |task| task.id == task_id }
    Async.logger.info("Node #{node_name} failed task #{task.array_index} for job #{job_id}")
    task.state = task.cancelling? ? 'CANCELLED' : 'FAILED'
    if job.array_tasks.any?(&:pending?) && !(job.cancelled? || job.cancelling?)
      FlightScheduler::Submission::ArrayTask.new(allocation).call
    else
      job.state =
        if job.cancelled? || job.cancelling?
          'CANCELLED'
        elsif job.array_tasks.any?(&:failed?)
          'FAILED'
        else
          'COMPLETED'
        end
      FlightScheduler.app.scheduler.remove_job(job)
      FlightScheduler.app.allocations.delete(allocation)
      allocate_resources_and_run_jobs
    end
  end
  module_function :node_failed_task
end
