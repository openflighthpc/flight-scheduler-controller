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
    Async.logger.info("Created job step #{job_step.display_id}")
    FlightScheduler::Submission::JobStep.new(job_step).call
  end
  module_function :job_step_created

  def allocate_resources_and_run_jobs
    Async.logger.info("Attempting to allocate rescources to jobs")
    Async.logger.debug("Queued jobs") {
      FlightScheduler.app.scheduler.queue.map do |job|
        attrs = %w(cpus_per_node gpus_per_node memory_per_node).reduce("") do |a, attr|
          a << " #{attr}=#{job.send(attr)}"
        end
        "#{job.display_id}:#{attrs} state=#{job.state}"
      end.join("\n")
    }
    Async.logger.debug("Allocated jobs") {
      FlightScheduler.app.allocations.each.map do |a|
        job = a.job
        attrs = %w(cpus_per_node gpus_per_node memory_per_node).reduce("") do |a, attr|
          a << " #{attr}=#{job.send(attr)}"
        end
        "#{job.display_id}:#{attrs}"
      end.join("\n")
    }
    Async.logger.debug("Connected nodes") {
      FlightScheduler.app.processors.connected_nodes.map do |name|
        node = FlightScheduler.app.nodes[name]
        attrs = %w(cpus gpus memory).reduce("") { |a, attr| a << " #{attr}=#{node.send(attr)}" }
        "#{node.name}:#{attrs}"
      end.join("\n")
    }
    Async.logger.debug("Allocated nodes") {
      allocated_nodes = FlightScheduler.app.allocations.each
        .map { |a| a.nodes }
        .flatten
        .sort_by(&:name)
        .uniq
      if allocated_nodes.empty?
        "None"
      else
        allocated_nodes.map do |node|
          "#{node.name}: #{FlightScheduler.app.allocations.debug_node_allocations(node.name)}"
        end.join("\n")
      end
    }
    new_allocations = FlightScheduler.app.scheduler.allocate_jobs
    new_allocations.each do |allocation|
      allocated_node_names = allocation.nodes.map(&:name).join(',')
      Async.logger.info("Allocated #{allocated_node_names} to job #{allocation.job.display_id}")
      FlightScheduler::Submission::Job.new(allocation).call
    end
    FlightScheduler.app.persist_scheduler_state
  end
  module_function :allocate_resources_and_run_jobs

  def cancel_job(job)
    case job.job_type
    when 'JOB'
      cancel_batch_job(job)
    when 'ARRAY_JOB'
      cancel_array_job(job)
    else
      Async.logger.error("Cannot cancel #{job.job_type} #{job.display_id}")
    end
  end
  module_function :cancel_job

  # TODO: Which one is required for module_function?
  private
  private_class_method

  def cancel_batch_job(job)
    case job.state
    when 'PENDING'
      Async.logger.info("Cancelling pending job #{job.display_id}")
      job.state = 'CANCELLED'
      allocate_resources_and_run_jobs
    when 'CONFIGURING', 'RUNNING', 'CANCELLING'
      Async.logger.info("Cancelling running job #{job.display_id}")
      FlightScheduler::Cancellation::Job.new(job).call
    else
      Async.logger.info("Not cancelling #{job.state} job #{job.display_id}")
    end
  end
  module_function :cancel_batch_job

  def cancel_array_job(job)
    if job.running_tasks.empty?
      Async.logger.info("Cancelling pending job #{job.display_id}")
      job.state = 'CANCELLED'
      allocate_resources_and_run_jobs
    else
      Async.logger.info("Cancelling running array jobs for #{job.display_id}")
      FlightScheduler::Cancellation::ArrayJob.new(job).call
    end
  end
  module_function :cancel_array_job
end
