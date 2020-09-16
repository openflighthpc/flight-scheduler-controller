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

  def batch_job_created(job)
    FlightScheduler.app.scheduler.add_job(job)
    allocate_resources_and_run_jobs
  end
  module_function :batch_job_created

  def resources_allocated(allocation)
  end
  module_function :resources_allocated

  def node_connected
    allocate_resources_and_run_jobs
  end
  module_function :node_connected


  def allocate_resources_and_run_jobs
    Async.logger.info("Attempting to allocate rescources to jobs")
    new_allocations = FlightScheduler.app.scheduler.allocate_jobs
    new_allocations.each do |allocation|
      allocated_nodes = allocation.nodes.map(&:name).join(',')
      Async.logger.info("Allocated #{allocated_nodes} to job #{allocation.job.id}")
      begin
        job = allocation.job
        job.state = 'running'
        allocation.nodes.each do |node|
          Async.logger.debug("Sending job #{job.id} to #{node.name}")
          processor = FlightScheduler.app.daemon_connections[node.name]
          if processor.nil?
            # The node has lost its connection since we allocated the job.  This
            # is unlikely but possible.
            # XXX What to do here?  We could:
            # 1. abort/cancel the job
            # 2. allow the job to run on fewer nodes than we thought
            # 3. something else?
          else
            processor.connection.write(['JOB_ALLOCATED', job.id, job.script] + job.arguments)
            processor.connection.flush
            Async.logger.debug("Sent job #{job.id} to #{node.name}")
          end
        end
      rescue
        # XXX What else to do here?
        # * Cancel the job on any nodes?
        # * Remove the allocation?
        # * Remove the job from the scheduler?
        # * More?
        Async.logger.warn("Error running job #{job_id}: #{$!.message}")
        job.state = 'failed'
      end
    end
  end
  module_function :allocate_resources_and_run_jobs

  def node_completed_job(node_name, job_id)
    Async.logger.info("Node #{node_name} completed job #{job_id}")
    allocation = FlightScheduler.app.allocations.for_job(job_id)
    if allocation.nodes.length > 1
      # XXX Handle allocations across multiple nodes better.
    else
      # The job has completed.
      allocation.job.state = 'completed'
      FlightScheduler.app.scheduler.remove_job(allocation.job)
      FlightScheduler.app.allocations.delete(allocation)
      allocate_resources_and_run_jobs
    end
  end
  module_function :node_completed_job
end
