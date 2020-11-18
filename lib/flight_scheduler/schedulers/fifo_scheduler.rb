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

class FifoScheduler
  FlightScheduler.app.schedulers.register(:fifo, self)

  attr_reader :queue

  def initialize
    @queue = Concurrent::Array.new([])
    @allocation_mutex = Mutex.new
  end

  # Add a single job to the queue.
  def add_job(job)
    # As this is a FIFO queue, it can be assumed that the job won't start
    # immediately due to a previous job. Ipso facto the reason should be Priority
    #
    # There is a corner case when the previous job has finished, where the next
    # job's reason should be WaitingForScheduling. However this should only
    # be for a brief moment before the job is either:
    # * ran which reverts the reason to blank, or
    # * the reason is changed to Resources
    job.reason_pending = 'Priority'

    @queue << job
    Async.logger.debug("Added job #{job.id} to #{self.class.name}")
  end

  # Remove a single job from the queue.
  def remove_job(job)
    @queue.delete(job)
    if job.job_type == 'ARRAY_JOB'
      @queue.delete_if { |j| j.array_job == job }
    end
    Async.logger.debug("Removed job #{job.id} from #{self.class.name}")
  end

  # Allocate any jobs that can be scheduled.
  #
  # In order for a job to be scheduled, the partition must contain sufficient
  # available resources to meet the job's requirements.
  def allocate_jobs
    # This is a simple FIFO. Only consider the next unallocated job in the
    # FIFO.  If it can be allocated, keep going until we either run out of
    # jobs or find one that cannot be allocated.

    new_allocations = []
    @allocation_mutex.synchronize do
      loop do
        next_job = @queue.detect do |job|
          job.pending? && job.allocation.nil?
        end

        break if next_job.nil?

        if next_job.job_type == 'ARRAY_JOB'
          # Special handling for array jobs.
          #
          # * Grab the next task for it.
          # * If it cannot be allocated break out of the loop.
          # * If it can be allocated, allocate and add it to the queue.
          # * If the array job has no more tasks, remove it from the queue.
          next_task = next_job.task_registry.next_task(false)
          allocation = allocate_job(next_task)
          if allocation.nil?
            next_job.reason_pending = 'Resources'
            break
          else
            @queue.insert(@queue.index(next_job) + 1, next_task)
            FlightScheduler.app.allocations.add(allocation)
            new_allocations << allocation
            next_task = next_job.task_registry.next_task(true)
            if next_task.nil?
              # All of the job's tasks are now scheduled.  We're done with the
              # array job.
              @queue.delete(next_job)
            end
          end
        else
          allocation = allocate_job(next_job)
          if allocation.nil?
            next_job.reason_pending = 'Resources'
            break
          else
            FlightScheduler.app.allocations.add(allocation)
            new_allocations << allocation
          end
        end
      end
    end
    new_allocations
  end

  private

  # Attempt to allocate a single job.
  #
  # If the partition has sufficient resources available for the job, create a
  # new +Allocation+ and return.  Otherwise return +nil+.
  def allocate_job(job)
    partition = job.partition
    nodes = partition.available_nodes_for(job)
    return nil if nodes.nil?
    Allocation.new(job: job, nodes: nodes)
  end

  # These methods exist to facilitate testing.
  def clear
    @queue.clear
  end
end
