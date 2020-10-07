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

  def initialize
    @group_id_queue = Concurrent::Array.new([])
    @data = Concurrent::Map.new
    @allocation_mutex = Mutex.new
  end

  def queue
    @group_id_queue.map { |id| @data[id][:job] }
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

    # Queues the group_id and saves the job's enumerator
    @group_id_queue << job.group_id
    @data[job.group_id] = Concurrent::Map.new.tap do |m|
      m[:job] = job
      m[:enum] = job.to_enum
      m[:active] = Concurrent::Array.new
    end

    Async.logger.debug("Added job #{job.id} to #{self.class.name}")
  end

  # Remove a single job from the queue.
  def remove_job(job)
    @group_id_queue.delete(job)
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
        # Select the next available job
        next_group_id = @group_id_queue.detect do |id|
          @data[id][:enum].peek
        end
        break unless next_group_id

        # Fetches the next job
        # NOTE: Intentionally peek instead of next! The enumerator must not
        #       progress until the allocation has been confirmed
        enum = @data[next_group_id][:enum]
        next_job = enum.peek

        # Fast-Forward past allocated jobs
        # TODO: Confirm if the is a valid use case or a by-product of the spec
        #       Notionally jobs which are already allocated are being managed
        #       by some external scheduler. This implies they shouldn't have
        #       been added to this scheduler in the first place
        if next_job.allocation
          enum.next
          next
        end

        # Create the allocation
        allocation = allocate_job(next_job)
        if allocation.nil?
          @data[next_group_id][:job].reason_pending = 'Resources'
          next_job.reason_pending = 'Resources'
          break
        else
          FlightScheduler.app.allocations.add(allocation)
          new_allocations << allocation
          enum.next
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
    @group_id_queue.clear
    @data.clear
  end
end
