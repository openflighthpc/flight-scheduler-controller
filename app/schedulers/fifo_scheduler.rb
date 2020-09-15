#==============================================================================
# Copyright (C) 2020-present Alces Flight Ltd.
#
# This file is part of FlurmAPI.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License 2.0 which is available at
# <https://www.eclipse.org/legal/epl-2.0>, or alternative license
# terms made available by Alces Flight Ltd - please direct inquiries
# about licensing to licensing@alces-flight.com.
#
# FlurmAPI is distributed in the hope that it will be useful, but
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR
# IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR CONDITIONS
# OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A
# PARTICULAR PURPOSE. See the Eclipse Public License 2.0 for more
# details.
#
# You should have received a copy of the Eclipse Public License 2.0
# along with FlurmAPI. If not, see:
#
#  https://opensource.org/licenses/EPL-2.0
#
# For more information on FlurmAPI, please visit:
# https://github.com/openflighthpc/flurm-api
#==============================================================================

class FifoScheduler
  attr_reader :queue

  def initialize
    @queue = Concurrent::Array.new([])
    @allocation_mutex = Mutex.new
  end

  # Add a single job to the queue.
  def add_job(job)
    @queue << job
  end

  # Remove a single job from the queue.
  def remove_job(job)
    @queue.delete(job)
  end

  # Allocate any jobs that can be scheduled.
  #
  # In order for a job to be scheduled, the partition must contain sufficient
  # available resources to meet the job's requirements.
  def allocate_jobs
    # This is a simple FIFO. Only consider the next unallocated job in the
    # FIFO.  If it can be allocated, keep going until we either run out of
    # jobs or find one that cannot be allocated.

    return nil if @queue.empty?
    @allocation_mutex.synchronize do
      loop do
        next_job = @queue.detect { |job| job.allocation.nil? }
        break if next_job.nil?
        allocation = allocate_job(next_job)
        break if allocation.nil?
        AllocationSet.instance.add(allocation)
      end
    end
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

  private

  # These methods exist to facilitate testing.

  def clear
    @queue.clear
  end
end
