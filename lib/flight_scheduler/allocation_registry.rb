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

# Registry of all active allocations.
#
# This class provides a single location that can be queried for the current
# resource allocations.
#
# Using this class as the single source of truth for allocations has some
# important properties:
#
# 1. Creating an `Allocation` object does not allocate any resources.
# 2. Resources are only allocated once they are +add+ed to the
#    +AllocationRegistry+.
# 3. Adding +Allocation+s to the +AllocationRegistry+ is atomic; and therefore
#    thread safe.
#
class FlightScheduler::AllocationRegistry
  class AllocationConflict < RuntimeError ; end

  def initialize
    @node_allocations = {}
    @job_allocations  = {}
    @mutex = Mutex.new
  end

  def add(allocation)
    @mutex.synchronize do
      allocation.nodes.each do |node|
        raise AllocationConflict, node unless for_node(node.name).empty?
      end
      raise AllocationConflict, allocation.job if for_job(allocation.job.id)

      allocation.nodes.each do |node|
        (@node_allocations[node.name] ||= []) << allocation
      end
      @job_allocations[allocation.job.id] = allocation
    end
  end

  def delete(allocation)
    @mutex.synchronize do
      # NOTE: It is assumed that that the job_allocation registry maintains
      # a 1-1 mapping to allocations. This is used as a proxy to the number
      # of allocations
      @job_allocations.delete(allocation.job.id)

      # NOTE: This method assumes the allocation has not changed size post
      # being created, or very least remains consistent with the registry
      allocation.nodes.each do |node|
        next unless @node_allocations.key? node.name
        @node_allocations[node.name].delete(allocation)
      end
    end
  end

  def for_job(job_id)
    with_lock { @job_allocations[job_id] }
  end

  def for_node(node_name)
    with_lock { @node_allocations[node_name] || [] }
  end

  def size
    with_lock do
      @job_allocations.size
    end
  end

  def each(&block)
    values = with_lock do
      @job_allocations.values
    end
    values.each(&block)
  end

  private

  def with_lock
    if @mutex.owned?
      yield
    else
      @mutex.synchronize do
        yield
      end
    end
  end

  # These methods exist to facilitate testing.

  def empty?
    with_lock do
      @job_allocations.empty?
    end
  end

  def clear
    @mutex.synchronize do
      @job_allocations.clear
      @node_allocations.clear
    end
  end
end
