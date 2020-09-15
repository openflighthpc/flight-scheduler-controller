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
    @allocations = []
    @mutex = Mutex.new
  end

  def add(allocation)
    @mutex.synchronize do
      allocation.nodes.each do |node|
        raise AllocationConflict, node if for_node(node.name)
      end
      raise AllocationConflict, allocation.job if for_job(allocation.job.id)

      @allocations << allocation
    end
  end

  def delete(allocation)
    @mutex.synchronize do
      @allocations.delete(allocation)
    end
  end

  def for_job(job_id)
    with_lock do
      @allocations.detect do |allocation|
        allocation.job.id == job_id
      end
    end
  end

  def for_node(node_name)
    with_lock do
      @allocations.detect do |allocation|
        allocation.nodes.any? { |node| node.name == node_name }
      end
    end
  end

  def size
    with_lock do
      @allocations.size
    end
  end

  def each(&block)
    dup = with_lock do
      @allocations.dup
    end
    dup.each(&block)
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
      @allocations.empty?
    end
  end

  def clear
    @mutex.synchronize do
      @allocations.clear
    end
  end
end
