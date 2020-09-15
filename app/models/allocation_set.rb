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
require 'singleton'

# Registry of all active allocations.
#
# This class provides a single location that can be queried for the current
# resource allocations.  This has a very important property: adding an
# allocation to this set is atomic.  Until the allocation has been
# added to this set nothing has been allocated.
#
class AllocationSet
  include Singleton

  def add(allocation)
    @allocations.add(allocation)
  end

  def delete(allocation)
    @allocations.delete(allocation)
  end

  def for_job(job_id)
    @allocations.detect do |allocation|
      allocation.job.id == job_id
    end
  end

  def for_node(node_name)
    @allocations.detect do |allocation|
      allocation.nodes.any? { |node| node.name == node_name }
    end
  end

  def size
    @allocations.size
  end

  def each(&block)
    @allocations.dup.each(&block)
  end

  private

  def initialize
    @allocations = Concurrent::Set.new
  end

  private

  # These methods exist to facilitate testing.

  def empty?
    @allocations.empty?
  end

  def clear
    @allocations.clear
  end
end
