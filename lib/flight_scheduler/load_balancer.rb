#==============================================================================
# Copyright (C) 2021-present Alces Flight Ltd.
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

module FlightScheduler
  class LoadBalancer
    def initialize(job:, nodes:)
      @job = job
      @nodes = nodes
    end

    def allocate
      sorted = connected_nodes.map do |node|
        [node, FlightScheduler.app.allocations.max_parallel_per_node(@job, node)]
      end
        .reject { |_, count| count == 0 }
        .sort { |(_n1, count1), (_n2, count2)| count1 <=> count2 }
        .tap { |a| Async.logger.debug("Available allocations") {
            a.map { |node, count| [node.name, count] }
          }
        }
        .map { |n, _| n }
        .reverse

      if sorted.length < @job.min_nodes
        nil
      else
        selected_nodes = sorted[0...@job.min_nodes]
        Async.logger.debug("Selected node for allocation") {
          selected_nodes.map(&:name)
        }
        Allocation.new(job: @job, nodes: selected_nodes)
      end
    end

    private

    def connected_nodes
      @nodes.select(&:connected?)
    end
  end
end
