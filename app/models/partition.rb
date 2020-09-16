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

class Partition
  attr_reader :name, :nodes

  def initialize(name:, nodes:, default: false)
    @name = name
    @nodes = nodes
    @default = default
  end

  # Return a list of nodes available to run +job+ or +nil+ if there are
  # insufficient nodes available.
  def available_nodes_for(job)
    available_nodes = nodes.select { |node| node.satisfies?(job) }
    if available_nodes.length >= job.min_nodes.to_i
      available_nodes[0...job.min_nodes.to_i]
    else
      nil
    end
  end

  def default?
    !!@default
  end

  def ==(other)
    self.class == other.class &&
      name == other.name &&
      nodes == other.nodes
  end
  alias eql? ==

  def hash
    ( [self.class, name] + nodes.map(&:hash) ).hash
  end
end
