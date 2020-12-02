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
  attr_reader :name, :nodes, :time_limit

  def initialize(name:, nodes:, default: false, time_limit: nil)
    @name = name
    @nodes = nodes
    @default = default
    @time_limit = FlightScheduler::TimeResolver.new(time_limit).resolve
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
