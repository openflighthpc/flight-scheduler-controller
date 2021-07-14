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

class Node
  attr_reader :name

  STATES = ['IDLE', 'ALLOC', 'DOWN']

  def initialize(name:)
    @name = name
  end

  def state
    if connected?
      if allocations.any?
        'ALLOC'
      else
        'IDLE'
      end
    else
      'DOWN'
    end
  end

  def allocations
    FlightScheduler.app.allocations.for_node(self.name)
  end

  def connected?
    FlightScheduler.app.connection_registry.connected?(self.name)
  end

  def ==(other)
    self.class == other.class &&
      name == other.name
  end
  alias eql? ==

  def hash
    [self.class, name].hash
  end

  # A static view of whether this node is suitable for the given job.  This
  # considers the current state of node but not its allocations.
  def satisfies_job?(job)
    return false if state == 'DOWN'
    key_map = FlightScheduler::AllocationRegistry::KEY_MAP
    key_map.all? do |node_key, job_key|
      (self.send(node_key) || 0) >= job.send(job_key)
    end
  end
end
