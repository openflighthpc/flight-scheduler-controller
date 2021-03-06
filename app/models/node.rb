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

# The node's attributes are updated dynamically post initialisation
# However without a DBMS, making atomic changes becomes a bit tricky
# A work around is to store all the dynamic attributes on a different
# model which can be substituted when an update occurs
require 'active_model'
require 'forwardable'

class Node
  class NodeAttributes
    include ActiveModel::Model

    DELEGATES = [:cpus, :gpus, :memory]

    attr_writer :type
    attr_accessor(*DELEGATES)
    validates(*DELEGATES, allow_nil: true, numericality: { only_integers: true })

    def type
      str = @type.to_s
      str.empty? ? 'unknown' : str
    end

    def to_h
      self.class::DELEGATES.each_with_object({ type: type }) do |key, memo|
        memo[key] = self.send(key)
      end
    end

    def ==(other)
      return false unless other.class == self.class
      self.class::DELEGATES.all? do |key|
        self.send(key) == other.send(key)
      end
    end
  end

  extend Forwardable
  attr_accessor :attributes
  def_delegators  :attributes, :type, *NodeAttributes::DELEGATES

  attr_reader :name

  STATES = ['IDLE', 'ALLOC', 'DOWN']

  def initialize(name:, attributes: nil)
    @name = name
    @attributes = attributes || NodeAttributes.new(cpus: 1, memory: 1048576)
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
    FlightScheduler.app.processors.connected?(self.name)
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
