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

    attr_accessor(*DELEGATES)
    validates(*DELEGATES, allow_nil: true, numericality: { only_integers: true })

    def to_h
      self.class::DELEGATES.each_with_object({}) do |key, memo|
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
  attr_accessor   :attributes
  def_delegators  :attributes, *NodeAttributes::DELEGATES

  attr_reader :name

  STATES = ['IDLE', 'ALLOC']

  def initialize(name:, attributes: nil)
    @name = name
    @attributes = attributes || NodeAttributes.new(cpus: 1, memory: 1048576)
  end

  def state
    if allocation
      'ALLOC'
    elsif connected?
      'IDLE'
    else
      'DOWN'
    end
  end

  def allocations
    FlightScheduler.app.allocations.for_node(self.name)
  end

  def connected?
    FlightScheduler.app.daemon_connections[self.name]
  end

  # TODO: Replace this with the satisfies count
  def satisfies?(job)
    connected? && allocations.empty? && (satisfies(job) > 0)
  end

  def satisfies(job)
    # Ensure the job is valid to prevent maths errors
    unless job.valid?
      Async.logger.error "Can not determine resource satisfication for an invalid job: #{job.id}"
      return 0
    end

    cpus / job.cpus_per_node.to_i
  end

  def ==(other)
    self.class == other.class &&
      name == other.name
  end
  alias eql? ==

  def hash
    [self.class, name].hash
  end
end
