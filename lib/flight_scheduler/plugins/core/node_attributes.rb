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

require 'active_model'
require 'forwardable'

# Plugin providing basic attributes/resources for nodes.
#
# Extends `Node` with basic resource attributes cpus, gpus and memory, and
# provides a mechanism enabling them to be retrieved.
#
# It is expected that eventually a "general resource" plugin might be used
# alongside or instead of this.  Exactly how that will look is currently
# unknown.
#
# This plugin also manages a nodes type, but that would perhaps be better off
# in a "partition events"/"partition scripts" plugin.
class NodeAttributes
  def self.plugin_name
    'core/node_attributes'
  end

  def self.init
    ::Node.prepend(NodeExtensions)
    self.new
  end

  class EventProcessor
    def daemon_connected(node, message)
      # Update the node's attributes
      attributes = node.attributes.dup
      attributes.cpus   = message[:cpus]   if message.key?(:cpus)
      attributes.gpus   = message[:gpus]   if message.key?(:gpus)
      attributes.memory = message[:memory] if message.key?(:memory)
      attributes.type   = message[:type]   if message.key?(:type)

      if attributes == node.attributes
        Async.logger.debug("[resources] unchanged '#{node.name}' attributes:") {
          attributes.to_h.map { |k, v| "#{k}: #{v}" }.join("\n")
        }
      elsif attributes.valid?
        Async.logger.debug("[resources] updated '#{node.name}' attributes:") {
          attributes.to_h.map { |k, v| "#{k}: #{v}" }.join("\n")
        }
        node.attributes = attributes
        FlightScheduler.app.nodes.update_partition_cache(node)
      else
        Async.logger.error("[resources] invalid attributes for #{node.name}") {
          attributes.errors.messages
        }
      end
    end
  end

  class Attributes
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

  module NodeExtensions
    extend Forwardable
    attr_accessor :attributes
    def_delegators  :attributes, :type, *Attributes::DELEGATES

    def initialize(*args)
      super
      @attributes = Attributes.new(cpus: 1, memory: 1048576)
    end
  end

  def event_processor
    @event_processor ||= EventProcessor.new
  end

  def resources_for(node)
    node.attributes.to_h
  end
end
