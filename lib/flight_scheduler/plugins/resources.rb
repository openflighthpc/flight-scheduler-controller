require 'active_model'
require 'forwardable'

module FlightScheduler
  class Plugins
    class Resources
      def self.init
        ::Node.prepend(NodeExtensions)
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

      module NodeExtensions
        extend Forwardable
        attr_accessor :attributes
        def_delegators  :attributes, :type, *NodeAttributes::DELEGATES

        def initialize(*args)
          super
          @attributes = NodeAttributes.new(cpus: 1, memory: 1048576)
        end
      end

      def event_processor
        @event_processor ||= EventProcessor.new
      end

      def resources_for(node)
        node.attributes.to_h
      end
    end

    Resources.init
    FlightScheduler.app.plugins.register('resources/basic', Resources.new)
  end
end
