module FlightScheduler
  class Plugins
    class Resources
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

      def event_processor
        @event_processor ||= EventProcessor.new
      end
    end

    FlightScheduler.app.plugins.register('resources/basic', Resources.new)
  end
end
