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

# Maintains a registry of plugins.
class FlightScheduler::Plugins
  class DuplicatePlugin < RuntimeError; end
  class UnknownPlugin < RuntimeError; end

  # XXX Move this to Configuration
  PLUGINS = [
    'core/domain_model',
    'core/node_attributes',
    'core/scheduling',

    'scheduler_state/filetxt',
  ]

  def initialize
    @registry = {}
    @type_registry = {}
  end

  def register(plugin)
    name = plugin.plugin_name
    Async.logger.info("[plugins] registering #{name}")

    # Determine the plugin type.  Only a single plugin of each type is
    # allowed, appart from 'core' plugins.
    type = name.split('/').first
    type = nil if type == 'core'

    if @registry.key?(name)
      raise DuplicatePlugin, name
    end
    if type && @type_registry.key?(type)
      raise DuplicatePlugin, type
    end
    if plugin.respond_to?(:init)
      Async.logger.info("[plugins] initializing #{name}")
      plugin.init
    end
    p = plugin.new
    @registry[name] = p
    @type_registry[type] = p unless type.nil?
  end

  def lookup(name)
    unless @registry.key?(name)
      raise UnknownPlugin, name
    end
    @registry[name]
  end

  def lookup_type(type)
    unless @type_registry.key?(type)
      raise UnknownPlugin, type
    end
    @type_registry[type]
  end

  def load
    PLUGINS.each do |plugin_name|
      path = File.join(__dir__, 'plugins', "#{plugin_name}.rb")
      if File.exist?(path)
        Async.logger.info("[plugins] loading #{plugin_name} (#{path})")

        m = Module.new do
          def self.get_plugin
            plugins = constants.map do |c|
              const = const_get(c)
              if const.is_a?(Class) && const.respond_to?(:plugin_name)
                const
              end
            end
            raise "No plugins defined" if plugins.empty?
            raise "Multiple plugins defined" if plugins.length > 1
            plugins[0]
          end
        end

        m.class_eval(File.read(path))
        register(m.get_plugin)
      else
        Async.logger.warn("[plugins] no such plugin #{plugin_name} (#{path})")
      end
    end
  end

  def event_processors
    @registry
      .values
      .map { |p| p.respond_to?(:event_processor) ? p.event_processor : nil }
      .compact
  end
end
