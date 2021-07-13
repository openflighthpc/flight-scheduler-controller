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

# Maintains a registry of plugins.
class FlightScheduler::Plugins
  class DuplicatePlugin < RuntimeError; end
  class UnknownPlugin < RuntimeError; end

  def initialize
    @registry = {}
    @type_registry = {}
  end

  def register(name, plugin)
    Async.logger.info("[plugins] registering #{name}")
    type = name.split('/').first
    if @registry.key?(name)
      raise DuplicatePlugin, name
    end
    if @type_registry.key?(type)
      raise DuplicatePlugin, type
    end
    @registry[name] = plugin
    @type_registry[type] = plugin
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
    Dir.glob(File.join(__dir__, 'plugins', '*.rb')).each do |plugin|
      Async.logger.info("[plugins] loading #{plugin}")
      require plugin
    end
  end

  def event_processors
    @registry
      .values
      .map { |p| p.respond_to?(:event_processor) ? p.event_processor : nil }
      .compact
  end
end
