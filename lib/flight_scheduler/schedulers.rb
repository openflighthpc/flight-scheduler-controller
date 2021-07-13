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

# Maintains a registry of schedulers.  Allowing the configured scheduler to be
# selected when the application boots.
class FlightScheduler::Schedulers
  class DuplicateScheduler < RuntimeError; end
  class UnknownScheduler < RuntimeError; end

  def initialize
    @registry = {}
  end

  def register(name, scheduler)
    if @registry.key?(name)
      raise DuplicateScheduler, name
    end
    @registry[name] = scheduler
  end

  def lookup(name)
    unless @registry.key?(name)
      raise UnknownScheduler, name
    end
    @registry[name]
  end

  def load(name)
    Async.logger.info("[scheduler] loading #{name.inspect} scheduling algorithm")
    require_relative "schedulers/#{name}_scheduler"
    lookup(name).new
  end
end
