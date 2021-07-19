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
require 'concurrent'

module FlightScheduler
  # Registry of all connections to the various daemons comprising
  # flight-scheduler-daemons.
  class ConnectionRegistry
    class DuplicateConnection < RuntimeError; end
    class UnconnectedError < RuntimeError ; end
    class UnknownConnection < RuntimeError; end

    def self.id_for_type(type, *args)
      case type
      when :daemon
        unless args.length == 1
          raise UnexpectedError, "Bad ID format #{args}. Expected [node_name]."
        end
        args.first
      when :jobd
        unless args.length == 2
          raise UnexpectedError, "Bad ID format #{args}. Expected [node_name, job_id]."
        end
        "#{args[0]}:#{args[1]}"
      when :stepd
        unless args.length == 3
          raise UnexpectedError, "Bad ID format #{args}. Expected [node_name, job_id, step_id]."
        end
        "#{args[0]}:#{args[1]}.#{args[2]}"
      else
        raise UnexpectedError, "Unknown connection type: #{type}"
      end
    end

    def initialize
      @mutex = Mutex.new
      @connections = {
        daemon: {},
        jobd: {},
        stepd: {}
      }
    end

    def add(connection)
      assert_known_type(connection.type)
      type, id = connection.type.to_sym, connection.id

      @mutex.synchronize do
        if @connections[type].key?(id)
          raise DuplicateConnection, "#{type} process - #{id}"
        end
        @connections[type][id] = connection
      end
      Async.logger.info("[connection registry] added #{type}:#{id}")
    end

    def remove(connection)
      assert_known_type(connection.type)
      type, id = connection.type.to_sym, connection.id

      @mutex.synchronize do
        @connections[type].delete(id)
      end
      Async.logger.info("[connection registry] removed #{type}:#{id}")
    end

    def connected_nodes
      @mutex.synchronize do
        @connections[:daemon].keys
      end
    end

    def connected?(type, *args)
      assert_known_type(type)
      id = self.class.id_for_type(type, *args)
      @mutex.synchronize do
        @connections[type].key?(id)
      end
    end

    def connection_for(type, *args)
      assert_known_type(type)
      id = self.class.id_for_type(type, *args)
      @mutex.synchronize do
        @connections[type].fetch(id) do
          raise UnknownConnection, "connection not found: #{type}:#{id}"
        end
      end
    end

    private

    def assert_known_type(type)
      unless @connections.keys.include?(type.to_sym)
        raise UnexpectedError, "Unknown connection type: #{type}"
      end
    end
  end
end
