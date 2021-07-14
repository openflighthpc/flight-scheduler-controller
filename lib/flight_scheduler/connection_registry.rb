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
  class ConnectionRegistry
    class DuplicateConnection < RuntimeError; end
    class UnconnectedError < RuntimeError ; end
    class UnknownConnection < RuntimeError; end

    def initialize
      @mutex = Mutex.new
      @connections = {
        daemon: {},
        jobd: {},
        stepd: {}
      }
    end

    def add(processor)
      assert_known_type(processor)
      type, id = processor.type.to_sym, processor.id

      @mutex.synchronize do
        if @connections[type].key?(id)
          raise DuplicateConnection, "#{type} process - #{id}"
        end
        @connections[type][id] = processor
      end
      Async.logger.info("[processor registry] added #{type}:#{id}")
    end

    def remove(processor)
      assert_known_type(processor)
      type, id = processor.type.to_sym, processor.id

      @mutex.synchronize do
        @connections[type].delete(id)
      end
      Async.logger.info("[processor registry] removed #{type}:#{id}")
    end

    def connected_nodes
      @mutex.synchronize do
        @connections[:daemon].keys
      end
    end

    def connected?(node_name)
      @mutex.synchronize do
        @connections[:daemon].key?(node_name)
      end
    end

    # XXX Remove this???
    def connection
      Connection
    end

    def daemon_processor_for(node_name)
      type, id = :daemon, node_name
      @mutex.synchronize do
        @connections[type].fetch(id) do
          raise UnknownConnection, "connection not found: #{type}:#{id}"
        end
      end
    end

    def job_processor_for(node_name, job_id)
      type, id = :jobd, "#{node_name}:#{job_id}"
      @mutex.synchronize do
        @connections[type].fetch(id) do
          raise UnknownConnection, "connection not found: #{type}:#{id}"
        end
      end
    end

    private

    def assert_known_type(processor)
      unless @connections.keys.include?(processor.type.to_sym)
        raise UnexpectedError, "Unknown processor type: #{processor.type}"
      end
    end
  end
end
