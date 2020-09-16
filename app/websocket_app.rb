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

require 'async/websocket/adapters/rack'

class MessageProcessor
  attr_reader :connection

  def initialize(node_name, connection)
    @node_name = node_name
    @connection = connection
  end

  def call(message)
    Async.logger.info("Processing message #{message.inspect}")
    command = message.first
    case command

    when 'NODE_COMPLETED_JOB'
      _, job_id = message
      FlightScheduler.app.event_processor.node_completed_job(@node_name, job_id)

    else
      Async.logger.info("Unknown message #{message}")
    end
  rescue
    Async.logger.info("Error processing message #{$!.message}")
  end
end

class WebsocketApp
  def call(env)
    Async::WebSocket::Adapters::Rack.open(env) do |connection|
      begin
        message = connection.read
        unless message.is_a?(Array) && message.length == 2 && message.first == 'CONNECTED'
          Async.logger.info("Badly formed connection message #{message.inspect}")
          connection.close
          break
        end

        _, node = message
        Async.logger.info("#{node.inspect} connected")
        processor = MessageProcessor.new(node, connection)
        connections.add(node, processor)
        while message = connection.read
          processor.call(message)
        end
        connection.close
      ensure
        Async.logger.info("#{node.inspect} disconnected")
        connections.remove(processor)
      end
    end
  end

  private

  def connections
    FlightScheduler.app.daemon_connections
  end
end
