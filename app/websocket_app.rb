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
  attr_reader :node_name

  def initialize(node_name, connection)
    @node_name = node_name
    @connection = connection
  end

  def call(message)
    Async.logger.info("Processing message #{message.inspect}")
    command = message[:command]
    case command

    when 'NODE_COMPLETED_JOB'
      job_id = message[:job_id]
      FlightScheduler.app.event_processor.node_completed_job(@node_name, job_id)

    when 'NODE_FAILED_JOB'
      job_id = message[:job_id]
      FlightScheduler.app.event_processor.node_failed_job(@node_name, job_id)

    else
      Async.logger.info("Unknown message #{message}")
    end
  rescue
    Async.logger.info("Error processing message #{$!.message}")
  end
end

class WebsocketApp
  include Swagger::Blocks

  swagger_schema :connectWS do
    property :command, type: :string, required: true, value: 'CONNECTED'
    property :node, type: :string, required: true
  end

  swagger_schema :nodeCompletedJobWS do
    property :command, type: :string, required: true, value: 'NODE_COMPLETED_JOB'
    property :node, type: :string, required: true
  end

  swagger_schema :nodeFailedJobWS do
    property :command, type: :string, required: true, value: 'NODE_FAILED_JOB'
    property :node, type: :string, required: true
  end

  swagger_schema :jobAllocatedWS do
    property :command, type: :string, required: true, value: 'JOB_ALLOCATED'
    property :job_id, type: :string, required: true
    property :script, type: :string, required: true
    property :arguments, type: :array, required: true do
      items type: :string
    end
    prefix = FlightScheduler::EventProcessor.env_var_prefix
    property :environment, required: true do
      property "#{prefix}CLUSTER_NAME", required: true, type: :string
      property "#{prefix}JOB_ID", required: true, type: :string
      property "#{prefix}JOB_PARTITION", required: true, type: :string
      property "#{prefix}JOB_NODES", requied: true, type: :string, format: 'natural-number',
                description: 'The total number of nodes assigned to the job'
      property "#{prefix}JOB_NODELIST", required: :true, type: :string, format: 'csv',
                description: 'The names of the nodes as a comma spearated list'
      property "#{prefix}NODENAME", required: true, type: :string
      property "#{prefix}<other>", required: false, type: :string,
                description: 'Any number of other arbitrary variables'
    end
  end

  swagger_path '/ws' do
    operation :get do
      key :summary, 'Establish a control-daemon connection'
      key :operationId, :getWebSocket
      parameter name: :connect, in: :body do
        schema { key :'$ref', :connectWS }
      end
      parameter name: :nodeCompletedJob, in: :body do
        schema { key :'$ref', :nodeCompletedJobWS }
      end
      parameter name: :nodeFailedJob, in: :body do
        schema { key :'$ref', :nodeFailedJobWS }
      end
      # NOTE: At time or writing, this response is handled in FlightScheduler::EventProcessor
      # NOTE: Check the response code
      response 200 do
        schema do
          key '$ref', :jobAllocatedWS
        end
      end
    end
  end

  def call(env)
    Async::WebSocket::Adapters::Rack.open(env) do |connection|
      begin
        message = connection.read
        unless message.is_a?(Hash) && message[:command] == 'CONNECTED'
          Async.logger.info("Badly formed connection message #{message.inspect}")
          connection.close
          break
        end

        node = message[:node]
        Async.logger.info("#{node.inspect} connected")
        processor = MessageProcessor.new(node, connection)
        connections.add(node, processor)
        Async.logger.debug("Connected nodes #{connections.connected_nodes}")
        while message = connection.read
          processor.call(message)
        end
        connection.close
      ensure
        Async.logger.info("#{node.inspect} disconnected")
        connections.remove(processor)
        Async.logger.debug("Connected nodes #{connections.connected_nodes}")
      end
    end
  end

  private

  def connections
    FlightScheduler.app.daemon_connections
  end
end
