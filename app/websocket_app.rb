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

    when 'NODE_COMPLETED_ARRAY_TASK'
      task_id = message[:array_task_id]
      job_id = message[:array_job_id]
      FlightScheduler.app.event_processor.node_completed_task(@node_name, task_id, job_id)

    when 'NODE_FAILED_ARRAY_TASK'
      task_id = message[:array_task_id]
      job_id = message[:array_job_id]
      FlightScheduler.app.event_processor.node_failed_task(@node_name, task_id, job_id)

    when 'NODE_DEALLOCATED'
      job_id = message[:job_id]
      FlightScheduler.app.event_processor.node_failed_task(@node_name, job_id)

    when 'RUN_STEP_COMPLETED'
      job_id = message[:job_id]
      step_id = message[:step_id]
      FlightScheduler.app.event_processor.job_step_completed(@node_name, job_id, step_id)

    when 'RUN_STEP_FAILED'
      job_id = message[:job_id]
      step_id = message[:step_id]
      FlightScheduler.app.event_processor.job_step_failed(@node_name, job_id, step_id)

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
    property :command, type: :string, required: true, enum: ['CONNECTED']
    property :node, type: :string, required: true
  end

  swagger_schema :nodeCompleteArrayTaskWS do
    property :command, type: :string, requird: true, enum: ['NODE_COMPLETED_ARRAY_TASK']
    property :node, type: :string, required: true
    property :array_task_id, type: :string, required: true
    property :array_job_id, type: :string, required: true
  end

  swagger_schema :nodeFailedArrayTaskWS do
    property :command, type: :string, requird: true, enum: ['NODE_FAILED_ARRAY_TASK']
    property :node, type: :string, required: true
    property :array_task_id, type: :string, required: true
    property :array_job_id, type: :string, required: true
  end

  swagger_schema :nodeCompletedJobWS do
    property :command, type: :string, required: true, enum: ['NODE_COMPLETED_JOB']
    property :node, type: :string, required: true
    property :job_id, type: :string, required: true
  end

  swagger_schema :nodeFailedJobWS do
    property :command, type: :string, required: true, enum: ['NODE_FAILED_JOB']
    property :node, type: :string, required: true
    property :job_id, type: :string, required: true
  end

  swagger_schema :jobCancalled do
    property :command, type: :string, required: true, enum: ['JOB_CANCELLED']
    property :job_id, type: :string, required: true
  end

  swagger_schema :jobDeallocated do
    property :command, type: :string, required: true, enum: ['JOB_DEALLOCATED']
    property :job_id, type: :string, required: true
  end

  swagger_schema :jobAllocatedWS do
    property :command, type: :string, required: true, enum: ['JOB_ALLOCATED']
    property :job_id, type: :string, required: true
    property :script, type: :string, required: true
    property :arguments, type: :array, required: true do
      items type: :string
    end
    property :username, type: :string, required: true
    property :stdout_path, type: :string, required: true, format: :path
    property :stderr_path, type: :string, required: true, format: :path

    prefix = FlightScheduler.app.config.env_var_prefix
    property :environment, required: true do
      property "#{prefix}CLUSTER_NAME", required: true, type: :string,
               value: FlightScheduler.app.config.cluster_name
      property "#{prefix}JOB_ID", required: true, type: :string
      property "#{prefix}JOB_PARTITION", required: true, type: :string
      property "#{prefix}JOB_NODES", requied: true, type: :string, pattern: '^\d+$',
                description: 'The total number of nodes assigned to the job'
      property "#{prefix}JOB_NODELIST", required: :true, type: :string, format: 'csv',
                description: 'The node names as a comma spearated list'
      property "#{prefix}NODENAME", required: true, type: :string

      # TODO: It might be worth splitting array tasks into a different schema
      # NOTE: The required: false is a misnomer. These env vars are all or nothing
      property "#{prefix}ARRAY_JOB_ID", required: false, type: :string
      property "#{prefix}ARRAY_TASK_ID", required: false, type: :string
      property "#{prefix}ARRAY_TASK_COUNT", required: false, type: :string
      property "#{prefix}ARRAY_TASK_MIN", required: false, type: :string
      property "#{prefix}ARRAY_TASK_MAX", required: false, type: :string

      other_desc = 'Additional arbitrary environment variables'
      other_opts = { required: true, type: :string }
      if prefix.empty?
        property '<other>', description: other_desc, **other_opts
      else
        desc = "#{other_desc}. The '#{prefix}' prefix maybe omitted."
        property "[#{prefix}]<other>", description: desc, **other_opts
      end
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
      parameter name: :nodeCompleteArrayTask, in: :body do
        schema { key :'$ref', :nodeCompleteArrayTaskWS }
      end
      parameter name: :nodeFailedArrayTask, in: :body do
        schema { key :'$ref', :nodeFailedArrayTaskWS }
      end
      parameter name: :jobCancelled, in: :body do
        schema { key :'$ref', :jobCancelled }
      end
      parameter name: :jobDeallocated, in: :body do
        schema { key :'$ref', :jobDeallocated }
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
