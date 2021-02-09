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
    if command == 'NODE_DEALLOCATED'
      FlightScheduler::EventProcessor::DaemonProcessor.new(connection, node_name).process(message)
    elsif ['NODE_COMPLETED_JOB', 'NODE_FAILED_JOB', 'JOB_TIMED_OUT'].include?(command)
      FlightScheduler::EventProcessor::JobProcessor.new(connection, node_name, message[:job_id]).process(message)
    elsif ['RUN_STEP_STARTED', 'RUN_STEP_COMPLETED', 'RUN_STEP_FAILED']
      FlightScheduler::EventProcessor::StepProcessor.new(connection, node_name, message[:job_id], message[:step_id]).process(message)
    else
      Async.logger.info("Unknown message #{message}")
    end
  rescue
    Async.logger.warn("Error processing message #{$!.message}")
    Async.logger.debug($!.full_message)
  end
end

class WebsocketApp
  include Swagger::Blocks

  swagger_schema :connectedWS do
    property :command, type: :string, required: true, enum: ['CONNECTED']
    property :node,   type: :string,  required: true
    property :cpus,   type: :integer, required: false
    property :gpus,   type: :integer, required: false
    property :memory, type: :integer, required: false
    property :type,   type: :string,  required: false
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

  swagger_schema :nodeDeallocatedWS do
    property :command, type: :stirng, required: true, enum: ['NODE_DEALLOCATED']
    property :job_id, type: :string, required: true
  end

  swagger_schema :jobCancelledWS do
    property :command, type: :string, required: true, enum: ['JOB_CANCELLED']
    property :job_id, type: :string, required: true
  end

  swagger_schema :jobDeallocatedWS do
    property :command, type: :string, required: true, enum: ['JOB_DEALLOCATED']
    property :job_id, type: :string, required: true
  end

  swagger_schema :runScriptWS do
    property :command, type: :string, required: true, enum: ['RUN_SCRIPT']
    property :job_id, type: :string, required: true
    property :script, type: :string, required: true
    property :arguments, type: :array, required: true do
      items type: :string
    end
    property :stdout_path, type: :string, required: true, format: :path
    property :stderr_path, type: :string, required: true, format: :path

    property :array_job_id, type: :string, required: false
    property :array_task_id, type: :string, required: false
  end

  swagger_schema :runStepWS do
    property :command, type: :string, required: true, enum: ['RUN_STEP']
    property :job_id, type: :string, required: true
    property :step_id, type: :string, required: true
    property :path, type: :string, required: true
    property :arguments, type: :array, required: true do
      items type: :string
    end
  end

  swagger_schema :runStepStartedWS do
    property :command, type: :string, required: true, enum: ['RUN_STEP_STARTED']
    property :job_id, type: :string, required: true
    property :step_id, type: :string, required: true
  end

  swagger_schema :runStepCompletedWS do
    property :command, type: :string, required: true, enum: ['RUN_STEP_COMPLETED']
    property :job_id, type: :string, required: true
    property :step_id, type: :string, required: true
  end

  swagger_schema :runStepFailedWS do
    property :command, type: :string, required: true, enum: ['RUN_STEP_FAILED']
    property :job_id, type: :string, required: true
    property :step_id, type: :string, required: true
  end

  swagger_schema :jobAllocatedWS do
    property :command, type: :string, required: true, enum: ['JOB_ALLOCATED']
    property :job_id, type: :string, required: true
    property :username, type: :string, required: true
    property :time_limit, type: :integer, require: false

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

  swagger_schema 'jobAllocationFailedWS' do
    property :command, type: :string, required: true, enum: ['JOB_ALLOCATION_FAILED']
    property :job_id, type: :string, required: true
  end

  swagger_schema 'jobTimedOut' do
    property :command, type: :string, required: true, enum: ['JOB_TIMED_OUT']
    property :job_id, type: :string, required: true
  end

  swagger_path '/ws' do
    operation :get do
      key :summary, 'Establish a control-daemon connection'
      key :operationId, :getWebSocket
      parameter name: 'CONNECTED', in: :body do
        schema { key :'$ref', :connectedWS }
      end
      parameter name: 'NODE_COMPLETED_JOB', in: :body do
        schema { key :'$ref', :nodeCompletedJobWS }
      end
      parameter name: 'NODE_FAILED_JOB', in: :body do
        schema { key :'$ref', :nodeFailedJobWS }
      end
      parameter name: 'NODE_DEALLOACTED', in: :body do
        schema { key :'$ref', :nodeDeallocatedWS }
      end
      parameter name: 'RUN_STEP_STARTED', in: :body do
        schema { key '$ref', :runStepStartedWS }
      end
      parameter name: 'RUN_STEP_COMPLETED', in: :body do
        schema { key '$ref', :runStepCompletedWS }
      end
      parameter name: 'RUN_STEP_FAILED', in: :body do
        schema { key '$ref', :runStepFailedWS }
      end
      response 'JOB_ALLOCATED' do
        schema { key '$ref', :jobAllocatedWS }
      end
      response 'JOB_ALLOCATION_FAILED' do
        schema { key '$ref', :jobAllocationFailedWS }
      end
      response 'RUN_SCRIPT' do
        schema { key '$ref', :runScriptWS }
      end
      response 'RUN_STEP' do
        schema { key '$ref', :runStepWS }
      end
      response 'JOB_CANCELLED' do
        schema { key :'$ref', :jobCancelledWS }
      end
      response 'JOB_DEALLOCATED' do
        schema { key :'$ref', :jobDeallocatedWS }
      end
      response 'JOB_TIMED_OUT' do
        schema { key :'$ref', :jobTimedOut }
      end
    end
  end

  def update_node(node_name, message)
    node = FlightScheduler.app.nodes[node_name] || FlightScheduler.app.nodes.register_node(node_name)

    attributes = node.attributes.dup
    attributes.cpus   = message[:cpus]    if message.key?(:cpus)
    attributes.gpus   = message[:gpus]    if message.key?(:gpus)
    attributes.memory = message[:memory]  if message.key?(:memory)
    attributes.type   = message[:type]    if message.key?(:type)

    if attributes == node.attributes
      Async.logger.debug("Unchanged '#{node_name}' attributes:") {
        attributes.to_h.map { |k, v| "#{k}: #{v}" }.join("\n")
      }
    elsif attributes.valid?
      Async.logger.info("Updating '#{node_name}' attributes:") {
        attributes.to_h.map { |k, v| "#{k}: #{v}" }.join("\n")
      }

      node.attributes = attributes
      FlightScheduler.app.nodes.update_partition_cache(node)
    else
      Async.logger.error <<~ERROR
        Invalid node attributes for #{node.name}:
        #{attributes.errors.messages}
      ERROR
    end
  end

  def call(env)
    Async::WebSocket::Adapters::Rack.open(env) do |connection|
      begin
        message = connection.read
        unless message_valid?(message)
          Async.logger.info("Badly formed connection message #{message.inspect}")
          connection.close
          next
        end

        begin
          node_name = FlightScheduler::Auth.node_from_token(message[:auth_token])
        rescue FlightScheduler::Auth::AuthenticationError
          Async.logger.info("Could not authenticate connection: #{$!.message}")
          connection.close
        else
          case message[:command]
          when 'CONNECTED'
            process_daemon_connection(node_name, connection, message)
          when 'BATCHD_CONNECTED'
            process_batchd_connection(node_name, connection, message)
          when 'STEPD_CONNECTED'
            process_stepd_connection(node_name, connection, message)
          end
        end
      end
    end
  end

  private

  def process_daemon_connection(node_name, connection, message)
    update_node(node_name, message)
    Async.logger.info("#{node_name.inspect} connected")
    message_processor = MessageProcessor.new(node_name, connection)
    processor = FlightScheduler::EventProcessor::DaemonProcessor.new(connection, node_name)
    FlightScheduler.app.processors.add(processor)
    Async.logger.debug("Connected nodes #{FlightScheduler.app.processors.connected_nodes}")
    while message = connection.read
      message_processor.call(message)
    end
    connection.close
  ensure
    Async.logger.info("#{node_name.inspect} disconnected")
    FlightScheduler.app.processors.remove(processor)
    Async.logger.debug("Connected nodes #{FlightScheduler.app.processors.connected_nodes}")
  end

  def process_batchd_connection(node_name, connection, message)
    name = message[:name]
    Async.logger.info("#{name.inspect} connected")
    processor = MessageProcessor.new(node_name, connection)
    while message = connection.read
      processor.call(message)
    end
    connection.close
  ensure
    Async.logger.info("#{name.inspect} disconnected")
  end

  def process_stepd_connection(node_name, connection, message)
    name = message[:name]
    Async.logger.info("#{name.inspect} connected")
    processor = MessageProcessor.new(node_name, connection)
    while message = connection.read
      processor.call(message)
    end
    connection.close
  ensure
    Async.logger.info("#{name.inspect} disconnected")
  end

  def message_valid?(message)
    return false unless message.is_a?(Hash)
    return false unless %w(CONNECTED BATCHD_CONNECTED STEPD_CONNECTED).include?(message[:command])
    return true
  end
end
