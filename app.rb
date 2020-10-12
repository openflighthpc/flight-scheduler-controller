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

require 'securerandom'
require 'sinatra/custom_logger'
require 'base64'

require_relative 'app/serializers'

class App < Sinatra::Base
  include Swagger::Blocks

  helpers Sinatra::CustomLogger
  configure do
    o = Object.new()
    def o.write(msg) ; Async.logger.info(msg) ; end
    use Rack::CommonLogger, o
    set :logger, Async.logger
  end

  before do
    # Set the header to bypass the over restrictive nature of JSON:API
    env['HTTP_ACCEPT'] = 'application/vnd.api+json'

    auth_header = env.fetch('HTTP_AUTHORIZATION', '')
    @current_user =
      begin
        FlightScheduler::Auth.user_from_header(auth_header)
      rescue FlightScheduler::Auth::AuthenticationError
        nil
      end
  end

  register Sinja
  self.prepend SinjaContentPatch

  helpers do
    def current_user
      @current_user
    end

    def role
      current_user.to_s.empty? ? :unknown : :user
    end
  end

  configure_jsonapi do |c|
    c.validation_exceptions << ActiveModel::ValidationError
    c.validation_formatter = ->(e) do
      e.model.errors.messages
    end

    # Resource roles
    c.default_roles = {
      index: :user,
      show: :user,
      create: :user,
      update: :user,
      destroy: :user
    }

    # To-one relationship roles
    c.default_has_one_roles = {
      pluck: :user,
      prune: :user,
      graft: :user
    }

    # To-many relationship roles
    c.default_has_many_roles = {
      fetch: :user,
      clear: :user,
      replace: :user,
      merge: :user,
      subtract: :user
    }
  end

  resource :partitions do
    swagger_schema :Partition do
      key :required, :id
      property :id, type: :string
      property :type, type: :string, enum: ['partitions']
      property :attributes do
        property :name, type: :string
        property :nodes, type: :array do
          items { key :type, :string }
        end
      end
      property :relationships do
        property :'nodes' do
          property(:data, type: :array) do
            items { key '$ref', :rioNode }
          end
        end
      end
    end

    swagger_schema :rioPartition do
      property :type, type: :string, enum: ['partitions']
      property :id, type: :string
    end

    swagger_path '/partitions' do
      operation :get do
        key :summary, 'All partitions'
        key :description, 'Returns a list of all the partions and related nodes'
        key :operaionId, :indexPartitions
        response 200 do
          schema do
            property :data, type: :array do
              items { key :'$ref', :Partition }
            end
          end
        end
      end
    end

    index do
      FlightScheduler.app.partitions
    end
  end

  resource :job_steps, pkre: /[\w.-]+/ do
    helpers do
      def find(id)
        job_id, step_id = id.split('.')
        job = FlightScheduler.app.scheduler.queue.find { |j| j.id == job_id }
        return nil unless job
        job.job_steps.detect { |step| step.id.to_s == step_id }
      end

      def validate!
        if @created && resource.validate!
          resource.job.job_steps << resource
          FlightScheduler.app.event_processor.job_step_created(resource)
        end
      end
    end

    create do |attr|
      @created = true
      job = FlightScheduler.app.scheduler.queue.find { |j| j.id == attr[:job_id] }
      step = JobStep.new(
        arguments: attr[:arguments],
        job: job,
        id: job.next_step_id,
        path: attr[:path],
        pty: attr[:pty],
      )
      next step.id, step
    end

    show
  end

  resource :jobs, pkre: /[\w-]+/ do
    swagger_schema :Job do
      property :type, type: :string, enum: ['jobs']
      property :id, type: :string
      property :attributes do
        property 'min-nodes', type: :integer, minimum: 1
        property :state, type: :string, enum: Job::STATES
        property 'script-name', type: :string
        property :reason_pending, type: :string, enum: Job::PENDING_REASONS, nullable: true
      end
      property :relationships do
        property :partition do
          property(:data) { key '$ref', :rioPartition }
        end
        property :'allocated-nodes' do
          property(:data, type: :array) do
            items { key '$ref', :rioNode }
          end
        end
      end
    end

    swagger_schema :rioJob do
      property :type, type: :string, enum: ['jobs']
      property :id, type: :string
    end

    swagger_schema :newJob do
      property :type, type: :string, enum: ['jobs']
      property :attributes do
        key :required, [:'min-nodes', :script, 'script-name', :arguments]
        property :'min-nodes' do
          one_of do
            key :type, :string
            key :pattern, '^\d+[km]?$'
          end
          one_of do
            key :type, :integer
            key :minimum, 1
          end
        end
        property :script, type: :string
        property 'script-name',  type: :string
        property 'stdout-path', type: :string, description: FlightScheduler::PathGenerator::DESC
        property 'stderr-path', type: :string, description: FlightScheduler::PathGenerator::DESC
        property :arguments, type: :array do
          items type: :string
        end
        property :array, type: :string, pattern: FlightScheduler::RangeExpander::DOC_REGEX
      end
    end

    swagger_path '/jobs' do
      operation :get do
        key :summary, 'Return all the current jobs'
        key :operationId, :indexJobs
        response 200 do
          schema do
            property :data, type: :array do
              items do
                key :'$ref', :Job
              end
            end
          end
        end
      end

      operation :post do
        key :summary, 'Create a new batch job'
        key :operationId, :createJob
        parameter do
          key :name, :data
          key :in, :body
          schema do
            property(:data) { key :'$ref', :newJob }
          end
        end
        response 201 do
          schema do
            property :data do
              key :'$ref', :Job
            end
          end
        end
      end
    end

    swagger_path 'jobs/{id}' do
      parameter do
        key :name, :id
        key :in, :path
        key :description, 'The job ID'
        key :required, true
      end

      operation :delete do
        key :summary, 'Clear a scheduled job'
        key :operationId, :destroyJob
        response 204
      end
    end

    helpers do
      def find(id)
        job_or_task = FlightScheduler.app.scheduler.queue.find do |job|
          job.id == id || job&.array_job&.id == id
        end
        return unless job_or_task
        job_or_task.id == id ? job_or_task : job_or_task.array_job
      end

      def validate!
        if @created && resource.validate!
          if resource.has_batch_script?
            resource.batch_script.write
          end
          FlightScheduler.app.event_processor.job_created(resource)
        end
      end
    end

    index do
      FlightScheduler.app.scheduler.queue
    end

    create do |attr|
      @created = true
      job = Job.new(
        array: attr[:array],
        id: SecureRandom.uuid,
        min_nodes: attr[:min_nodes],
        partition: FlightScheduler.app.default_partition,
        reason_pending: 'WaitingForScheduling',
        state: 'PENDING',
        username: current_user,
      )
      if attr[:script] || attr[:arguments] || attr[:script_name]
        job.batch_script = BatchScript.new(
          arguments: attr[:arguments],
          content: attr[:script],
          job: job,
          name: attr[:script_name],
          stderr_path: attr[:stderr_path],
          stdout_path: attr[:stdout_path],
        )
      end
      next job.id, job
    end

    destroy do
      FlightScheduler.app.event_processor.cancel_job(resource)
    end
  end

  swagger_schema :Node do
    key :required, :id
    property :id, type: :string
    property :type, type: :string, enum: ['nodes']
    property :attributes do
      property :name, type: :string
      property :state, type: :string, enum: ::Node::STATES
    end
    property :relationships do
      property :'allocated' do
        property(:data) { key '$ref', :rioJobTask }
      end
    end
  end

  swagger_schema :rioNode do
    property :type, type: :string, enum: ['nodes']
    property :id, type: :string
  end
end
