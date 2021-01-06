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

    c.query_params[:long_poll_pending] = nil
    c.query_params[:long_poll_submitted] = nil

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
    swagger_schema :newJobStep do
      property :type, type: :string, enum: ['job-steps']
      property :attributes do
        key :required, [:job_id, :arguments, :path, :pty]
        property :job_id, type: :string
        property :arguments, type: :array do
          items type: :string
        end
        property :path, type: :string
        property :pty, type: :string # Should this be an integer?
      end
    end

    swagger_path '/job-step/{:id}' do
      parameter name: :id, in: :path, required: true

      operation :get do
        key :summary, 'Return a job step'
        key :operationId, :showJobStep
        parameter in: :query, name: 'long_poll_submitted'
        response 200 do
          schema do
            property :data do
              key :'$ref', :JobStep
            end
          end
        end
      end
    end

    swagger_path '/job-step' do
      operation :post do
        key :summary, 'Create a new job step'
        key :operaionId, :createJobStep
        parameter name: :data, in: :body do
          schema do
            property(:data) { key :'$ref', :newJobStep }
          end
        end
      end
    end

    helpers do
      def find(id)
        job_id, step_id = id.split('.')
        job = FlightScheduler.app.job_registry.lookup(job_id)
        return nil unless job
        job.job_steps.detect { |step| step.id.to_s == step_id }
      end

      def validate!
        if @created && resource.validate!
          resource.write
          resource.job.job_steps << resource
          FlightScheduler.app.event_processor.job_step_created(resource)
        end
      end
    end

    show do
      # Exit early unless doing a long poll
      next resource unless params[:long_poll_submitted]

      # Long poll until the resource is "submitted" or timeout
      task = Async do |t|
        t.with_timeout(FlightScheduler.app.config.polling_timeout) do
          until resource.submitted? do
            t.sleep FlightScheduler.app.config.generic_short_sleep.to_f
          end
        end
      rescue Async::TimeoutError
        # NOOP
      end
      task.wait
      next resource
    end

    # NOTE: This does not conform to the JSON:API specification on creating related resources
    #       The idiomatic approach would be to specify the job in the 'relationships' section
    #
    #       No change is required here, using the `job_id` as an attribute works perfectly fine
    #       However clients need to be aware that standard syntax will not work.
    create do |attr|
      @created = true
      job = FlightScheduler.app.job_registry.lookup(attr[:job_id])
      step = JobStep.new(
        arguments: attr[:arguments],
        job: job,
        id: job.next_step_id,
        path: attr[:path],
        pty: attr[:pty],
        # NOTE: Sinja's attr hash has been parsed which downcases the keys
        # Instead the data hash needs to be used as it contains the originals
        env: data.fetch(:attributes, {}).fetch(:environment, {})
      )
      next step.id, step
    end
  end

  resource :jobs, pkre: /[\w-]+/ do
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
        property 'script-name', type: :string
        property 'stdout-path', type: :string, description: FlightScheduler::PathGenerator::DESC
        property 'stderr-path', type: :string, description: FlightScheduler::PathGenerator::DESC
        property 'time-limit-spec', type: :string
        property :environment,  type: 'object', additionalProperties: {
          type: 'string',
          description: 'Environment variables for batch jobs (including batch array jobs). Ignored for other job types'
        }
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
        key :summary, 'Create a new job'
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

    swagger_path '/jobs/{id}' do
      parameter do
        key :name, :id
        key :in, :path
        key :description, 'The job ID'
        key :required, true
      end

      operation :get do
        key :summary, 'Return a job'
        key :operationId, :showJob
        parameter in: :query, name: 'long_poll_pending'
        response 200 do
          schema do
            property :data do
              key :'$ref', :Job
            end
          end
        end
      end

      operation :delete do
        key :summary, 'Clear a scheduled job'
        key :operationId, :destroyJob
        response 204
      end
    end

    helpers do
      def find(id)
        FlightScheduler.app.job_registry.lookup(id)
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
      # Returns the scheduler's view of the queue.  This will ensure that the
      # order displayed by the queue command is a reasonable approximation of
      # the order in which they will run.
      FlightScheduler.app.scheduler.queue
    end

    show do
      # Exit early unless doing a long poll
      next resource unless params[:long_poll_pending]

      # Long poll until the resource is no longer pending or timeout.
      task = Async do |t|
        t.with_timeout(FlightScheduler.app.config.polling_timeout) do
          while resource.pending? do
            t.sleep FlightScheduler.app.config.generic_short_sleep.to_f
          end
        end
      rescue Async::TimeoutError
        # NOOP
      end
      task.wait
      next resource
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
        **attr.slice(:cpus_per_node, :gpus_per_node, :memory_per_node, :exclusive, :time_limit_spec)
      )
      if attr[:script] || attr[:arguments] || attr[:script_name]
        job.batch_script = BatchScript.new(
          arguments: attr[:arguments],
          content: attr[:script],
          job: job,
          name: attr[:script_name],
          stderr_path: attr[:stderr_path],
          stdout_path: attr[:stdout_path],
          # Use the original data hash as the keys have not been processed
          # NOTE: They do need type casting from symbols to keys
          env: data.fetch(:attributes, {}).fetch(:environment, {}).transform_keys(&:to_s)
        )
      end
      next job.id, job
    end

    destroy do
      FlightScheduler.app.event_processor.cancel_job(resource)
      nil
    end

    has_one :partition do
      graft(sideload_on: :create) do |rio|
        # This could set the partition to `nil`.  This intended, the job
        # validation will pickup on this and report a suitable error message.
        partition = FlightScheduler.app.partitions.detect { |p| p.name == rio[:id] }
        resource.partition = partition
      end
    end
  end
end
