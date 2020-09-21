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
require_relative 'app/serializers'

class App < Sinatra::Base
  include Swagger::Blocks

  configure :development do
    set :logging, Logger::DEBUG
  end

  # Set the header to bypass the over restrictive nature of JSON:API
  before { env['HTTP_ACCEPT'] = 'application/vnd.api+json' }

  register Sinja
  self.prepend SinjaContentPatch

  configure_jsonapi do |c|
    c.validation_exceptions << ActiveModel::ValidationError
    c.validation_formatter = ->(e) do
      e.model.errors.messages
    end
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

  resource :jobs, pkre: /[\w-]+/ do
    swagger_schema :Job do
      property :type, type: :string, enum: ['jobs']
      property :id, type: :string
      property :attributes do
        property 'min-nodes', type: :integer, minimum: 1
        property :state, type: :string, enum: Job::STATES
        property 'script-name', type: :string
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
        property :arguments, type: :array do
          items type: :string
        end
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
        FlightScheduler.app.scheduler.queue.find { |j| j.id == id }
      end

      def validate!
        if @created && resource.validate!
          resource.write_script(@script)
          FlightScheduler.app.event_processor.batch_job_created(resource)
        else
          # TODO: Raise some form of error instead of noop
        end
      end
    end

    index do
      FlightScheduler.app.scheduler.queue
    end

    create do |attr|
      @created = true
      @script = attr[:script]
      job = Job.new(
        arguments: attr[:arguments],
        array: attr[:array],
        id: SecureRandom.uuid,
        job_type: attr[:array].present? ? 'ARRAY_JOB' : 'JOB',
        min_nodes: attr[:min_nodes],
        partition: FlightScheduler.app.default_partition,
        script_provided: @script ? true : false,
        script_name: attr[:script_name],
        state: 'PENDING',
      )
      job.create_array_tasks if job.job_type == 'ARRAY_JOB'
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
      property :'allocated-job' do
        property(:data) { key '$ref', :rioJob }
      end
    end
  end

  swagger_schema :rioNode do
    property :type, type: :string, enum: ['nodes']
    property :id, type: :string
  end
end
