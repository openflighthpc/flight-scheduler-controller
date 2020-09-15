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

require_relative 'app/models'
require_relative 'app/serializers'

class App < Sinatra::Base
  include Swagger::Blocks

  # Set the header to bypass the over restrictive nature of JSON:API
  before { env['HTTP_ACCEPT'] = 'application/vnd.api+json' }

  register Sinja

  # TODO: Replace this with actual configuations
  DUMMY = {
    default: [:node01, :node02, :node03],
    gpus: [:gpu01, :gpu02, :gpu03]
  }
  PARTITIONS = DUMMY.map do |name, nodes|
    Partition.new(name: name, nodes: nodes)
  end
  DEFAULT_SCHEDULAR = Schedular.new(PARTITIONS.first)

  resource :partitions do
    swagger_schema :Partition do
      key :required, :id
      property :id do
        key :type, :integer
      end
      property :type do
        key :type, :string
        key :value, 'partitions'
      end
      property :attributes do
        property :name do
          key :type, :string
        end
        property :nodes do
          key :type, :array
          items { key :type, :string }
        end
      end
      property :relationships do
        property :schedular do
          property :data do
            key :'$ref', :rioSchedular
          end
        end
      end
    end

    swagger_schema :rioPartition do
      property :type do
        key :type, :string
        key :value, :partitions
      end
      property :id do
        key :type, :string
      end
    end

    swagger_path '/partitions' do
      operation :get do
        key :summary, 'All partitions'
        key :description, 'Returns a list of all the partions and related nodes'
        key :operaionId, :indexPartitions
        response 200 do
          schema do
            property :data do
              items do
                key :'$ref', :Partition
              end
            end
          end
        end
      end
    end

    helpers do
      index do
        DUMMY.map do |name, nodes|
          Partition.new(name: name, nodes: nodes)
        end
      end
    end
  end

  resource :schedulars do
    swagger_schema :rioSchedular do
      property :type do
        key :type, :string
        key :value, :schedulars
      end
      property :id do
        key :type, :string
      end
    end
  end

  resource :jobs do
    swagger_schema :Job do
      property :type do
        key :type, :string
        key :value, 'jobs'
      end
      property :id do
        key :type, :string
      end
      property :attributes do
        property :min_nodes do
          key :type, :integer
          key :default, 1
          key :minimum, 1
        end
      end
      property :relationships do
        property :schedular do
          property :data do
            key :'$ref', :rioSchedular
          end
        end
      end
    end

    swagger_schema :newJob do
      property :type do
        key :type, :string
        key :value, 'jobs'
      end
      property :attributes do
        property :min_nodes do
          key :type, :integer
          key :nullable, true
          key :minimum, 1
        end
      end
      property :relationships do
        property :schedular do
          property :data do
            key :'$ref', :rioSchedular
          end
        end
      end
    end

    swagger_path '/jobs' do
      operation :get do
        key :summary, 'Return all the current jobs'
        key :operationId, :indexJobs
        response 200 do
          schema do
            property :data do
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
            key :'$ref', :newJob
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

    helpers do
      def validate!
        resource.ensure_scheduled
      end
    end

    index do
      PARTITIONS.map { |p| p.jobs }.flatten
    end

    create do |attr|
      job = Job.new(
        min_nodes: attr[:min_nodes],
        schedular: DEFAULT_SCHEDULAR
      )
      next job.id, job
    end
  end

  swagger_root do
    key :swagger, '2.0'
    info do
      key :title, 'Flight Scheduler Controller'
      key :description, 'WIP'
      contact do
        key :name, 'Alces Flight'
      end
      license do
        key :name, 'EPL-2.0'
      end
    end
  end
end

class SwaggerApp < Sinatra::Base
  register Sinatra::Cors

  set :allow_origin, '*'
  set :allow_methods, "GET,HEAD,POST"
  set :allow_headers, "content-type,if-modified-since"

  set :expose_headers, "location,link"

  SWAGGER_DOC = Swagger::Blocks.build_root_json([App]).to_json
  get '/' do
    SWAGGER_DOC
  end
end
