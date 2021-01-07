#==============================================================================
# Copyright (C) 2021-present Alces Flight Ltd.
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

class BaseSerializer
  include JSONAPI::Serializer
  include Swagger::Blocks

  class << self
    attr_reader :subclasses

    def inherited(subclass)
      @subclasses ||= []
      @subclasses << subclass
    end
  end
end

class PartitionSerializer < BaseSerializer
  swagger_schema :Partition do
    key :required, :id
    property :id, type: :string
    property :type, type: :string, enum: ['partitions']
    property :attributes do
      property :name, type: :string
      property :max_time_limit, type: :integer
      property :default_time_limit, type: :integer
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

  def id
    object.name
  end
  attributes :name, :max_time_limit, :default_time_limit

  has_many(:nodes) { object.nodes }
end

class NodeSerializer < BaseSerializer
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

  def id
    object.name
  end

  attribute :name
  attribute :state
  attribute :cpus
  attribute :gpus
  attribute :memory

  has_one(:allocated) { object.allocation.job }
  # TODO: Implement the partition link
  # has_one :partition
end

class JobSerializer < BaseSerializer
  swagger_schema :Job do
    property :type, type: :string, enum: ['jobs']
    property :id, type: :string
    property :attributes do
      property 'min-nodes', type: :integer, minimum: 1
      property :state, type: :string, enum: Job::STATES
      property 'script-name', type: :string
      property :reason, type: :string, enum: Job::PENDING_REASONS, nullable: true
      property :username
      property :time_limit, type: :integer, nullable: true
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

  def id
    case object.job_type
    when 'ARRAY_JOB'
      "#{object.id}#{object.task_generator.remaining_array_range}"
    else
      object.display_id
    end
  end

  attribute :min_nodes
  attribute :state
  attribute(:script_name) { ( object.array_job || object ).batch_script&.name }
  attribute(:reason) { object.reason_pending }
  attribute :username
  attribute :time_limit

  has_one :partition
  has_many(:allocated_nodes) { (object.allocation&.nodes || []) }

  # Defining the environment is tricky as it is node + task specific
  # Eventually a "has_many :environments" relationship maybe added to handle
  # the three-way relationship between job, tasks, and nodes
  #
  # ATM however only the shared_environment is required. This is the portion
  # of the environment which all others share. It is required for the alloc
  # command
  has_one :shared_environment do
    env = FlightScheduler::Submission::EnvGenerator.for_shared(object)
    id = "#{object}.shared"
    Environment.new(id, env)
  end
end

Environment = Struct.new(:id, :hash)
class EnvironmentSerializer < BaseSerializer
  attributes :hash
end

class JobStepSerializer < BaseSerializer
  swagger_schema :JobStep do
    key :required, :id
    property :type, type: :string, enum: ['job-steps']
    property :attributes do
      property :arguments, type: :array do
        items type: :string
      end
      property :path, type: :string
      property :submitted, type: :boolean
    end
    property :relationships do
      property :executions do
        property(:data, type: :array) do
          items { key '$ref', :rioJobStepExecution }
        end
      end
    end
  end

  swagger_schema :rioJobStep do
    property :type, type: :string, enum: ['job-steps']
    property :id, type: :string
  end

  attribute :arguments
  attribute :path
  attribute(:submitted) { object.submitted? }

  has_many(:executions)
end

class JobStep::ExecutionSerializer < BaseSerializer
  swagger_schema :rioJobStepExecution do
    # NOTE: This type might be wrong
    property :type, type: :string, enum: ['job-step-executions']
    property :id, type: :string
  end

  # NOTE: The idiomatic approach would be to specify the `node` as a has_one
  #       relationship. Not doing so prevents the node data from being sideloaded
  #       by the client
  attribute(:node) { object.node_name }
  attribute :port
  attribute :state
end
