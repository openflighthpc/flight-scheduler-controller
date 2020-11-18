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

class BaseSerializer
  include JSONAPI::Serializer
end

class PartitionSerializer < BaseSerializer
  def id
    object.name
  end
  attribute :name

  has_many(:nodes) { object.nodes }
end

class NodeSerializer < BaseSerializer
  def id
    object.name
  end

  attribute :name
  attribute :state

  has_one(:allocated) { object.allocation.job }
  # TODO: Implement the partition link
  # has_one :partition
end

class JobSerializer < BaseSerializer
  def id
    case object.job_type
    when 'ARRAY_JOB'
      next_idx = object.task_registry.next_task(false)&.array_index
      last_idx = object.array_range.expanded.last
      if next_idx.nil? || next_idx == last_idx
        "#{object.id}[#{last_idx}]"
      else
        "#{object.id}[#{next_idx}-#{last_idx}]"
      end
    when 'ARRAY_TASK'
      "#{object.array_job.id}[#{object.array_index}]"
    else
      object.id
    end
  end

  attribute :min_nodes
  attribute :state
  attribute(:script_name) { ( object.array_job || object ).batch_script&.name }
  attribute(:reason) { object.reason_pending }
  attribute :username

  has_one :partition
  has_many(:allocated_nodes) { (object.allocation&.nodes || []) }
end

class JobStepSerializer < BaseSerializer
  attribute :arguments
  attribute :path

  has_many(:executions)
end

class JobStep::ExecutionSerializer < BaseSerializer
  attribute(:node) { object.node.name }
  attribute :port
  attribute :state
end
