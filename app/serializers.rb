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

  # NOTE: This may not be a job ¯\_(ツ)_/¯
  has_one(:allocated) { object.allocation.job }
  # TODO: Implement the partition link
  # has_one :partition
end

class JobSerializer < BaseSerializer
  # Refresh the task_registry when a new serializer is created. This prevents
  # excessive calls whilst serializing the object
  def initialize(model, *_)
    super
    model.task_registry.next_task if model.job_type == 'ARRAY_JOB'
  end


  attribute :min_nodes
  attribute :state
  attribute :script_name
  attribute(:reason) { object.reason_pending }

  attribute(:first_index) { object.array_range.expanded.first if object.job_type == 'ARRAY_JOB' }
  attribute(:last_index) { object.array_range.expanded.last if object.job_type == 'ARRAY_JOB' }
  attribute(:next_index) { object.task_registry.next_task(false)&.array_index if object.job_type == 'ARRAY_JOB' }

  has_one :partition
  has_many(:allocated_nodes) { (object.allocation&.nodes || []) }

  has_many(:running_tasks) { object.task_registry.running_tasks(false) if object.job_type == 'ARRAY_JOB' }
end

class TaskSerializer < BaseSerializer
  attribute :state
  attribute :min_nodes
  attribute(:index) { object.array_index }

  has_one :job
  has_many(:allocated_nodes) { object.allocation.nodes }
end
