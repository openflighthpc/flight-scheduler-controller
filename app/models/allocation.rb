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

class Allocation
  include ActiveModel::Model
  include ActiveModel::Serialization

  class  MissingNodeError < RuntimeError; end

  # The job that this allocation has been made for.
  attr_reader :job

  def self.from_serialized_hash(hash)
    new(
      job: FlightScheduler.app.job_registry.lookup(hash['job_id'], with_lock: false),
      node_names: hash['node_names'],
    )
  end

  validate do
    if ! @node_names.is_a?(Array)
      errors.add :node_names, 'Must be an array'
    elsif @node_names.empty?
      errors.add :node_names, 'Must contain at least one node'
    elsif @node_names.any? { |n| ! FlightScheduler.app.nodes[n] }
      errors.add :node_names, 'Must contain nodes that are within the registry'
    end
  end

  validate do
    if job.is_a? Job
      errors.add :job, 'Must be valid' unless job.valid?
    else
      errors.add :job, 'Must be a job'
    end
  end

  def initialize(job:, node_names:)
    @job = job
    @node_names = node_names
  end

  def nodes
    @nodes ||= @node_names.map do |node_name|
      FlightScheduler.app.nodes[node_name]
    end
  end

  def remove_node(node_name)
    @nodes = nil
    @node_names.delete(node_name)
  end

  # Used to make a copy of the allocation when adding to the AllocationRegistry
  # This is required as the registry will modify its copy of the Allocation
  # which risks breaking external references
  def dup
    self.class.new(job: job, node_names: @node_names.dup)
  end

  def partition
    @job.partition
  end

  def ==(other)
    self.class == other.class &&
      job == other.job &&
      nodes == other.nodes
  end
  alias :eql? :==

  def hash
    ( [self.class, job.hash] + nodes.map(&:hash) ).hash
  end

  def serializable_hash
    {
      job_id: job.id,
      node_names: @node_names,
    }
  end

  def debug
    "#{job.debug_id}:#{@node_names}"
  end
end
