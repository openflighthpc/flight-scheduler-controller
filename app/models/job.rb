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
require 'active_model'

# A Job represents a request for some resource allocation.  It encapsulates
# both the resource request and the state of that request.  Any time that a
# resource allocation is required it *must* be allocated to a Job.
#
# A Job may or may not have a "work payload" at the point that it is created.
#
# * A non-array batch job will have a batch script at the point that it is
#   created.  The script will run on a single node that has been allocated to
#   the Job.
#
# * An array job will have a batch script at the point that it is created. The
#   script will run on all nodes that have been allocated to the Job.
#
# * An interactive job will not have a "work payload" at the point that it is
#   created.  Once the resources are allocated, the user will be able to add a
#   "work payload" that will be ran at that point.
#
class Job
  include ActiveModel::Model
  include ActiveModel::Serialization

  PENDING_REASONS = %w( WaitingForScheduling Priority Resources ).freeze
  STATES = %w( PENDING RUNNING CANCELLING CANCELLED COMPLETED FAILED ).freeze
  # NOTE: If adding new states to TERMINAL_STATES, `update_array_job_state`
  # will need updating too.
  TERMINAL_STATES = %w( CANCELLED COMPLETED FAILED ).freeze
  STATES.each do |s|
    define_method("#{s.downcase}?") { self.state == s }
  end

  JOB_TYPES = %w( JOB ARRAY_JOB ARRAY_TASK ).freeze

  def self.from_serialized_hash(hash)
    partition = FlightScheduler.app.partitions.detect do |p|
      p.name == hash['partition_name']
    end
    array_job = 
      if hash['array_job_id']
        FlightScheduler.app.job_registry.lookup(hash['array_job_id'])
      else
        nil
      end

    # Attributes copied directly from the persisted state.
    attr_names = %w(array array_index id job_type min_nodes reason_pending state username)
    attrs = hash.stringify_keys.slice(*attr_names)

    new(array_job: array_job, partition: partition, **attrs).tap do |job|
      job.instance_variable_set(:@next_step_id, hash['next_step_id'])
      if hash['batch_script']
        job.batch_script = BatchScript.from_serialized_hash(
          hash['batch_script'].merge(job: job)
        )
      end
      if hash['next_array_index'] && job.task_generator
        job.task_generator.next_index = hash['next_array_index']
      end
      if hash['job_steps']
        job.job_steps = hash['job_steps'].map do |h|
          JobStep.from_serialized_hash(h.merge(job: job))
        end
      end
    end
  end

  # The index of the task inside the array job.  Only present for ARRAY_TASKS.
  attr_accessor :array_index

  # A reference to the ARRAY_JOB.  Only present for ARRAY_TASKS.
  attr_accessor :array_job
  attr_accessor :batch_script
  attr_accessor :id
  attr_accessor :job_steps
  attr_accessor :job_type
  attr_accessor :partition
  attr_accessor :state
  attr_accessor :username

  attr_writer :reason_pending

  attr_reader :array_range
  attr_reader :min_nodes

  # Specify the required resources per node
  attr_accessor :cpus_per_node
  attr_accessor :gpus_per_node
  attr_accessor :memory_per_node # In bytes

  # Flags the job must have exclusive access to the nodes
  attr_accessor :exclusive

  def initialize(params={})
    # Sets the default job_type to JOB
    self.job_type = 'JOB'
    @next_step_id_mutex = Mutex.new
    @next_step_id = 0
    @job_steps = []

    # Set the default required resources per node
    @cpus_per_node = 1
    @gpus_per_node = 0
    # TBC Each node has at least 1MB (by default), but each job may not need it
    @memory_per_node = 1024

    super
  end

  def batch_script
    if job_type == 'ARRAY_TASK'
      array_job.batch_script
    else
      @batch_script
    end
  end

  def has_batch_script?
    if job_type == 'ARRAY_TASK'
      array_job.has_batch_script?
    else
      !!batch_script
    end
  end

  # A dummy method that wraps min_nodes until max_nodes is implemented
  # TODO: Implement me!
  def max_nodes
    min_nodes
  end

  # Handle the k and m suffix
  def min_nodes=(raw)
    str = raw.to_s
    @min_nodes = if /\A\d+k\Z/.match?(str)
      str.sub('k', '').to_i * 1024
    elsif /\A\d+m\Z/.match(str)
      str.sub('m', '').to_i * 1048576
    elsif /\A\d+\Z/.match?(str)
      str.to_i
    else
      # This will error during validation with an appropriate error message
      str
    end
  end

  def name
    batch_script&.name || id
  end

  def next_step_id
    @next_step_id_mutex.synchronize { @next_step_id += 1 }
  end

  # Validations for all job types.
  validates :id, presence: true
  validates :state,
    presence: true,
    inclusion: { within: STATES }
  validates :reason_pending,
    inclusion: { within: [*PENDING_REASONS, nil] }

  validates :job_type,
    presence: true,
    inclusion: { within: JOB_TYPES }

  validates :partition, presence: true

  validates :username, presence: true

  # Validations for `JOB`s.
  validates :min_nodes,
    presence: true,
    numericality: { allow_blank: false, only_integer: true, greater_than_or_equal_to: 1 },
    if: ->() { job_type == 'JOB' }

  # Validates the *per_node requirements
  validates :cpus_per_node, numericality: { only_integers: true, greater_than_or_equal_to: 1 }
  validates :gpus_per_node, numericality: { only_integers: true, greater_than_or_equal_to: 0 }
  validates :memory_per_node, numericality: { only_integers: true, greater_than_or_equal_to: 0 }

  validates :exclusive, inclusion: { in: [true, false, nil] }

  validate :validate_batch_script

  # Validations the range for array tasks
  # NOTE: The tasks themselves can be assumed to be valid if the indices are valid
  #       This is because all the other data comes from the ARRAY_JOB itself
  validate :validate_array_range, if: ->() { job_type == 'ARRAY_JOB' }

  # Sets the job as an array task
  def array=(range)
    return if range.nil?
    self.job_type = 'ARRAY_JOB'
    @array_range = FlightScheduler::RangeExpander.new(range.to_s)
  end

  def attributes
    {
      array_index: nil,
      id: nil,
      job_type: nil,
      min_nodes: nil,
      reason_pending: nil,
      state: nil,
      username: nil,
    }
  end

  def serializable_hash
    super.merge(
      next_step_id: @next_step_id,
      partition_name: partition.name,
      job_steps: job_steps.map(&:serializable_hash),
    ).tap do |h|
      if job_type == 'ARRAY_JOB'
        h[:array] = @array_range.compressed
        h[:next_array_index] = task_generator.next_index
      end
      if job_type == 'ARRAY_TASK'
        h[:array_job_id] = array_job.id
      end
      if has_batch_script? && job_type != 'ARRAY_TASK'
        h[:batch_script] = batch_script.serializable_hash
      end
    end
  end

  def reason_pending
    @reason_pending if pending?
  end

  # Must be called at the end of the job lifecycle.
  def cleanup
    job_steps.each(&:cleanup)
    if has_batch_script? && job_type != 'ARRAY_TASK'
      batch_script.cleanup
    end
  end

  def allocated?(include_tasks: false)
    if allocation
      true
    elsif include_tasks && job_type == 'ARRAY_JOB'
      FlightScheduler.app.job_registry.tasks_for(self).any?(&:allocated?)
    else
      false
    end
  end

  def allocation
    FlightScheduler.app.allocations.for_job(self.id)
  end

  def hash
    [self.class, id].hash
  end

  def display_id
    case job_type
    when 'ARRAY_TASK'
      "#{array_job.id}[#{array_index}]"
    else
      id
    end
  end

  def validate_array_range
    @errors.add(:array, 'is not a valid range expression') unless array_range.valid?
  end

  def validate_batch_script
    if !has_batch_script? && job_type == 'ARRAY_JOB'
      @errors.add(:batch_script, 'array jobs must have a batch script')
    elsif has_batch_script? && !batch_script.valid?
      @errors.add(:batch_script, batch_script.errors.full_messages)
    end
  end

  def running_tasks
    if job_type == 'ARRAY_JOB'
      FlightScheduler.app.job_registry.running_tasks_for(self)
    else
      nil
    end
  end

  def state=(new_state)
    @state = new_state
    if job_type == 'ARRAY_TASK' && terminal_state?
      array_job.update_array_job_state
    end
  end

  def task_generator
    if job_type == 'ARRAY_JOB'
      @task_generator ||= FlightScheduler::ArrayTaskGenerator.new(self)
    else
      nil
    end
  end

  def terminal_state?
    TERMINAL_STATES.include?(state)
  end

  def user_env
    has_batch_script? ? batch_script.env : {}
  end

  def time_limit
    partition.default_time_limit
  end

  protected

  def update_array_job_state
    return unless job_type == 'ARRAY_JOB'
    return unless task_generator.finished? || state == 'CANCELLING'

    tasks = FlightScheduler.app.job_registry.tasks_for(self)
    return unless tasks.all?(&:terminal_state?)

    if tasks.any? { |t| t.state == 'FAILED' }
      self.state = 'FAILED'
    elsif tasks.any? { |t| t.state == 'CANCELLED' }
      self.state = 'CANCELLED'
    else
      # They must all be completed then.
      self.state = 'COMPLETED'
    end
  end
end
