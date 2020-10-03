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

  PENDING_REASONS = %w( WaitingForScheduling Priority Resources ).freeze
  STATES = %w( PENDING RUNNING CANCELLING CANCELLED COMPLETED FAILED ).freeze
  STATES.each do |s|
    define_method("#{s.downcase}?") { self.state == s }
  end

  JOB_TYPES = %w( JOB ARRAY_JOB ARRAY_TASK ).freeze

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

  def initialize(params={})
    # Sets the default job_type to JOB
    self.job_type = 'JOB'
    @next_step_id_mutex = Mutex.new
    @next_step_id = 0
    @job_steps = []
    super
  end

  def has_batch_script?
    !!batch_script
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

  validates :username, presence: true

  # Validations for `JOB`s.
  validates :min_nodes,
    presence: true,
    numericality: { allow_blank: false, only_integer: true, greater_than_or_equal_to: 1 },
    if: ->() { job_type == 'JOB' }

  validate :validate_batch_script

  # Validations the range for array tasks
  # NOTE: The tasks themselves can be assumed to be valid if the indices are valid
  #       This is because all the other data comes from the ARRAY_JOB itself
  validate :validate_array_range, if: ->() { job_type == 'ARRAY_JOB' }

  # Sets the job as an array task
  def array=(range)
    return if range.nil?
    self.job_type = 'ARRAY_JOB'
    @array_range = FlightScheduler::RangeExpander.split(range.to_s)
  end

  def task_registry
    @task_registry ||= FlightScheduler::TaskRegistry.new(self)
  end

  def reason_pending
    @reason_pending if pending?
  end

  # Must be called at the end of the job lifecycle.
  def cleanup
    batch_script.cleanup if has_batch_script?
  end

  def allocated?
    allocation ? true : false
  end

  def allocation
    FlightScheduler.app.allocations.for_job(self.id)
  end

  def hash
    [self.class, id].hash
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
end
