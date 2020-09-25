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

class Job
  include ActiveModel::Model

  class << self
    attr_reader :job_dir
  end

  REASONS = %w( WaitingForScheduling Priority Resources ).freeze
  STATES = %w( PENDING RUNNING CANCELLING CANCELLED COMPLETED FAILED ).freeze
  STATES.each do |s|
    define_method("#{s.downcase}?") { self.state == s }
  end

  # TODO: Check if the complexity in the complete? method is required
  def completed?
    if job_type == 'ARRAY_JOB'
      return false unless task_registry.finished?
      task_registry.past_tasks.reduce(true) do |memo, task|
        memo && task.complete?
      end
    else
      self.state == 'COMPLETED'
    end
  end

  JOB_TYPES = %w( JOB ARRAY_JOB ARRAY_TASK ).freeze

  # The index of the task inside the array job.  Only present for ARRAY_TASKS.
  attr_accessor :array_index

  # A reference to the ARRAY_JOB.  Only present for ARRAY_TASKS.
  attr_accessor :array_job

  # The node an individual task is ran on. This is not used by the "job" variety of Job
  # NOTE: Do not expose this publicly! It is solely used by the scheduler to track
  #       which task is running where
  attr_accessor :task_node

  attr_accessor :id
  attr_accessor :job_type
  attr_accessor :partition
  attr_accessor :script_name
  attr_accessor :script_provided
  attr_accessor :state

  attr_writer :reason
  attr_writer :arguments

  attr_reader :min_nodes
  attr_reader :array_range

  def initialize(params={})
    # Sets the default job_type to JOB
    self.job_type = 'JOB'
    super
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

  # Validations for all job types.
  validates :id, presence: true
  validates :state,
    presence: true,
    inclusion: { within: STATES }
  validates :reason,
    inclusion: { within: [*REASONS, nil] }

  validates :job_type,
    presence: true,
    inclusion: { within: JOB_TYPES }

  # Validations for `JOB`s.
  validates :min_nodes,
    presence: true,
    numericality: { allow_blank: false, only_integer: true, greater_than_or_equal_to: 1 },
    if: ->() { job_type == 'JOB' }
  validates :script_name, presence: true, if: ->() { job_type == 'JOB' }
  validates :script_provided, inclusion: { in: [true] },
    if: ->() { job_type == 'JOB' }

  # Validations the range for array tasks
  # NOTE: The tasks themselves can be assumed to be valid if the indices are valid
  #       This is because all the other data comes from the ARRAY_JOB itself
  validate :validate_array_range, if: ->() { job_type == 'ARRAY_JOB' }

  # Validations for `ARRAY_TASK`s
  validates :array_index,
    presence: true,
    numericality: { allow_blank: false, only_integer: true, greater_than_or_equal_to: 0 },
    if: ->() { job_type == 'ARRAY_TASK' }
  validates :array_job,
    presence: true, if: ->() { job_type == 'ARRAY_TASK' }

  # Validations for `JOB`s and `ARRAY_JOB`s.
  validates :array_index,
    absence: true, unless: ->() { job_type == 'ARRAY_TASK' }
  validates :array_job,
    absence: true, unless: ->() { job_type == 'ARRAY_TASK' }

  # Sets the job as an array task
  def array=(range)
    return if range.nil?
    self.job_type = 'ARRAY_JOB'
    @array_range = FlightScheduler::RangeExpander.split(range.to_s)
  end

  def task_registry
    @task_registry ||= FlightScheduler::TaskRegistry.new(self)
  end

  def reason
    @reason if pending?
  end

  # Must be called at the end of the job lifecycle to remove the script
  def cleanup
    FileUtils.rm_rf File.dirname(script_path)
  end

  def write_script(content)
    FileUtils.mkdir_p File.dirname(script_path)
    File.write script_path, content
  end

  def read_script
    File.read script_path
  end

  def script_path
    if job_type == 'ARRAY_TASK'
      array_job.script_path
    else
      File.join(self.class.job_dir, id.to_s, 'job-script')
    end
  end

  def allocated?
    allocation ? true : false
  end

  def allocation
    FlightScheduler.app.allocations.for_job(self.id)
  end

  # NOTE: Is wrapping the arguments in an array required?
  #       Confirm the documentation is correct
  def arguments
    Array(@arguments)
  end

  def hash
    [self.class, id].hash
  end

  def validate_array_range
    @errors.add(:array, 'is not a valid range expression') unless array_range.valid?
  end
end
