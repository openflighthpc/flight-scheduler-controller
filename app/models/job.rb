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

  STATES = %w( PENDING RUNNING CANCELLED COMPLETED FAILED ).freeze
  STATES.each do |s|
    define_method("#{s.downcase}?") { self.state == s }
  end
  def completed?
    job_type == 'ARRAY_JOB' ?
      array_tasks.all?(&:completed?) :
      self.state == 'COMPLETED'
  end

  JOB_TYPES = %w( JOB ARRAY_JOB ARRAY_TASK ).freeze

  # The index of the task inside the array job.  Only present for ARRAY_TASKS.
  attr_accessor :array_index

  # A reference to the ARRAY_JOB.  Only present for ARRAY_TASKS.
  attr_accessor :array_job

  # A reference to all of an ARRAY_JOB's ARRAY_TASKS.  Only present for
  # ARRAY_JOBS.
  attr_accessor :array_tasks

  attr_accessor :id
  attr_accessor :job_type
  attr_accessor :partition
  attr_accessor :script_name
  attr_accessor :script_provided
  attr_accessor :state
  attr_writer :arguments

  # A list of array indexes.  Only present for ARRAY_JOBS.
  attr_writer :array

  attr_reader :min_nodes

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

  validates :id, presence: true
  validates :min_nodes,
    presence: true,
    numericality: { allow_blank: false, only_integer: true, greater_than_or_equal_to: 1 }
  validates :script_name, presence: true
  validates :script_provided, inclusion: { in: [true] }
  validates :state,
    presence: true,
    inclusion: { within: STATES }
  validates :job_type,
    presence: true,
    inclusion: { within: JOB_TYPES }

  # Validations for ARRAY_JOBS
  validate :validate_array_tasks!, if: ->() { job_type == 'ARRAY_JOB' }

  # Validations for ARRAY_TASKS
  validates :array_index,
    presence: true,
    numericality: { allow_blank: false, only_integer: true, greater_than_or_equal_to: 0 },
    if: ->() { job_type == 'ARRAY_TASK' }
  validates :array_job,
    presence: true, if: ->() { job_type == 'ARRAY_TASK' }

  # Validations for non-ARRAY_TASKS
  validates :array_index,
    absence: true, unless: ->() { job_type == 'ARRAY_TASK' }
  validates :array_job,
    absence: true, unless: ->() { job_type == 'ARRAY_TASK' }

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
    File.join(self.class.job_dir, id.to_s, 'job-script')
  end

  def allocation
    job_id = job_type == 'ARRAY_TASK' ? array_job.id : self.id
    FlightScheduler.app.allocations.for_job(job_id)
  end

  def create_array_tasks
    self.array_tasks ||= @array.split(',').map do |idx|
      self.dup.tap do |task|
        task.array_index = idx
        task.array_job = self
        task.id = SecureRandom.uuid
        task.job_type = 'ARRAY_TASK'
        task.state = 'PENDING'
        task.validate!
      end
    end
  end

  def arguments
    Array(@arguments)
  end

  def hash
    [self.class, id].hash
  end

  def validate_array_tasks!
    array_tasks.each do |task|
      task.validate!
    end
  end
end
