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
require 'async/io/threads'

# BatchScript represents a script that a Job may have when it is submitted.
#
# Not all jobs have a batch script, e.g., interactive jobs.  If a job has a
# batch script all of the details for running it are stored here.
#
# The batch script will run on a single node allocated to the job.  It may
# create JobSteps to run on some or all of the nodes allocated to the job.
#
class BatchScript
  include ActiveModel::Model

  DEFAULT_PATH = 'flight-scheduler-%j.out'
  ARRAY_DEFAULT_PATH = 'flight-scheduler-%A_%a.out'

  attr_accessor :arguments
  attr_accessor :content
  attr_accessor :job
  attr_accessor :name
  attr_accessor :env

  attr_writer :stderr_path
  attr_writer :stdout_path

  validates :content, presence: true
  validates :job, presence: true
  validates :name, presence: true
  validate  :validate_env_hash

  def stdout_path
    if @stdout_path.blank? && job.job_type == 'ARRAY_JOB'
      ARRAY_DEFAULT_PATH
    elsif @stdout_path.blank?
      DEFAULT_PATH
    else
      @stdout_path
    end
  end

  def stderr_path
    @stderr_path.blank? ? stdout_path : @stderr_path
  end

  # Must be called at the end of the job lifecycle to remove the script
  def cleanup
    Async::IO::Threads.new.async do
      FileUtils.rm_rf(File.dirname(path))
    end.wait
  end

  def write
    Async::IO::Threads.new.async do
      FileUtils.mkdir_p File.dirname(path)
      File.write(path, content)
      # We don't want the content hanging around in memory.
      self.content = nil
    end.wait
  end

  def content
    # We deliberately don't cache the value here.
    @content || Async::IO::Threads.new.async { File.read(path) }.wait
  rescue Errno::ENOENT
  end

  def path
    File.join(FlightScheduler.app.config.spool_dir, 'jobs', job.id.to_s, 'job-script')
  end

  def validate_env_hash
    if env.is_a? Hash
      unless env.keys.all? { |k| k.is_a? String }
        @errors.add(:env, 'must have string keys')
      end
      unless env.values.all? { |k| k.is_a? String }
        @errors.add(:env, 'must have string values')
      end
    else
      @errors.add(:env, 'must be a hash')
    end
  end
end
