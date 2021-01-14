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
  include ActiveModel::Serialization

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

  def self.from_serialized_hash(hash)
    new(hash)
  end

  def initialize(params={})
    self.env = {}
    super
  end

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

  def attributes
    {
      arguments: nil, name: nil, stdout_path: nil, stderr_path: nil,
    }
  end

  # Must be called at the end of the job lifecycle to remove the script
  def cleanup
    if Async::Task.current?
      Async::IO::Threads.new.async { FileUtils.rm_rf(dirname) }.wait
    else
      FileUtils.rm_rf(dirname)
    end
  end

  def write
    Async::IO::Threads.new.async do
      FileUtils.mkdir_p(dirname)
      File.write(script_path, content)
      # We don't want the content hanging around in memory.
      self.content = nil

      File.write(env_path, JSON.dump(env))
      # We don't want the env hanging around in memory.
      self.env = nil
    end.wait
  end

  def content
    # We deliberately don't cache the value here.
    return @content if @content
    if Async::Task.current?
      Async::IO::Threads.new.async { File.read(script_path) }.wait
    else
      File.read(script_path)
    end
  rescue Errno::ENOENT
  end

  def env
    # We deliberately don't cache the value here.
    return @env if @env
    json_env =  if Async::Task.current?
                  Async::IO::Threads.new.async { File.read(env_path) }.wait
                else
                  File.read(env_path)
                end
    JSON.load(json_env)
  end

  private

  def dirname
    File.join(FlightScheduler.app.config.spool_dir, 'jobs', job.id.to_s)
  end

  def script_path
    File.join(dirname, 'job-script')
  end

  def env_path
    File.join(dirname, 'environment.json')
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
