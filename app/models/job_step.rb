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

# JobStep is a single parallel step for the Job.  It consists of an executable
# and arguments to execute.
#
# The step will be ran in parallel over all nodes that have been allocated to
# the job.
#
class JobStep
  include ActiveModel::Model
  include ActiveModel::Serialization

  attr_accessor :arguments
  attr_accessor :executions
  attr_accessor :id
  attr_accessor :job
  attr_accessor :path
  attr_accessor :pty

  # Additional environment variables to be set in the job step
  attr_accessor :env

  validates :job, presence: true
  validates :path, presence: true
  validate  :validate_env_is_a_hash

  def self.from_serialized_hash(hash)
    new(**hash.stringify_keys.slice(*%w(arguments id job path pty))).tap do |step|
      step.executions = hash['executions'].map do |h|
        Execution.from_serialized_hash(h.merge(job_step: step))
      end
    end
  end

  def initialize(params={})
    self.env = {}
    super
    self.executions ||= []
  end

  def attributes
    {
      arguments: nil, id: nil, path: nil, pty: nil
    }
  end

  def serializable_hash
    super.merge(executions: executions.map(&:serializable_hash))
  end

  def pty?
    !!@pty
  end

  def submitted?
    executions.all?(&:port)
  end

  def add_execution(node)
    Execution.new(
      id: "#{self.job.id}.#{id}.#{node.name}",
      job_step: self,
      node_name: node.name,
    ).tap do |execution|
      self.executions << execution
    end
  end

  def cleanup
    if Async::Task.current?
      Async::IO::Threads.new.async { FileUtils.rm_rf(env_path) }.wait
    else
      FileUtils.rm_rf(env_path)
    end
  end

  def display_id
    "#{job.display_id}.#{id}"
  end

  def env
    # We deliberately don't cache the value here.
    return @env if @env
    serialized_env = 
      if Async::Task.current?
        Async::IO::Threads.new.async { File.read(env_path) }.wait
      else
        File.read(env_path)
      end
    Hash[serialized_env.split("\0").map { |pairs| pairs.split('=', 2) }]
  end

  def execution_for(node_name)
    executions.detect { |exe| exe.node_name == node_name }
  end

  def write
    Async::IO::Threads.new.async do
      FileUtils.mkdir_p(dirname)
      serialized_env = env.map { |k, v| "#{k}=#{v}" }.join("\0")
      File.write(env_path, serialized_env)
      # We don't want the env hanging around in memory.
      self.env = nil
    end.wait
  end

  private

  def dirname
    File.join(FlightScheduler.app.config.spool_dir, 'jobs', job.id.to_s)
  end

  def env_path
    File.join(dirname, "#{id}.environment")
  end

  def validate_env_is_a_hash
    unless env.is_a? Hash
      errors.add(@env, 'must be a hash')
    end
  end

  # An execution of a job step on a single node.
  class Execution
    include ActiveModel::Model
    include ActiveModel::Serialization

    STATES = %w( INITIALIZING RUNNING COMPLETED FAILED ).freeze
    STATES.each do |s|
      define_method("#{s.downcase}?") { self.state == s }
    end

    attr_accessor :id
    attr_accessor :job_step
    attr_accessor :node_name
    attr_accessor :port
    attr_accessor :state

    validates :job_step, presence: true
    validates :node_name, presence: true
    validates :state,
      presence: true,
      inclusion: { within: STATES }

    def self.from_serialized_hash(hash)
      new(**hash.stringify_keys.slice(*%w(id job_step node_name port state)))
    end

    def attributes
      { id: nil, node_name: nil, port: nil, state: nil}
    end
  end
end
