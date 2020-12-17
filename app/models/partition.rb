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

require 'open3'

class Partition
  PARTITION_SCHEMA = {
    "type" => "object",
    "additionalProperties" => false,
    "required" => ["name"],
    "properties" => {
      "name" => { "type" => 'string' },
      "default" => { "type" => 'boolean' },
      "nodes" => { "type" => "array", "items" => { "type" => "string" } },
      "max_time_limit" => { "type" => ['string', 'integer'] },
      "default_time_limit" => { "type" => ['string', 'integer'] },
      "node_matchers" => FlightScheduler::NodeMatcher::SCHEMA,
      "dynamic" => {
        "type" => "object",
        "additionalProperties" => false,
        "required" => ["grow_script", "shrink_script", "status_script"],
        "properties" => {
          "grow_script" => { "type" => "string" },
          "shrink_script" => { "type" => "string" },
          "status_script" => { "type" => "string" }
        }
      }
    },
    "if" => { "properties" => { "dynamic" => { "type" => "null" } } },
    "else" => { "required" => ["node_matchers"] }
  }
  ROOT_SCHEMA = {
    "type" => "object",
    "additionalProperties" => false,
    "properties" => {
      "partitions" => {
        "type" => "array",
        "items" => PARTITION_SCHEMA
      }
    }
  }

  SPEC_KEYS   = ['max_time_limit', 'default_time_limit', 'node_matchers']
  OTHER_KEYS  = ['default', 'name']
  VALIDATOR = JSONSchemer.schema(ROOT_SCHEMA)

  Builder = Struct.new(:specs) do
    # NOTE: This is only a syntactic validation of the config. It does not guarantee
    # the resultant partitions are valid semantically
    #
    # The partitions themselves can not be validated until after config load. Doing
    # it during config load creates a circular logic as the libexec_dir hasn't been set
    def valid?
      errors.empty?
    end

    def to_partitions
      specs.map do |spec|
        spec_attrs = spec.slice(*SPEC_KEYS).transform_keys { |k| :"#{k}_spec" }
        other_attrs = spec.slice(*OTHER_KEYS).transform_keys(&:to_sym)
        dynamic_attrs = spec.fetch('dynamic', {}).transform_keys(&:to_sym)
        Partition.new(**other_attrs, **spec_attrs, **dynamic_attrs, static_node_names: spec['nodes'])
      end
    end

    def to_node_names
      specs.reduce([]) { |memo, spec| [*memo, *spec.fetch('nodes', [])] }.uniq
    end

    def errors
      @errors ||= VALIDATOR.validate({ "partitions" => specs }).to_a
    end
  end

  ScriptRunner = Struct.new(:partition) do
    def initialize(*a)
      super
      @mutex = Mutex.new
      @debouncing = {
        tasks: {},
        after: {}
      }
    end

    def grow
      debounce_runner partition.grow_script_path, type: 'grow'
    end

    def shrink
      debounce_runner partition.shrink_script_path, type: 'shrink'
    end

    def status
      debounce_runner partition.status_script_path, type: 'status'
    end

    private

    def debounce_runner(path, type:)
      @mutex.synchronize do
        # Set the next time the debouncer can run according to the minimum period
        @debouncing[:after][type] = Process.clock_gettime(Process::CLOCK_MONOTONIC) + FlightScheduler.app.config.min_debouncing
        task = @debouncing[:tasks][type]
        if task && !task&.finished?
          Async.logger.debug "Skipping partition '#{partition.name}' #{type} script as it is scheduled to run"
          return
        end

        # Start the main script handler
        t = Async do |task|
          Async.logger.info "Scheduling partition '#{partition.name}' #{type} script to be ran"
          start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          finish_time = start_time + FlightScheduler.app.config.max_debouncing

          # Loop until the debouncing condition is met
          loop do
            current_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

            # Exit if the minimum time has elapsed without an additional request
            if current_time > @debouncing[:after][type]
              break
            # Wait the minimum time if it would not exceed the maximum
            elsif finish_time > current_time + FlightScheduler.app.config.min_debouncing
              task.sleep FlightScheduler.app.config.min_debouncing
            # Wait out the minimum time period
            elsif (diff = (finish_time - current_time).to_i) > 0
              task.sleep diff
            # Exit because the maximum time has been exceeded +/- one second
            else
              break
            end
          end

          # Run the script in a new task, allowing the debouncing task to end
          Async { run(path, type: type) }
        end

        # Set the current active task
        @debouncing[:tasks][type] = t
      end
    end

    def stdin
      jobs = FlightScheduler.app.job_registry.jobs.select { |j| j.partition == partition }
      pending_jobs = jobs.select(&:pending?)
      resource_jobs = jobs.select { |j| j.reason_pending == 'Resources' }
      {
        partition: partition.name,
        alloc_nodes: partition.nodes.select { |n| n.state == 'ALLOC' }.map(&:name),
        idle_nodes: partition.nodes.select { |n| n.state == 'IDLE' }.map(&:name),
        down_nodes: partition.nodes.select { |n| n.state == 'DOWN' }.map(&:name),
        jobs: jobs.map do |job|
          [job.id, {
            min_nodes: job.min_nodes,
            cpus_per_node: job.cpus_per_node,
            gpus_per_node: job.gpus_per_node,
            memory_per_node: job.memory_per_node,
            state: job.state,
            reason: job.reason_pending
          }]
        end.to_h,
        pending_jobs: pending_jobs.map(&:id),
        resource_jobs: resource_jobs.map(&:id),
        pending_aggregate: aggregate_jobs(*pending_jobs),
        resource_aggregate: aggregate_jobs(*resource_jobs)
      }
    end

    def aggregate_jobs(*jobs)
      {
        cpus_per_node: jobs.map(&:cpus_per_node).max,
        gpus_per_node: jobs.map(&:gpus_per_node).max,
        memory_per_node: jobs.map(&:memory_per_node).max,
        nodes_per_job: jobs.map(&:min_nodes).max,
        exclusive_nodes_count: jobs.select(&:exclusive).map(&:min_nodes).reduce(&:+),
        shared_cpus_count: jobs.reject(&:exclusive).map do |job|
          job.min_nodes * job.cpus_per_node
        end.reduce(&:+),
        shared_gpus_count: jobs.reject(&:exclusive).map do |job|
          job.min_nodes * job.gpus_per_node
        end.reduce(&:+),
        shared_memory_count: jobs.reject(&:exclusive).map do |job|
          job.min_nodes * job.memory_per_node
        end.reduce(&:+)
      }
    end

    def run(path, type:)
      if path.nil?
        Async.logger.debug "Skipping #{type} script for partition #{partition.name} as it does not exist"
        return
      end

      Async.logger.info "Running (#{type}): #{path}"
      stdin_str = JSON.pretty_generate(stdin)
      Async.logger.debug("STDIN:") { stdin_str }
      out, err, status = Open3.capture3(path, stdin_data: stdin_str,
        close_others: true, unsetenv_others: true, chdir: FlightScheduler.app.config.libexec_dir)
      msg = <<~MSG
        COMMAND (#{type}): #{path}
        STATUS: #{status.exitstatus}
        STDOUT:
        #{out}

        STDERR:
        #{err}
      MSG
      if status.success?
        Async.logger.info { msg }
      else
        Async.logger.warn { msg }
      end
    end
  end

  include ActiveModel::Validations

  attr_reader :name, :nodes, :max_time_limit, :default_time_limit

  validate if: :grow_script_path do
    next if File.executable?(grow_script_path)
    @errors.add(:grow_script, 'must exist and be executable')
  end
  validate if: :shrink_script_path do
    next if File.executable?(shrink_script_path)
    @errors.add(:shrink_script, 'must exist and be executable')
  end
  validate if: :status_script_path do
    next if File.executable?(status_script_path)
    @errors.add(:status_script, 'must exist and be executable')
  end
  validate do
    case FlightScheduler.app.partitions.select { |p| p.name == name }.length
    when 1
      next
    when 0
      @errors.add(:partition, 'must be registered')
    else
      @errors.add(:name, 'must be unique')
    end
  end

  validate if: -> { @max_time_limit_spec } do
    next if max_time_limit
    @errors.add(:max_time_limit_spec, 'is not a valid syntax')
  end

  validate if: -> { @default_time_limit_spec } do
    next if default_time_limit
    @errors.add(:default_time_limit_spec, 'is not a valid syntax')
  end

  validate if: :max_time_limit do
    next if max_time_limit >= default_time_limit
    @errors.add(:max_time_limit_spec, 'must be greater than or equal the default')
  end

  def initialize(
    name:,
    default: false,
    static_node_names: nil,
    default_time_limit_spec: nil,
    max_time_limit_spec: nil,
    node_matchers_spec: nil,
    grow_script: nil,
    shrink_script: nil,
    status_script: nil
  )
    @name = name
    @default = default
    @max_time_limit_spec = max_time_limit_spec
    @default_time_limit_spec = default_time_limit_spec
    @node_matchers_spec = node_matchers_spec
    @static_node_names = static_node_names || []
    @grow_script    = grow_script
    @shrink_script  = shrink_script
    @status_script  = status_script
  end

  def dynamic?
    grow_script_path ? true : false
  end

  # NOTE: This method considers the partition shrinkable if any nodes are IDLE
  # Some additional handling maybe required for node's which are DOWN but not terminated
  # OR if IDLE static nodes count
  def shrinkable?
    dynamic? && nodes.any? { |n| n.state == 'IDLE' }
  end

  # Intentionally not cached to help ensure it remains up to date
  def nodes
    FlightScheduler.app.nodes.for_partition(self)
  end

  def node_match?(node)
    return true if @static_node_names.include? node.name
    return false if matchers.empty?
    matchers.all? { |m| m.match?(node) }
  end

  def grow_script_path
    return nil unless @grow_script
    @grow_script_path ||= File.expand_path(@grow_script, FlightScheduler.app.config.libexec_dir)
  end

  def shrink_script_path
    return nil unless @shrink_script
    @shrink_script_path ||= File.expand_path(@shrink_script, FlightScheduler.app.config.libexec_dir)
  end

  def status_script_path
    return nil unless @status_script
    @status_script_path ||= File.expand_path(@status_script, FlightScheduler.app.config.libexec_dir)
  end

  def script_runner
    @script_runner ||= ScriptRunner.new(self)
  end

  def max_time_limit
    @max_time_limit ||= FlightScheduler::TimeResolver.new(@max_time_limit_spec).resolve
  end

  def default_time_limit
    @default_time_limit ||= if @default_time_limit_spec
      FlightScheduler::TimeResolver.new(@default_time_limit_spec).resolve
    else
      max_time_limit
    end
  end

  def default?
    !!@default
  end

  def ==(other)
    self.class == other.class && name == other.name
  end
  alias eql? ==

  def hash
    ( [self.class, name] + nodes.map(&:hash) ).hash
  end

  private

  def matchers
    @matchers ||= (@node_matchers_spec || {}).map do |key, spec|
      FlightScheduler::NodeMatcher.new(key, **spec)
    end
  end
end
