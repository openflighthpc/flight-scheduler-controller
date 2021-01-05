#==============================================================================
# Copyright (C) 2021-present Alces Flight Ltd.
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
  ScriptRunner = Struct.new(:partition) do
    def initialize(*a)
      super
      @mutex = Mutex.new
      @debouncing = {
        tasks: {},
        after: {}
      }
    end

    def insufficient
      debounce_runner partition.insufficient_script_path, type: 'insufficient'
    end

    def excess
      debounce_runner partition.excess_script_path, type: 'excess'
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

    def stdin(action)
      jobs = FlightScheduler.app.job_registry.jobs.select { |j| j.partition == partition }
      jobs_hash = build_jobs_hash(*jobs)
      nodes = partition.nodes
      grouped_types = nodes.group_by(&:type)
      all_types = grouped_types.map { |t, nodes| [t, build_type_hash(t, nodes)] }.to_h
      partition.types.keys.each do |type|
        all_types[type] ||= build_type_hash(type, [])
      end
      {
        partition: partition.name,
        # NOTE: STDIN is intentionally the same for all script types. This is to allows the
        # same script to handle all types if required. Only the 'action' field should differ
        # between script types
        action: action,
        nodes: nodes.map do |node|
          [node.name, {
            type: node.type,
            state: node.state,
            # Only include jobs which appear in the jobs_hash, this prevents lookup issues
            # in the called script
            jobs: FlightScheduler.app.allocations.for_node(node.name)
                                 .map { |a| a.job.id }.select { |id| jobs_hash.key?(id) },
            **Node::NodeAttributes::DELEGATES.map { |k| [k, node.send(k)] }.to_h
          }]
        end.to_h,
        types: all_types,
        jobs: jobs_hash
      }
    end

    def build_type_hash(type_name, nodes)
      count = nodes.length
      base = {
        nodes: nodes.map(&:name),
        count: count,
      }
      if type = partition.types[type_name]
        base.merge!(recognized: true)
        if type.minimum
          base.merge!(minimum: type.minimum, undersubscribed: count < type.minimum)
        else
          base.merge!(minimum: nil, undersubscribed: nil)
        end
        if type.maximum
          base.merge!(maximum: type.maximum, oversubscribed: type.maximum < count)
        else
          base.merge!(maximum: nil, oversubscribed: nil)
        end
      else
        base.merge!(recognized: false, minimum: nil, maximum: nil, undersubscribed: nil, oversubscribed: nil)
      end
      base
    end

    def build_jobs_hash(*jobs)
      jobs.map do |job|
        [job.id, {
          min_nodes: job.min_nodes,
          cpus_per_node: job.cpus_per_node,
          gpus_per_node: job.gpus_per_node,
          memory_per_node: job.memory_per_node,
          state: job.state,
          reason: job.reason_pending
        }]
      end.to_h
    end

    def run(path, type:)
      if path.nil?
        Async.logger.debug "Skipping #{type} script for partition #{partition.name} as it does not exist"
        return
      end

      Async.logger.info "Running (#{type}): #{path}"
      stdin_str = JSON.pretty_generate(stdin(type))
      Async.logger.debug("STDIN:") { stdin_str }
      out, err, status = Open3.capture3({ 'PATH' => ENV['PATH'] }, path, stdin_data: stdin_str,
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
end
