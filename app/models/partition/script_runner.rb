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
  SHARED_DEBOUNCING = Hash.new { |_, k| k }
  SHARED_DEBOUNCING.merge!(
    'excess'        => 'excess_insufficient',
    'insufficient'  => 'excess_insufficient'
  )

  ScriptRunner = Struct.new(:partition) do
    def initialize(*a)
      super
      @mutex = Mutex.new
      @cooldown = Hash.new(Process.clock_gettime(Process::CLOCK_MONOTONIC) - 1)
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
        key = SHARED_DEBOUNCING[type]
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        interval = @cooldown[key] - now

        if interval < 0
          @cooldown[key] = now + FlightScheduler.app.config.debouncing_cooldown

          # Run the script asynchronously to prevent it blocking
          Async do
            run(path, type: type)
          end
        else
          Async.logger.debug <<~DEBUG.squish
            Skipping '#{type}' script on partition '#{partition.name}' as it has been
            debounced (group: #{key}). Remaining cooldown: #{interval.to_i} seconds.
          DEBUG
          return
        end
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
        nodes: build_nodes_hash(nodes),
        types: all_types,
        jobs: jobs_hash
      }
    end

    def build_nodes_hash(nodes)
      nodes.map do |node|
        # NOTE: The node maybe part of other partitions and thus have other jobs.
        # These other jobs are not serialized to provide a limit on the data provided
        # to the script. Instead the other job ids are provided separately
        jobs, others = FlightScheduler.app.allocations.for_node(node.name)
                                      .map(&:job)
                                      .partition { |j| j.partition == partition }

        node_attributes = FlightScheduler.app.plugins.lookup('core/node_attributes')
        attrs =
          if node_attributes.nil?
            {}
          else
            node_attributes.resources_for(node)
          end

        [node.name, {
          type: node.type,
          state: node.state,
          # Only include jobs which appear in the jobs_hash, this prevents lookup issues
          # in the called script
          jobs: jobs.map(&:id),
          other_jobs: others.map(&:id),
          **attrs
        }]
      end.to_h
    end

    def build_type_hash(type_name, nodes)
      count = nodes.reject { |n| n.state == 'DOWN' }.length
      base = {
        name: type_name,
        nodes: nodes.map(&:name),
        count: count,
        known_count: nodes.length
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
          id: job.id,
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
        Async.logger.debug "[script runner] skipping #{type} script for partition #{partition.name}; it does not exist"
        return
      end

      Async.logger.info "[script runner] running (#{type}): #{path}"
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
        Async.logger.info("[script runner]") { msg }
      else
        Async.logger.warn("[script runner]") { msg }
      end
    end
  end
end
