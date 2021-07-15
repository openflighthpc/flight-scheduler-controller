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

# Plugin encapsulating when the scheduling algorithm should run.
#
# This plugins is naive in a few ways, it
#
# * Runs the algorithm without any debouncing.
# * Runs the algorithm across all partitions for every event.  A less naive
#   plugin might determine if a subset of partitions should be scheduled.
# * Runs the algorithm synchronously.
class Scheduling
  def self.plugin_name
    'scheduling/builtin'
  end

  class EventProcessor
    def daemon_connected(*args)
      run_scheduler
    end

    def job_created(*args)
      run_scheduler
    end

    def job_cancelled(job)
      run_scheduler unless job.state == 'CANCELLING'
    end

    def resource_deallocated(*args)
      run_scheduler
    end

    private

    def run_scheduler
      Async.logger.info("[scheduling] attempting to allocate rescources to jobs")
      Async.logger.debug("[scheduling] queued jobs") { queued_jobs_debug }
      Async.logger.debug("[scheduling] allocated jobs") { allocated_jobs_debug }
      Async.logger.debug("[scheduling] connected nodes") { connected_nodes_debug }
      Async.logger.debug("[scheduling] allocated nodes") { allocated_nodes_debug }

      new_allocations = FlightScheduler.app.scheduler.allocate_jobs
      new_allocations.each do |allocation|
        allocated_node_names = allocation.nodes.map(&:name).join(',')
        Async.logger.info("[scheduling] allocated #{allocated_node_names} to job #{allocation.job.display_id}")
      end

      unless new_allocations.empty?
        FlightScheduler.app.dispatch_event(:resources_allocated, new_allocations)
      end
    end

    def queued_jobs_debug
      FlightScheduler.app.scheduler.queue.map do |job|
        attrs = %w(cpus_per_node gpus_per_node memory_per_node).reduce("") do |a, attr|
          a << " #{attr}=#{job.send(attr)}"
        end
        "#{job.display_id}:#{attrs} state=#{job.state}"
      end.join("\n")
    end

    def allocated_jobs_debug
      FlightScheduler.app.allocations.each.map do |a|
        job = a.job
        attrs = %w(cpus_per_node gpus_per_node memory_per_node).reduce("") do |a, attr|
          a << " #{attr}=#{job.send(attr)}"
        end
        "#{job.display_id}:#{attrs}"
      end.join("\n")
    end

    def connected_nodes_debug
      FlightScheduler.app.connected_nodes.map do |name|
        node = FlightScheduler.app.nodes[name]
        resources_plugin = FlightScheduler.app.plugins.lookup_type('resources')
        attrs_string =
          if resources_plugin.nil?
            ""
          else
            attrs = resources_plugin.resources_for(node)
            attrs_string = attrs.reduce("") { |a, r| a << " #{r[0]}=#{r[1]}" }
          end
        "#{node.name}:#{attrs_string}"
      end.join("\n")
    end

    def allocated_nodes_debug
      allocated_nodes = FlightScheduler.app.allocations.each
        .map { |a| a.nodes }
        .flatten
        .sort_by(&:name)
        .uniq
      if allocated_nodes.empty?
        "None"
      else
        allocated_nodes.map do |node|
          "#{node.name}: #{FlightScheduler.app.allocations.debug_node_allocations(node.name)}"
        end.join("\n")
      end
    end
  end

  def event_processor
    @event_processor ||= EventProcessor.new
  end
end
