module FlightScheduler
  class Plugins
    class Scheduling
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
            FlightScheduler.app.processors.event.resources_allocated(new_allocations)
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
          FlightScheduler.app.processors.connected_nodes.map do |name|
            node = FlightScheduler.app.nodes[name]
            attrs = %w(cpus gpus memory).reduce("") { |a, attr| a << " #{attr}=#{node.send(attr)}" }
            "#{node.name}:#{attrs}"
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

    FlightScheduler.app.plugins.register('scheduling/builtin', Scheduling.new)
  end
end
