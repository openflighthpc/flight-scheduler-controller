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

module FlightScheduler::EventProcessor
  class << self
    def run_plugins(method, *args, **opts)
      Async.logger.info("[event processor] running plugins for #{method}")
      FlightScheduler.app.plugins.event_processors.each do |plugin|
        if plugin.respond_to?(method)
          Async.logger.debug("[event processor] sending #{method} to #{plugin.class.name}")
          plugin.send(method, *args, **opts)
        end
      end
    end
  end

  class << self
    def daemon_connected(node, message)
      run_plugins(:daemon_connected, node, message)
    end

    def job_created(job)
      run_plugins(:job_created, job)
    end

    def job_cancelled(job)
      run_plugins(:job_cancelled, job)
    end

    def job_timed_out(job, _)
      run_plugins(:job_timed_out, job)
    end

    def resources_allocated(new_allocations)
      run_plugins(:resources_allocated, new_allocations)
    end

    def resource_deallocated(node_name, job_id)
      job = FlightScheduler.app.job_registry[job_id]
      return if job.nil?
      run_plugins(:resource_deallocated, job, node_name)
    end

    def node_completed_job(job, node_name)
      run_plugins(:node_completed_job, job, node_name)
    end

    def node_failed_job(job, node_name)
      run_plugins(:node_failed_job, job, node_name)
    end

    def jobd_connected(job, node_name)
      run_plugins(:jobd_connected, job, node_name)
    end

    def job_step_created(job_step)
      Async.logger.info("Created job step #{job_step.display_id}")
      run_plugins(:job_step_created, job_step)
    end

    def job_step_started(job, job_step, node_name, port)
      run_plugins(:job_step_started, job, job_step, node_name, port)
    end

    def job_step_completed(job, job_step, node_name)
      run_plugins(:job_step_completed, job, job_step, node_name)
    end

    def job_step_failed(job, job_step, node_name)
      run_plugins(:job_step_failed, job, job_step, node_name)
    end
  end
end
