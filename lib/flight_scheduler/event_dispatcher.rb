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

# Dispatches life cycle events to plugins.
class FlightScheduler::EventDispatcher
  def initialize
    @queue = []
    @dispatching = false
  end

  def daemon_connected(node, message)
    add_event(:daemon_connected, node, message)
  end

  def job_created(job)
    add_event(:job_created, job)
  end

  def job_cancelled(job)
    add_event(:job_cancelled, job)
  end

  def job_timed_out(job, _)
    add_event(:job_timed_out, job)
  end

  def resources_allocated(new_allocations)
    add_event(:resources_allocated, new_allocations)
  end

  def resource_deallocated(node_name, job_id)
    job = FlightScheduler.app.job_registry[job_id]
    if job.nil?
      Async.logger.error("[event dispatcher] unable to find job:#{job_id}")
      return
    end
    add_event(:resource_deallocated, job, node_name)
  end

  def node_completed_job(job, node_name)
    add_event(:node_completed_job, job, node_name)
  end

  def node_failed_job(job, node_name)
    add_event(:node_failed_job, job, node_name)
  end

  def jobd_connected(job, node_name)
    add_event(:jobd_connected, job, node_name)
  end

  def job_step_created(job_step)
    Async.logger.info("Created job step #{job_step.display_id}")
    add_event(:job_step_created, job_step)
  end

  def job_step_started(job, job_step, node_name, port)
    add_event(:job_step_started, job, job_step, node_name, port)
  end

  def job_step_completed(job, job_step, node_name)
    add_event(:job_step_completed, job, job_step, node_name)
  end

  def job_step_failed(job, job_step, node_name)
    add_event(:job_step_failed, job, job_step, node_name)
  end

  private

  def add_event(event, *args, **opts)
    Async.logger.info("[event dispatcher] queuing #{event}")
    @queue << [event, args, opts]
    Async.logger.debug("[event dispatcher] event queue") { @queue.inspect }

    dispatch_events unless @dispatching

    # Return nil to make sure that the return value isn't relied upon.
    nil
  end

  def dispatch_events
    @dispatching = true
    while !@queue.empty?
      event, args, opts = @queue.pop
      Async.logger.info("[event dispatcher] dispatching #{event}")
      FlightScheduler.app.plugins.event_processors.each do |plugin|
        if plugin.respond_to?(event)
          Async.logger.debug("[event dispatcher] sending #{event} to #{plugin.class.name}")
          plugin.send(event, *args, **opts)
        end
      end
    end
  ensure
    @dispatching = false
  end
end
