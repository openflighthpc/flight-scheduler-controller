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

module FlightScheduler::Submission
  # Run a single job step across all nodes allocated to the step's job.
  class JobStep
    def initialize(job_step)
      @job_step = job_step
      @job = @job_step.job
      @allocation = @job.allocation
    end

    def call
      @allocation.nodes.each do |node|
        run_step_on(node)
        # Currently, we only support running PTY sessions on a single node.
        break if @job_step.pty?
      end
    end

    private

    def run_step_on(node)
      execution = @job_step.add_execution(node)
      execution.state = 'INITIALIZING'
      Async.logger.debug("Sending step #{@job_step.display_id} to #{node.name}")
      jobd_connection = FlightScheduler.app.connection_for(:jobd, node.name, @job.id)
      jobd_connection.send_run_step(
        arguments: @job_step.arguments,
        path: @job_step.path,
        pty: @job_step.pty?,
        step_id: @job_step.id,
        environment: @job_step.env,
      )
    rescue
      # XXX What to do here for UnconnectedError errors?
      # 1. abort/cancel the entire job
      # 2. allow the step to run on fewer nodes than we thought
      # 3. something else?
      #
      # XXX What to do for other errors?
      # * Cancel the job on any nodes?
      # * Remove the allocation?
      # * Remove the job from the scheduler?
      # * More?

      Async.logger.warn(
        "Error running step #{@job_step.display_id} on #{node.name}: #{$!.message}"
      )
    end
  end
end
