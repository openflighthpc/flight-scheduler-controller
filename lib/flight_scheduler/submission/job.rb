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
  class Job
    def initialize(allocation)
      @allocation = allocation
      @job = allocation.job
    end

    def call
      @job.state = 'RUNNING'
      @allocation.nodes.each do |node|
        initialize_job_on(node)
      end
      if @job.has_batch_script?
        run_batch_script_on(@allocation.nodes.first)
      end
    rescue
      # XXX What to do here for UnconnectedNode errors?
      # 1. abort/cancel the job
      # 2. allow the job to run on fewer nodes than we thought
      # 3. something else?
      #
      # XXX What to do for other errors?
      # * Cancel the job on any nodes?
      # * Remove the allocation?
      # * Remove the job from the scheduler?
      # * More?

      Async.logger.warn("Error running job #{@job.display_id}: #{$!.message}")
      @job.state = 'FAILED'
    end

    private

    def initialize_job_on(node)
      connection = FlightScheduler.app.daemon_connections.connection_for(node.name)
      Async.logger.debug("Initializing job #{@job.display_id} on #{node.name}")
      connection.write({
        command: 'JOB_ALLOCATED',
        environment: EnvGenerator.call(node, @job),
        job_id: @job.id,
        username: @job.username,
      })
      connection.flush
      Async.logger.debug("Initialized job #{@job.display_id} on #{node.name}")
    end

    def run_batch_script_on(node)
      connection = FlightScheduler.app.daemon_connections.connection_for(node.name)
      Async.logger.debug("Sending batch script for job #{@job.display_id} to #{node.name}")
      pg = FlightScheduler::PathGenerator.build(node, @job)
      script = @job.batch_script
      connection.write({
        command: 'RUN_SCRIPT',
        arguments: script.arguments,
        job_id: @job.id,
        script: script.content,
        stderr_path: pg.render(script.stderr_path),
        stdout_path: pg.render(script.stdout_path),
      })
      connection.flush
      Async.logger.debug("Sent batch script job #{@job.display_id} to #{node.name}")
    end
  end
end
