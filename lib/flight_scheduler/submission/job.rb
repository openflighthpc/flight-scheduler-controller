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
      @allocation.nodes.each do |node|
        initialize_job_on(node)
      end
      @job.state = 'RUNNING'
    rescue
      # XXX What to do here for UnconnectedError errors?
      # 1. abort/cancel the job
      # 2. allow the job to run on fewer nodes than we thought
      # 3. something else?
      #
      # XXX What to do for other errors?
      # * What if the batch script fails?
      # * Cancel the job on any nodes?
      # * Remove the allocation?
      # * Remove the job from the scheduler?
      # * More?

      Async.logger.warn("Error running job #{@job.display_id}: #{$!.message}")
      @job.state = 'FAILED'
    end

    private

    def initialize_job_on(node)
      Async.logger.debug("Initializing job #{@job.display_id} on #{node.name}")
      daemon_connection = FlightScheduler.app.connection_for(:daemon, node.name)
      daemon_connection.send_job_allocated(
        @job.id,
        environment: EnvGenerator.call(node, @job),
        username: @job.username,
        time_limit: @job.time_limit
      )
    end
  end
end
