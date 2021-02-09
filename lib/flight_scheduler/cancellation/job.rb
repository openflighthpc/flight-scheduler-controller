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

module FlightScheduler::Cancellation
  # Kill all running processses for the given job.
  class Job
    def initialize(job)
      @job = job
    end

    def call
      @job.state = 'CANCELLING'
      allocation = FlightScheduler.app.allocations.for_job(@job.id)
      if allocation.nil?
        # The allocation has been cleaned up since we checked the status of
        # the job.  Perhaps the job has just completed.  This is unlikely, but
        # possible.
        return
      end

      allocation.nodes.each do |target_node|
        FlightScheduler.app.processors.job_processor_for(target_node.name, @job.id)
                       .send_job_cancelled
      end
    rescue
      # We've failed to cancel the job!
      # XXX What to do here?
      # XXX Something different for UnconnectedNode errors?

      Async.logger.warn("Error cancelling job #{@job.display_id}: #{$!.message}")
    end
  end
end
