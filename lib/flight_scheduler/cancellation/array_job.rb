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
  # Kill all running processses for the given array job.
  class ArrayJob
    def initialize(job)
      @job = job
    end

    def call
      while task = @job.task_registry.next_task
        task.state = 'CANCELLED'
      end

      running_tasks = @job.task_registry.running_tasks
      running_tasks.each do |task|
        allocation = task.allocation

        # The allocation has been cleaned up since the job was cancelled,
        # However other tasks may still be allocated
        next unless allocation

        begin
          allocation.nodes.each do |target_node|
            connection = FlightScheduler.app.daemon_connections.connection_for(target_node.name)
            connection.write({
              command: 'JOB_CANCELLED',
              job_id: task.id,
            })
            connection.flush
            Async.logger.debug(
              "Job cancellation for task #{task.array_index} of job #{@job.id} " +
              "sent to #{target_node.name}"
            )
          end
        rescue
          # We've failed to cancel one of the array tasks!
          # XXX What to do here?
          # XXX Something different for UnconnectedNode errors?

          Async.logger.warn(
            "Error cancelling task #{task.array_index} of job #{@job.id}: #{$!.message}"
          )
        else
          task.state = 'CANCELLED'
        end
      end
      @job.state = 'CANCELLED'
    end
  end
end
