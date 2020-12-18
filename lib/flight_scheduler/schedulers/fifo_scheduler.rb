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
require_relative 'base_scheduler'

class FifoScheduler < BaseScheduler
  FlightScheduler.app.schedulers.register(:fifo, self)

  private

  def run_allocation_loop(candidates)
    # This is a simple FIFO. Only consider the next unallocated job in the
    # FIFO.  If it can be allocated, keep going until we either run out of
    # jobs or find one that cannot be allocated.
    new_allocations = []

    loop do
      candidate = candidates.next
      break if candidate.nil?
      Async.logger.debug("Candidate #{candidate.display_id}")

      allocation = allocate_job(candidate)
      if allocation.nil?
        Async.logger.debug("Unable to allocate candidate.")
        # We're a FIFO scheduler.  As soon as we can't allocate resources to
        # a job we stop trying.  A more complicated scheduler would likely
        # do something more complicated here.
        break
      else
        Async.logger.debug("Candidate allocated.")
        new_allocations << allocation
      end
    rescue StopIteration
      # We've considered all jobs in the queue.
      break
    end

    # We've exited the allocation loop. As this is a FIFO, any jobs left
    # 'WaitingForScheduling' are blocked on priority.  We'll update a few of
    # them to show that is the case.
    #
    # A more complicated scheduler would likely do this whilst iterating
    # over the jobs.
    candidates
      .take(5)
      .select { |job| job.reason_pending == 'WaitingForScheduling' }
      .each { |job| job.reason_pending = 'Priority' }

    new_allocations
  end

  def excess_resources?(partition)
    FlightScheduler.app.job_registry.jobs.any? do |job|
      job.partition == partition && job.reason_pending == 'Resources'
    end
  end
end
