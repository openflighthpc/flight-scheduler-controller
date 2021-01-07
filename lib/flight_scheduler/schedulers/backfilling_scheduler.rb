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

class BackfillingScheduler < BaseScheduler
  FlightScheduler.app.schedulers.register(:backfilling, self)

  # Stores data for the backfilling algorithm.
  #
  # * Cumulative shortage of nodes to run all skipped jobs.
  # * How many nodes can be allocated to other jobs without delaying any
  #   skipped jobs.
  class Backfill < Struct.new(:shortage, :available)
  end

  private

  def run_allocation_loop(candidates)
    new_allocations = []
    backfill = Backfill.new(0, nil)

    loop do
      candidate = candidates.next
      break if candidate.nil?
      Async.logger.debug("Candidate #{candidate.display_id}")

      if backfill.available && candidate.min_nodes > backfill.available
        Async.logger.debug("Ignoring candidate. It wants more nodes than can be backfilled")
        next
      end

      allocation = allocate_job(candidate)
      if allocation.nil?
        calculate_available_backfill(candidate, backfill)
        Async.logger.debug("Unable to allocate candidate. Backfilling updated") { backfill }
        if backfill.available > 0
          next
        else
          Async.logger.debug("Backfilling currently exhausted")
          break
        end
      else
        if backfill.available
          backfill.available -= allocation.nodes.length
        end
        Async.logger.debug("Candidate allocated. Backfilling updated") { backfill }
        new_allocations << allocation
      end
    rescue StopIteration
      # We've considered all jobs in the queue.
      break
    end

    # We've exited the allocation loop. Any jobs left 'WaitingForScheduling'
    # are blocked on priority.  We'll update a few of them to show that is
    # the case.
    #
    # A more complicated scheduler would likely do this whilst iterating
    # over the jobs.
    candidates
      .take(5)
      .select { |job| job.reason_pending == 'WaitingForScheduling' }
      .each { |job| job.reason_pending = 'Priority' }
    new_allocations
  end

  def calculate_available_backfill(candidate, backfill)
    # The job cannot be allocated at the moment.  Currently, the only
    # possible reason for this is that there are not enough suitable
    # nodes available for allocation.
    #
    # Here we determine how many nodes will become available when jobs
    # complete.  If the next job to complete will result in an *excess*
    # of nodes being available, we have that excess to allocate to
    # backfilled jobs.
    #
    # NOTE: Currently, this does not consider the runtime of the jobs.
    # This has two consequences.
    #
    # 1. We are conservative in backfilling jobs. Perhaps overly
    #    conservative.
    # 2. A currently allocated job exiting *before* its max runtime
    #    expires, we *always* result in the next priority job being
    #    allocated.

    alloc_reg = FlightScheduler.app.allocations

    # The number of nodes required by the candidate.
    required_nodes = candidate.min_nodes

    # The number of nodes currently available to the candidate.
    available_nodes = candidate.partition.nodes.select do |n|
      alloc_reg.max_parallel_per_node(candidate, n) > 0
    end.length

    # Add the node shortage to the existing shortage.  This ensures that
    # backfilling a job only happens if it delays none of the skipped jobs.
    backfill.shortage += required_nodes - available_nodes

    allocated_jobs = FlightScheduler.app.job_registry.jobs
      .select { |j| j.allocated? }

    # For each allocated job, calculate the number of additional nodes that
    # will become available to candidate when the job is deallocated
    # completes.
    #
    # We select the minimum of these to use to update the nodes available for
    # backfilling.
    #
    # NOTE: This doesn't include any other skipped candidates.  Perhaps it
    # should.
    min_node_release = allocated_jobs.map do |job|
      available_without_job = candidate.partition.nodes.select do |n|
        alloc_reg.max_parallel_per_node(candidate, n, excluding_job: job) > 0
      end
      available_without_job.size - available_nodes
    end
      .min || 0

    backfill.available = min_node_release - backfill.shortage
  end
end
