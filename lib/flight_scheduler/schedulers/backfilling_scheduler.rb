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

  # Represents a reservation of some nodes for a job.
  class Reservation < Struct.new(:job, :start_time, :end_time, :nodes)
    def debug
      st = start_time.strftime("%FT%T%:z")
      et = end_time.strftime("%FT%T%:z")
      "job=#{job.display_id} start=#{st} end=#{et} nodes=#{nodes.map(&:name)}"
    end
  end

  # Encapsulates when a node will become available to run a particular job.
  #
  # `node` is the node.
  # `jobs` are the jobs which need to complete for it to become available.
  class NodeAvailability < Struct.new(:node, :jobs)
    def available_at
      if jobs.empty?
        Time.now
      else
        jobs.map(&:end_time).max
      end
    end

    def debug
      "node=#{first.name} available_at=#{available_at} waiting on jobs=#{jobs.map(&:display_id)}"
    end
  end

  private

  def run_allocation_loop(candidates)
    new_allocations = []
    reservations = []

    loop do
      candidate = candidates.next
      break if candidate.nil?
      Async.logger.debug("Candidate #{candidate.display_id}")

      allocation = allocate_job(candidate, reservations: reservations)
      if allocation.nil?
        Async.logger.debug("Unable to allocate candidate. Attempting to backfill.")
        reservation = create_reservation(candidate)
        if reservation.nil?
          Async.logger.debug("Unable to create reservation. Continuing normal allocation loop.")
        else
          reservations << reservation
          Async.logger.debug("Current reservations") { reservations.map(&:debug).join("\n") }
          backfilled_allocations = run_backfill_loop(reservations, candidates)
          new_allocations += backfilled_allocations
          break
        end
      else
        Async.logger.debug("Candidate allocated")
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

  def create_reservation(candidate)
    # 1. Determine which nodes satisfy the job.
    # 2. For each node, determine the earliest point at which the needed
    #    resources on the node will become available.
    # 3. Select the required number of nodes which are available first.
    # 4. If necessary, add currently unallocated nodes to the selection.

    alloc_reg = FlightScheduler.app.allocations

    potential_nodes = candidate.partition.nodes.select do |n|
      n.satisfies_job?(candidate)
    end

    Async.logger.debug("Potential nodes for reservation") { potential_nodes.map(&:name) }

    # For each potential node, grab the first set of jobs that need to
    # complete in order for the candidate to run on it once.
    availabilities = potential_nodes.reduce([]) do |accum, node|
      jobs = jobs_in_completion_order(node).detect do |jobs|
        alloc_reg.max_parallel_per_node(candidate, node, excluding_jobs: jobs) > 0
      end
      if jobs
        accum << NodeAvailability.new(node, jobs)
      end
      accum
    end

    availabilities = availabilities
      .sort_by(&:available_at)
      .take(candidate.min_nodes)

    if availabilities.empty?
      # Not possible to create a reservation for this job.  It is requesting
      # more resources than the partition currently has.
      candidate.reason_pending = 'Resources'
      return nil
    end

    Async.logger.debug("Allocated nodes selected for reservation") {
      availabilities.map(&:debug).join("\n")
    }

    if availabilities.length < candidate.min_nodes
      # We need to include some currently unused nodes in the reservation.

      extra_nodes = potential_nodes
        .select { |node| FlightScheduler.app.allocations.for_node(node.name).empty? }
        .take(candidate.min_nodes - availabilities.length)

      Async.logger.debug("Unallocated nodes added to reservation.") {
        extra_nodes.map(&:name).join("\n")
      }
      availabilities.unshift(*extra_nodes.map { |node| NodeAvailability.new(node, []) })
    end

    start_time = availabilities.last.available_at
    end_time = start_time + candidate.time_limit
    nodes = availabilities.map(&:node)
    Reservation.new(candidate, start_time, end_time, nodes)
  end

  # Yield an array of jobs in the order they are expected to complete.
  #
  # The first array yielded includes the first one job expected to complete.
  # The second array yielded includes the first two jobs expected to complete.
  # Etc..
  #
  # If jobs have the same expected completion time, their relative order is
  # undefined.
  def jobs_in_completion_order(node)
    jobs_ordered_by_end = FlightScheduler.app.allocations.for_node(node.name)
      .map { |alloc| alloc.job }
      .reject { |j| j.time_limit.nil? }
      .sort_by { |j| j.time_limit }
    Enumerator.new do |yielder|
      jobs_ordered_by_end.length.times do |i|
        yielder << jobs_ordered_by_end.slice(0, i + 1)
      end
    end
  end

  def run_backfill_loop(reservations, candidates)
    backfilled_allocations = []

    loop do
      candidate = candidates.next
      break if candidate.nil?
      Async.logger.debug("Backfill candidate #{candidate.display_id}")

      allocation = allocate_job(candidate, reservations: reservations)
      if allocation.nil?
        Async.logger.debug("Unable to backfill candidate.")
        # We deliberately don't break here as we want to attempt to backfill
        # the next candidate.
      else
        Async.logger.debug("Candidate backfilled")
        backfilled_allocations << allocation
      end
    rescue StopIteration
      # We've considered all jobs in the queue.
      break
    end

    backfilled_allocations
  end
end
