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

  class Reservation < Struct.new(:job, :start_time, :end_time, :nodes)
    def debug
      st = start_time.strftime("%FT%T%:z")
      et = end_time.strftime("%FT%T%:z")
      "job=#{job.display_id} start=#{st} end=#{et} nodes=#{nodes.map(&:name)}"
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

      allocation = allocate_job(candidate)
      if allocation.nil?
        Async.logger.debug("Unable to allocate candidate. Attempting to backfill.")
        reservation = create_reservation(candidate)
        if reservation.nil?
          Async.logger.debug("Unable to create reservation.")
        else
          reservations << reservation
          Async.logger.debug("Current reservations") { reservations.map(&:debug).join("\n") }
        end
        backfilled_allocations = run_backfill_loop(reservations, candidates)
        # ::STDERR.puts "=== backfilled_allocations: #{(backfilled_allocations).inspect rescue $!.message}"
        new_allocations += backfilled_allocations
        break
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
    # XXX
    # 1. Determine which nodes satisfy the job.
    # 2. Determine when the needed resources on the node will become
    #    available.
    # 3. Sort nodes by availability time.
    # 3. Select the required number of nodes which are available first.

    alloc_reg = FlightScheduler.app.allocations

    potential_nodes = candidate.partition.nodes.select do |n|
      n.satisfies_job?(candidate)
    end

    Async.logger.debug("Potential nodes for reservation") { potential_nodes.map(&:name) }

    # For each potential node, grab the first set of jobs that need to
    # complete in order for the candidate to run on it once.
    bar = potential_nodes.reduce([]) do |accum, node|
      # Find the first set of jobs that will allow the candidate to run.
      foo = jobs_in_completion_order(node).detect do |jobs|
        alloc_reg.max_parallel_per_node(candidate, node, excluding_jobs: jobs) > 0
      end
      if foo
        time_when_node_is_available = foo.map(&:end_time).max
        accum << [node, time_when_node_is_available, foo]
      end
      accum
    end

    baz = bar
      .sort_by { |b| b[1] }
      .take(candidate.min_nodes)

    if baz.empty?
      # Not possible to create a reservation for this job.  It is requesting
      # more resources than the partition currently has.
      return nil
    end

    Async.logger.debug("Allocated nodes selected for reservation") {
      baz
        .map{|b| "node=#{b.first.name} available_at=#{b[1]} waiting on jobs=#{b.last.map(&:display_id)}"}
      .join("\n")
    }

    if baz.length < candidate.min_nodes
      # We need to include some currently unused nodes in the reservation.

      extra_nodes = potential_nodes
        .select { |node| FlightScheduler.app.allocations.for_node(node.name).empty? }
        .take(candidate.min_nodes - baz.length)

      Async.logger.debug("Unallocated nodes added to reservation.") {
        extra_nodes.map(&:name).join("\n")
      }
      baz.unshift(*extra_nodes.map { |node| [node, Time.now, []] })
    end

    start_time = baz.last[1]
    end_time = start_time + candidate.time_limit
    nodes = baz.flat_map { |q| q.first }
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
