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

  # Determine when `node` will become available to run the `candidate` job.
  class NodeAvailability < Struct.new(:node, :candidate)
    # Return the time at which we can guarantee `node` will be available to
    # run `candidate` or `nil` if no such guarantee is possible.
    def available_at
      if !has_guarantee?
        nil
      elsif jobs_waited_on.empty?
        Time.now
      else
        jobs_waited_on.map(&:end_time).max
      end
    end

    def has_guarantee?
      !jobs_waited_on.nil?
    end

    # If we can determine a set of jobs such that once completed, they will
    # free up enough resources to run `candidate`, return the earliest
    # completing set of such jobs.
    #
    # If no set of jobs can be determined, return nil.
    def jobs_waited_on
      @jobs_waited_on ||=
        begin
          if FlightScheduler.app.allocations.for_node(node.name).empty?
            # No jobs are allocated to this node.  So we're not waiting on any.
            []
          else
            alloc_reg = FlightScheduler.app.allocations
            jobs_as_they_complete(node).detect do |jobs|
              alloc_reg.max_parallel_per_node(candidate, node, excluding_jobs: jobs) > 0
            end
          end
        end
    end

    def debug
      "node=#{first.name} available_at=#{available_at} waiting on jobs_waited_on=#{jobs_waited_on.map(&:display_id)}"
    end

    private

    # Returns an enumerator which yields arrays of jobs running on `node`.
    #
    # The first array yielded includes the first one job expected to complete.
    # The second array yielded includes the first two jobs expected to complete.
    # Etc..
    #
    # If a job does not have a known end time it is not included in any of the
    # yielded array.
    #
    # If jobs have the same expected completion time, their relative order is
    # undefined.
    #
    # These arrays allow us to determine the time at which we can guarantee
    # that the node will be able to run `candidate`.  Or determine that there
    # is no such time.
    def jobs_as_they_complete(node)
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
  end

  class ReservationFinder
    def initialize(candidate)
      @candidate = candidate
    end

    # Return a reservation for the `candidate` if one can be found.
    def call
      selection = select_nodes
      return if selection.nil?

      start_time = selection.map(&:available_at).max
      end_time =
        if @candidate.time_limit
          start_time + @candidate.time_limit
        else
          nil
        end
      nodes = selection.map(&:node)
      Reservation.new(@candidate, start_time, end_time, nodes)
    end

    private

    # Select the nodes for the reservation such that the reservation will be
    # able to start at the earliest time possible.
    def select_nodes
      selection = []

      available_later.sort_by { |time, _| time }.each do |_, candidate_nodes|
        selection += candidate_nodes.take(@candidate.min_nodes - selection.length)
        if selection.length == @candidate.min_nodes
          break
        elsif selection.length + available_now.length >= @candidate.min_nodes
          selection += available_now.take(@candidate.min_nodes - selection.length)
          break
        else
          # loop again and take some more.
        end
      end

      if selection.length < @candidate.min_nodes
        # We could not reserve sufficient resources.
        return nil
      end

      Async.logger.debug("Allocated nodes selected for reservation") {
        selection.map(&:debug).join("\n")
      }

      selection
    end

    # Return a list of `NodeAvailability` that are available now.
    def available_now
      availabilities_grouped_by_time_available[:now]
    end

    # Return the `NodeAvailability` that are available later grouped by the
    # time they are available.
    def available_later
      dup = availabilities_grouped_by_time_available.dup
      dup.delete(:now)
      dup
    end

    # Filter the partition's nodes to those that (1) have enough resources to
    # run `candidate`; and (2) have a guaranteed time at which they can do so.
    # Then group those nodes by the time at which they become available.
    def availabilities_grouped_by_time_available
      @availabilities_grouped_by_time_available ||=
        begin
          partitioned = {
            now: [],
          }
          potential_nodes = @candidate.partition.nodes.select do |n|
            n.satisfies_job?(@candidate)
          end
          Async.logger.debug("Potential nodes for reservation") { potential_nodes.map(&:name) }
          node_availabilities = potential_nodes.map do |node|
            NodeAvailability.new(node, @candidate)
          end

          node_availabilities.each do |availability|
            if !availability.has_guarantee?
              # There is no guarantee of when this node will be available.
            elsif availability.jobs_waited_on.empty?
              partitioned[:now] << availability
            else
              partitioned[availability.available_at] ||= []
              partitioned[availability.available_at] << availability
            end
          end

          partitioned
        end
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
        Async.logger.debug("Unable to allocate candidate #{candidate.display_id}. Attempting to backfill.")
        reservation = ReservationFinder.new(candidate).call
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
        Async.logger.debug("Candidate #{candidate.display_id} allocated")
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
