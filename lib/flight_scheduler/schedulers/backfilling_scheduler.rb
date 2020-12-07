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

class BackfillingScheduler
  FlightScheduler.app.schedulers.register(:backfilling, self)

  class InvalidJobType < RuntimeError; end

  # Stores data for the backfilling algorithm.
  #
  # * Cumulative shortage of nodes to run all skipped jobs.
  # * How many nodes can be allocated to other jobs without delaying any
  #   skipped jobs.
  class Backfill < Struct.new(:shortage, :available)
  end

  attr_reader :queue

  def initialize
    @allocation_mutex = Mutex.new
  end

  # Allocate any jobs that can be scheduled.
  #
  # In order for a job to be scheduled, the partition must contain sufficient
  # available resources to meet the job's requirements.
  def allocate_jobs
    new_allocations = []
    @allocation_mutex.synchronize do
      backfill = Backfill.new(0, nil)
      candidates = self.candidates

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
    end
    new_allocations
  end

  def queue
    # For the backfilling scheduler, the queue is sorted and slightly filtered
    # view of the registry of jobs.
    #
    # The sorting and filtering is primarily intended for display purposes,
    # but also using it for processing ensures that the jobs are processed in
    # the order in which the queue output suggests.
    #
    # The jobs are sorted to ensure that running jobs are placed higher in the
    # queue output than pending jobs.
    #
    # The jobs are filtered to ensure that array jobs without any pending
    # tasks are not included.
    #
    # A more complicated scheduler may implement a priority queue and need to
    # keep its own record of the queued jobs rather than using job registry.
    running_jobs_first = lambda { |job| job.reason_pending.nil? ? -1 : 1 }

    # NOTE: We rely on the sort being stable to ensure that earlier added jobs
    # are considered prior to later added jobs.
    FlightScheduler.app.job_registry.jobs
      .reject { |j| j.job_type == 'ARRAY_JOB' && j.task_generator.finished? }
      .reject { |j| j.terminal_state? }
      .sort_by.with_index { |x, idx| [running_jobs_first.call(x), idx] }
  end

  # Attempt to allocate a single job.
  #
  # If the partition has sufficient resources available for the job, a
  # new +Allocation+ is added to the allocations registry and returned.
  #
  # If an allocation is created for an `ARRAY_TASK`, it is added to the job
  # registry and the associated task generator advanced.
  #
  # If there are insufficient resources available to allocate to the job,
  # the job's `reason_pending` is updated and +nil+ is returned.
  #
  # NOTE: It is expected that this will not differ between schedulers.
  def allocate_job(job, reason: 'Resources')
    raise InvalidJobType, job if job.job_type == 'ARRAY_JOB'

    # Generate an allocation for the job
    nodes = job.partition.nodes
    FlightScheduler::LoadBalancer.new(job: job, nodes: nodes).allocate.tap do |allocation|
      if allocation.nil?
        if job.job_type == 'ARRAY_TASK'
          job.array_job.reason_pending = reason
        else
          job.reason_pending = reason
        end
        nil
      else
        if job.job_type == 'ARRAY_TASK'
          FlightScheduler.app.job_registry.add(job)
          job.array_job.task_generator.advance_next_task
        end
        FlightScheduler.app.allocations.add(allocation)
      end
    end
  end

  def candidates
    # The maximum number of queued jobs to consider.
    max_jobs_to_consider = 50
    considered = 0

    Enumerator.new do |yielder|
      queue.each do |job|
        considered += 1
        if considered > max_jobs_to_consider
          break
        end
        next unless job.pending? && job.allocation.nil?
        max_time_limit = job.partition.max_time_limit
        if max_time_limit && job.time_limit.nil? || job.time_limit > max_time_limit
          job.reason_pending = 'PartitionTimeLimit'
          next
        end

        if job.job_type == 'ARRAY_JOB'
          previous_task = nil
          loop do
            next_task = job.task_generator.next_task
            if next_task == previous_task
              # We failed to schedule the ARRAY_TASK.  Move onto the next
              # ARRAY_JOB or JOB.
              break
            elsif next_task.nil?
              # We've exhausted the ARRAY_JOB.  Move onto the next ARRAY_JOB
              # or JOB.
              break
            else
              previous_task = next_task
              yielder << next_task
            end
          end
        else
          yielder << job
        end
      end
    end
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
