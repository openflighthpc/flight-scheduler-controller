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

# Abstract base class for schedulers.
class BaseScheduler
  class InvalidJobType < RuntimeError; end

  def initialize
    @allocation_mutex = Mutex.new
  end

  # Allocate any jobs that can be scheduled.
  #
  # In order for a job to be scheduled, the job's partition must contain
  # sufficient available resources to meet the job's requirements.
  def allocate_jobs(partitions: FlightScheduler.app.partitions)
    new_allocations = []
    @allocation_mutex.synchronize do
      Array(partitions).each do |partition|
        new_allocations += run_allocation_loop(candidates(partition))
      end
    end
    new_allocations
  end

  def queue(partition=nil)
    # For some schedulers, the queue is a sorted and slightly filtered view of
    # the registry of jobs.
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
    # keep its own record of the queued jobs rather than using the job
    # registry.
    running_jobs_first = lambda { |job| job.reason_pending.nil? ? -1 : 1 }

    # NOTE: We rely on the sort being stable to ensure that earlier added jobs
    # are considered prior to later added jobs.
    FlightScheduler.app.job_registry.jobs
      .select { |j| partition.nil? || j.partition == partition }
      .reject { |j| j.job_type == 'ARRAY_JOB' && j.task_generator.finished? }
      .reject { |j| j.terminal_state? }
      .sort_by.with_index { |x, idx| [running_jobs_first.call(x), idx] }
  end

  private

  # Loop over the given Enumerator of candidates and allocate accordingly.
  # Return an array of any new allocations.
  #
  # Allocations are created by calling `allocate_job`.
  def run_allocation_loop(candidates)
    raise NotImplementedError
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
        allocation
      end
    end
  end

  # Return an enumerator that yields candidate jobs from the queue.
  #
  # The jobs on the queue are considered in turn.  If it is a JOB it is
  # yielded.
  #
  # If it is an ARRAY_JOB, its ARRAY_TASKs are yielded until either they are
  # exhausted or the yielded ARRAY_TASK is not allocated resources.
  def candidates(partition)
    # The maximum number of queued jobs to consider.
    max_jobs_to_consider = FlightScheduler.app.config.scheduler_max_jobs_considered
    considered = 0

    Enumerator.new do |yielder|
      queue(partition).each do |job|
        next unless job.pending? && job.allocation.nil?
        max_time_limit = job.partition.max_time_limit
        if max_time_limit && job.time_limit.nil? || job.time_limit > max_time_limit
          job.reason_pending = 'PartitionTimeLimit'
          next
        end

        considered += 1
        if considered > max_jobs_to_consider
          break
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
end
