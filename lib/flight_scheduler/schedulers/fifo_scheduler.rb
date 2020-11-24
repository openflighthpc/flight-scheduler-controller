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

class FifoScheduler
  FlightScheduler.app.schedulers.register(:fifo, self)

  class InvalidJobType < RuntimeError; end

  attr_reader :queue

  def initialize
    @allocation_mutex = Mutex.new
  end

  # Allocate any jobs that can be scheduled.
  #
  # In order for a job to be scheduled, the partition must contain sufficient
  # available resources to meet the job's requirements.
  def allocate_jobs
    # This is a simple FIFO. Only consider the next unallocated job in the
    # FIFO.  If it can be allocated, keep going until we either run out of
    # jobs or find one that cannot be allocated.

    new_allocations = []
    @allocation_mutex.synchronize do
      loop do
        # Select the next pending and unallocated `JOB` or `ARRAY_TASK` in the
        # queue.  Once this completes, `candidate` will be one of `nil`, a
        # `JOB` or an `ARRAY_TASK`.
        candidate = queue.reduce(nil) do |memo, job|
          break memo if memo
          next unless job.pending? && job.allocation.nil?
          if job.job_type == 'ARRAY_JOB'
            job.task_generator.next_task
          else
            job
          end
        end

        break if candidate.nil?

        allocation = allocate_job(candidate)
        if allocation.nil?
          # We're a FIFO scheduler.  As soon as we can't allocate resources to
          # a job we stop trying.  A more complicated scheduler would likely
          # do something more complicated here.
          break
        else
          new_allocations << allocation
        end
      end

      # We've exited the allocation loop. As this is a FIFO, any jobs left
      # 'WaitingForScheduling' are blocked on priority.  We'll update a few of
      # them to show that is the case.
      #
      # A more complicated scheduler would likely do this whilst iterating
      # over the jobs.
      queue[0...5]
        .select { |job| job.pending? && job.allocation.nil? }
        .select { |job| job.reason_pending == 'WaitingForScheduling' }
        .each { |job| job.reason_pending = 'Priority' }
    end
    new_allocations
  end

  def queue
    # For the FIFO scheduler, the queue is sorted and slightly filtered view
    # of the registry of jobs.
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

    partition = job.partition
    nodes = partition.available_nodes_for(job)
    if nodes.nil?
      if job.job_type == 'ARRAY_TASK'
        job.array_job.reason_pending = reason
      else
        job.reason_pending = reason
      end
      nil
    else
      Allocation.new(job: job, nodes: nodes).tap do |allocation|
        if job.job_type == 'ARRAY_TASK'
          FlightScheduler.app.job_registry.add(job)
          job.array_job.task_generator.advance_next_task
        end
        FlightScheduler.app.allocations.add(allocation)
      end
    end
  end
end
