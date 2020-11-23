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
    iterations = 0
    max_iterations = 100
    @allocation_mutex.synchronize do
      loop do
        if iterations > max_iterations
          break
        else
          iterations += 1
        end

        candidate = queue.detect do |job|
          job.pending? && job.allocation.nil?
        end

        break if candidate.nil?

        if candidate.job_type == 'ARRAY_JOB'
          array_job = candidate
          candidate = array_job.task_generator.next_task
          if candidate.nil?
            # The array job does not have any pending tasks left.  We
            # shouldn't ever get here, but let's handle the case anyway.
            next
          end
        end

        allocation = allocate_job(candidate)
        if allocation.nil?
          if candidate.job_type == 'ARRAY_TASK'
            candidate.array_job.reason_pending = 'Resources'
          else
            candidate.reason_pending = 'Resources'
          end
          break
        else
          if candidate.job_type == 'ARRAY_TASK'
            FlightScheduler.app.job_registry.add(candidate)
            candidate.array_job.task_generator.advance_next_task
          end
          FlightScheduler.app.allocations.add(allocation)
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
      .reject { |j| %w(COMPLETED CANCELLED FAILED).include?(j.state) }
      .sort_by.with_index { |x, idx| [running_jobs_first.call(x), idx] }
  end

  # Attempt to allocate a single job.
  #
  # If the partition has sufficient resources available for the job, create a
  # new +Allocation+ and return.  Otherwise return +nil+.
  def allocate_job(job)
    partition = job.partition
    nodes = partition.available_nodes_for(job)
    return nil if nodes.nil?
    Allocation.new(job: job, nodes: nodes)
  end
end
