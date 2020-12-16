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
require 'concurrent'

# Registry of all jobs.
#
# This class provides a single location that can be queried for the current
# state of known jobs.  Historic jobs are eventually lost.
#
class FlightScheduler::JobRegistry
  class DuplicateJob < RuntimeError; end

  def initialize
    @lock = Concurrent::ReadWriteLock.new

    # Map of job id to job.
    # NOTE: Hashes enumerate their values in the order that the corresponding
    # keys were inserted.  We make use of this.
    @jobs = {}

    # Map of ARRAY_JOB id to list of tasks.
    @tasks = {}
  end

  def add(job)
    @lock.with_write_lock do
      if @jobs[job.id]
        raise DuplicateJob, job.id, job.display_id
      end
      @jobs[job.id] = job
      if job.job_type == 'ARRAY_TASK'
        @tasks[job.array_job.id] ||= []
        @tasks[job.array_job.id] << job
      end
    end
    Async.logger.debug("Added job #{job.display_id} (#{job.id}) to job registry")
  end

  def remove_old_jobs
    Async.logger.info("Checking for old jobs to remove")
    jobs.each do |job|
      # ARRAY_TASKs are cleaned up along with the ARRAY_JOB.
      next if job.job_type == 'ARRAY_TASK'

      if job.terminal_state?
        begin
          # Its possible that we're still processing the request to deallocate
          # the job.  If that's the case, we'll clean this job up another time.
          if job.allocated?
            Async.logger.debug("Skipping job:#{job.display_id} due to existing allocation")
          else
            delete(job)
            Async.logger.debug("Removed job:#{job.display_id} from job registry")
          end
        rescue
          Async.logger.warn("Failed processing potentially old job:#{job.display_id}") { $! }
        end
      end
    end
  end

  def [](job_id)
    @lock.with_read_lock do
      @jobs[job_id]
    end
  end
  alias_method :lookup, :[]

  def jobs
    @lock.with_read_lock do
      @jobs.values
    end
  end

  def jobs_in_state(states)
    @lock.with_read_lock do
      @jobs.values.select do |job|
        Array(states).include? job.state
      end
    end
  end

  def tasks_for(job)
    @lock.with_read_lock do
      ( @tasks[job.id] || [] )
    end
  end

  def running_tasks_for(job)
    tasks_for(job).select do |task|
      task.running? || (task.allocated? && task.pending?)
    end
  end

  def load
    data = persistence.load
    return if data.nil?
    task_hashes, job_hashes = data.partition do |h|
      h['job_type'] == 'ARRAY_TASK'
    end
    (job_hashes + task_hashes).each do |hash|
      job = Job.from_serialized_hash(hash)
      if job.valid?
        add(job)
      else
        Async.logger.warn("Invalid job loaded: #{job.errors.inspect}")
      end
    end
    jobs.each do |job|
      # NOTE: There is an edge case where the "grow script" would have otherwise
      # ran before the reboot, but not after. This can occur if the default reason
      # in the scheduler is changed to anything other than 'Resources'
      #
      # This does not happen in the FiFoScheduler and BackfillingScheduler, so can
      # be ignored for the time being.
      # Consider revisiting when this no longer holds
      job.partition.script_runner.grow if job.reason_pending == 'Resources'
    end
  rescue
    Async.logger.warn("Error loading job registry: #{$!.message}")
    raise
  end

  def save
    persistence.save(jobs.map(&:serializable_hash))
  end

  private

  def delete(job_or_job_id)
    job = job_or_job_id.is_a?(Job) ? job_or_job_id : lookup(job_or_job_id)
    return if job.nil?

    @lock.with_write_lock do
      if job.job_type == 'ARRAY_JOB'
        @tasks[job.id].each { |job| job.cleanup }
        @jobs.delete_if { |id, j| j.array_job == job }
        @tasks.delete(job.id)
      end
      job.cleanup
      @jobs.delete(job.id)
    end
  end

  def persistence
    @persistence ||= FlightScheduler::Persistence.new('job registry', 'job_state')
  end

  # These methods exist to facilitate testing.
  def clear
    @lock.with_write_lock do
      @jobs.clear
    end
  end

  def size
    @lock.with_read_lock do
      @jobs.size
    end
  end
end
