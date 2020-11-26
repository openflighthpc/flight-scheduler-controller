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
        raise DuplicateJob, job.id
      end
      @jobs[job.id] = job
      if job.job_type == 'ARRAY_TASK'
        @tasks[job.array_job.id] ||= []
        @tasks[job.array_job.id] << job
      end
    end
    Async.logger.debug("Added job #{job.display_id} to job registry")
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
          Async.logger.warn("Failed processing potentially old job:#{job.display_id}")
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

  def save
    Async.logger.info("Saving job registry")
    job_hashes = jobs.map(&:serializable_hash)
    Async.logger.debug("Job hashes") { job_hashes }
    path = File.join(FlightScheduler.app.config.spool_dir, 'job_state')

    block = lambda do
      # We jump through some hoops to make writing the save state atomic and
      # consistent.
      #
      # 1. Create a copy of the original state, by creating a hard-link to it.
      # 2. Create a tempfile, being careful to make sure we create one that
      #    isn't automatically removed.
      # 3. Write the content to the tempfile.
      # 4. If all the content is written, move the tempfile to the correct
      #    path.
      FileUtils.mkdir_p(File.dirname(path))
      if File.exist?(path)
        FileUtils.cp_lr(path, "#{path}.old", remove_destination: true)
      end
      begin
        tmpfile = Tempfile.create(File.basename(path), File.dirname(path))
        content = job_hashes.to_json
        if tmpfile.write(content) == content.length
          FileUtils.mv(tmpfile.path, path)
        end
      ensure
        tmpfile.close
      end
    end

    if Async::Task.current?
      Async::IO::Threads.new.async do
        block.call
      end.wait
    else
      block.call
    end
  rescue
    Async.logger.warn("Error saving job registry: #{$!.message}")
    raise
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
