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

class FlightScheduler::TaskRegistry
  attr_reader :job

  def initialize(job)
    @job = job
    @next_task = task_enum.next
    @running_tasks = []
    @past_tasks = []
    @mutex = Mutex.new
  end

  def all_tasks(update = true)
    refresh if update
    [@next_task, *@running_tasks, *@past_tasks]
  end

  def next_task(update = true)
    refresh if update
    @next_task
  end

  def running_tasks(update = true)
    refresh if update
    @running_tasks
  end

  def past_tasks(update = true)
    refresh if update
    @past_tasks
  end

  def limit?(update = true)
    refresh if update
    @running_tasks.length >= job.min_nodes
  end

  def finished?(update = true)
    refresh if update
    if @next_task
      false
    else
      @running_tasks.empty?
    end
  end

  private

  def refresh
    @mutex.synchronize do
      @running_tasks.select! do |task|
        # Transition "finalised" tasks from running to past
        if task.running? || (task.allocated? && task.pending?)
          true
        else
          @past_tasks << task
          false
        end
      end

      # End the update if there are no more tasks
      return if @next_task.nil?

      # End the update as the next task has not "started"
      # NOTE: Tasks with allocated nodes are considered as good as started
      return if @next_task.pending? &&  !@next_task.allocated?

      # Transition the next task to either running or past
      if @next_task.running? || @next_task.allocated?
        @running_tasks << @next_task
      else
        @past_tasks << @next_task
      end

      # Build the new next task or end the registry
      begin
        @next_task = task_enum.next
      rescue StopIteration
        @next_task = nil
      end
    end
  end

  def task_enum
    @task_enum ||= Enumerator.new do |yielder|
      job.array_range.each do |idx|
        yielder << Task.new(
          array_index: idx,
          array_job: job,
          id: SecureRandom.uuid,
          state: 'PENDING'
        )
      end
    end
  end
end

