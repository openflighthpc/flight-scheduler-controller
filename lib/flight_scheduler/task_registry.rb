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
    @pending_task = task_enum.next
    @running_tasks = []
    @past_tasks = []
    @mutex = Mutex.new
  end

  def pending_task(update = true)
    refresh if update
    @pending_task
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

  private

  def refresh
    @mutex.synchronize do
      @running_tasks.select! do |task|
        (task.running? || task.allocated?).tap do |bool|
          @past_tasks << task unless bool
        end
      end
      if @pending_task && (@pending_task.allocated? || !@pending_task.pending?)
        if @pending_task.running? || @pending_task.allocated?
          @running_tasks << @pending_task
        else
          @past_tasks << @pending_task
        end
        begin
          @pending_task = task_enum.next
        rescue StopIteration
          @pending_task = nil
        end
      end
    end
  end

  # TODO: Eventually make the tasks here not in the Job object
  def task_enum
    @task_enum ||= Enumerator.new do |yielder|
      job.array_tasks.each { |t| yielder << t }
    end
  end
end

