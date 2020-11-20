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


# Generates ARRAY_TASKs for an ARRAY_JOB and provides methods to query current
# state of generation.
class FlightScheduler::ArrayTaskGenerator
  attr_reader :next_task

  def initialize(job)
    @job = job
    @next_task = get_next_task
  end

  def advance_array_index
    get_next_task
  end

  def finished?
    @next_task.nil?
  end

  # Return a condensed string serialization of the remaining array range.
  #
  # Invariant: expanding the returned serialization with `RangeExpander.split`
  # equals the array indexes of the remaining tasks.
  #
  # XXX Implement the invariant.
  def remaining_array_range
    next_idx = @next_task&.array_index
    last_idx = @job.array_range.expanded.last
    if next_idx.nil?
      "[]"
    elsif next_idx == last_idx
      "[#{last_idx}]"
    else
      "[#{next_idx}-#{last_idx}]"
    end
  end

  private

  def get_next_task
    @next_task = task_enum.next
  rescue StopIteration
    @next_task = nil
  end

  def task_enum
    @task_enum ||= Enumerator.new do |yielder|
      @job.array_range.each do |idx|
        yielder << Job.new(
          array_index: idx,
          array_job: @job,
          id: SecureRandom.uuid,
          job_type: 'ARRAY_TASK',
          min_nodes: @job.min_nodes,
          partition: @job.partition,
          state: 'PENDING',
          username: @job.username,
        )
      end
    end
  end
end
