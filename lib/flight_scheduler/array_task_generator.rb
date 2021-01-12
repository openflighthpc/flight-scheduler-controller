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
#
# `next_task`: will return the next pending ARRAY_TASK for the job if any.
#   The same ARRAY_TASK will be returned indefinitely until some external
#   actor such as scheduler call `advance_next_task`.
#
# `advance_next_task`: advances the ARRAY_TASK returned by `next_task`.
class FlightScheduler::ArrayTaskGenerator
  def initialize(job)
    @job = job
    @array_range = job.array_range.expanded
    @index_into_array_range = 0
  end

  def advance_next_task
    @index_into_array_range += 1
  end

  def next_index
    @array_range[@index_into_array_range]
  end

  def next_index=(array_index)
    @index_into_array_range = @array_range.find_index(array_index)
  end

  def next_task
    if finished?
      nil
    elsif !@next_task.nil? && @next_task.array_index == next_index
      @next_task
    else
      @next_task = build_next_task
    end
  end

  def finished?
    @index_into_array_range >= @array_range.length
  end

  # Return a condensed string serialization of the remaining array range.
  #
  # Invariant: expanding the returned serialization with `RangeExpander.split`
  # equals the array indexes of the remaining tasks.
  #
  # XXX Implement the invariant.
  def remaining_array_range
    next_idx = next_index
    last_idx = @array_range.last
    if next_idx.nil?
      "[]"
    elsif next_idx == last_idx
      "[#{last_idx}]"
    else
      "[#{next_idx}-#{last_idx}]"
    end
  end

  private

  def build_next_task
    return nil if finished?
    Job.new(
      array_index: @array_range[@index_into_array_range],
      array_job: @job,
      cpus_per_node: @job.cpus_per_node,
      gpus_per_node: @job.gpus_per_node,
      id: SecureRandom.uuid,
      job_type: 'ARRAY_TASK',
      memory_per_node: @job.memory_per_node,
      min_nodes: @job.min_nodes,
      partition: @job.partition,
      state: 'PENDING',
      time_limit_spec: @job.time_limit_spec,
      username: @job.username,
    )
  end
end
