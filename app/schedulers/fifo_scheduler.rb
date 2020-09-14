#==============================================================================
# Copyright (C) 2020-present Alces Flight Ltd.
#
# This file is part of FlurmAPI.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License 2.0 which is available at
# <https://www.eclipse.org/legal/epl-2.0>, or alternative license
# terms made available by Alces Flight Ltd - please direct inquiries
# about licensing to licensing@alces-flight.com.
#
# FlurmAPI is distributed in the hope that it will be useful, but
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR
# IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR CONDITIONS
# OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A
# PARTICULAR PURPOSE. See the Eclipse Public License 2.0 for more
# details.
#
# You should have received a copy of the Eclipse Public License 2.0
# along with FlurmAPI. If not, see:
#
#  https://opensource.org/licenses/EPL-2.0
#
# For more information on FlurmAPI, please visit:
# https://github.com/openflighthpc/flurm-api
#==============================================================================

class FifoScheduler
  attr_reader :partition
  attr_reader :queue

  def initialize(partition)
    @partition = partition
    @queue = []
  end

  # Add a single job to the queue.
  def add_job(job)
    @queue << job
  end

  # Adjust priorities for the jobs.
  def reprioritise
    # This is a simple FIFO.  The jobs are never reprioritised.  More complex
    # schedulers will likely do something more complex here.
  end

  # Allocate the next job that can be scheduled if any.
  #
  # If a job can be scheduled, return the new `Allocation`, otherwise return
  # `nil`.
  #
  # In order for a job to be scheduled, the partition must contain sufficient
  # available resources to meet the jobs requirements.
  def allocate_next_job
    # This is a simple FIFO. Only consider the next job in the FIFO.
    return nil if @queue.empty?
    return nil if @partition.available_nodes.length < @queue.first.min_nodes

    job = @queue.first
    selected_nodes = @partition.available_nodes[0...job.min_nodes]
    Allocation.new(
      job: job,
      nodes: selected_nodes,
      partition: @partition
    )
  end
end
