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

# Registry of all active allocations.
#
# This class provides a single location that can be queried for the current
# resource allocations.
#
# Using this class as the single source of truth for allocations has some
# important properties:
#
# 1. Creating an `Allocation` object does not allocate any resources.
# 2. Resources are only allocated once they are +add+ed to the
#    +AllocationRegistry+.
# 3. Adding +Allocation+s to the +AllocationRegistry+ is atomic; and therefore
#    thread safe.
#
class FlightScheduler::AllocationRegistry
  class AllocationConflict < RuntimeError ; end

  # Maps keys on the Node object to the per_node methods on Job
  KEY_MAP = {
    cpus:   :cpus_per_node,
    gpus:   :gpus_per_node,
    memory: :memory_per_node
  }

  def initialize(lock: nil)
    @node_allocations = Hash.new { |h, k| h[k] = [] }
    @job_allocations  = {}
    @lock = lock || Concurrent::ReadWriteLock.new
  end

  def add(allocation)
    unless allocation.valid?
      raise AllocationConflict, <<~ERROR.chomp
        The following allocation is invalid:
        #{allocation.errors.full_messages.join("\n")}
      ERROR
    end

    @lock.with_write_lock do
      allocation.nodes.each do |node|
        # Gets the existing allocations
        allocations = @node_allocations[node.name]

        # Ensures exclusive access is preserved
        if allocations.any? { |a| a.job.exclusive }
          raise AllocationConflict, "An existing job has exclusive access to '#{node.name}'"
        end
        if allocation.job.exclusive && !allocations.empty?
          raise AllocationConflict, "Can not obtain exclusive access to '#{node.name}'"
        end

        # Ensures there is sufficient resources
        allocated = allocated_resources(allocation, *allocations)
        KEY_MAP.each do |node_key, job_key|
          if node.send(node_key).to_i < allocated[job_key]
            raise AllocationConflict, "Node '#{node.name}' has insufficient #{node_key}"
          end
        end
      end

      # Ensures a 1-1 mapping of jobs to allocations
      raise AllocationConflict, allocation.job if @job_allocations[allocation.job.id]

      # Duplicate the allocation so it is safe to modify
      allocation = allocation.dup

      allocation.nodes.each do |node|
        @node_allocations[node.name] << allocation
      end
      @job_allocations[allocation.job.id] = allocation
    end
  end

  # NOTE: This method currently removes all instances of the node from the allocation
  # Revisit when duplicate nodes within an allocation are allowed
  def deallocate_node_from_job(job_id, node_name)
    @lock.with_write_lock do
      # Determine the allocation
      allocation = @job_allocations[job_id]

      # Ignore missing job allocations, in may have already been removed
      return unless allocation

      # Remove the node from the allocation
      allocation.remove_node(node_name)
      @node_allocations[node_name].delete(allocation)

      # Remove empty job allocations if required
      @job_allocations.delete(job_id) if allocation.nodes.empty?

      # Return the allocation
      allocation
    end
  end

  def for_job(job_id)
    @lock.with_read_lock { @job_allocations[job_id] }
  end

  def for_node(node_name)
    @lock.with_read_lock { @node_allocations[node_name] || [] }
  end

  def debug_node_allocations(node_name)
    allocations = for_node(node_name)
    if allocations.any? { |a| a.job.exclusive }
      "exclusive"
    else
      allocated = allocated_resources(*allocations)
      allocated.map do |key, value|
        pretty_key_name = "allocated_#{key.to_s.sub(/_.*/, '')}"
        "#{pretty_key_name}=#{value}"
      end.join(" ")
    end
  end

  def size
    @lock.with_read_lock do
      @job_allocations.size
    end
  end

  def each(&block)
    values = @lock.with_read_lock do
      @job_allocations.values
    end
    values.each(&block)
  end

  def max_parallel_per_node(job, node, allocations: nil, excluding_jobs: nil, reservations: nil)
    allocs = considering_allocations(node, allocations, excluding_jobs, reservations)
    allocated = allocated_resources(*allocs)

    # Handle jobs with exclusive accesse
    return 0 if job.exclusive && !allocs.empty?
    return 0 if allocs.map(&:job).any?(&:exclusive)

    # Determines how many times the job can be ran on the node given the
    # allocations we're considering.
    max_jobs = KEY_MAP.reduce(nil) do |max, (node_key, job_key)|
      dividor = job.send(job_key).to_i
      next max if dividor < 1
      current = ( node.send(node_key).to_i - allocated[job_key] ) / dividor

      break 0 if current < 1
      (max.nil? || max > current) ? current : max
    end
    max_jobs ||= 0

    Async.logger.debug("Node #{node.name} can run #{job.display_id} #{max_jobs} times") {
      {
        current_allocations: allocated,
        node_attributes: KEY_MAP.reduce({}) do |accum, (node_key, _)|
        accum[node_key] = node.send(node_key);
        accum
      end,
      job_requirements: KEY_MAP.reduce({}) do |accum, (_, job_key)|
        accum[job_key] = job.send(job_key);
        accum
      end,
      }
    }

    max_jobs
  end

  def serializable_data
    @lock.with_read_lock do
      @node_allocations.values.flatten.map(&:serializable_hash)
    end
  end

  def load(data)
    return if data.nil?
    @lock.with_write_lock do
      allocations = data.map do |h|
        h['node_names'].each do |name|
          next if FlightScheduler.app.nodes[name]
          FlightScheduler.app.nodes.register_node(name)
        end

        [h['job_id'], Allocation.from_serialized_hash(h)]
      end
      good, bad = allocations.partition { |_, a| a.valid? }

      # Add the allocations with jobs
      good.each do |_, allocation|
        allocation.nodes.each do |node|
          @node_allocations[node.name] << allocation
        end
        @job_allocations[allocation.job.id] = allocation
      end

      # Log the allocations without jobs
      unless bad.empty?
        Async.logger.error("Failed to load some of the allocations as they are invalid:") do
          bad.map do |id, alloc|
            "Job #{id}: #{alloc.errors.full_messages.join(", ")}"
          end.join("\n")
        end
      end
    end
  rescue
    Async.logger.warn("Error loading allocation registry") { $! }
    raise
  end

  private

  # TODO: Confirm how duplicate job to node assignments are handled here
  def allocated_resources(*allocations)
    KEY_MAP.values.each_with_object({}) do |key, memo|
      memo[key] = allocations.map { |a| a.job.send(key).to_i }.reduce(&:+).to_i
    end
  end

  # These methods exist to facilitate testing.

  def empty?
    @lock.with_read_lock do
      @job_allocations.empty?
    end
  end

  def clear
    @lock.with_write_lock do
      @job_allocations.clear
      @node_allocations.clear
    end
  end

  def considering_allocations(node, given_allocations, excluding_jobs, reservations)
    allocations = nil
    if given_allocations
      allocations = given_allocations.filter { |r| r.nodes.include? node }
    else
      allocations = @lock.with_read_lock { @node_allocations[node.name] }
    end
    Async.logger.debug("Allocations for #{node.name}") {
      if allocations.empty?
        "NONE"
      else
        allocations.map { |a| "job=#{a.job.display_id} nodes=#{a.nodes.map(&:name)}" }.join("\n")
      end
    }
    if excluding_jobs
      excluded_allocations = Array(excluding_jobs).map { |job| for_job(job.id) }
      allocations -= excluded_allocations.compact
    end
    if reservations
      relevant_reservations = reservations.filter { |r| r.nodes.include? node }
      Async.logger.debug("Relevant reservations for #{node.name}") {
        if relevant_reservations.empty?
          "NONE"
        else
          relevant_reservations.map { |a| "job=#{a.job.display_id} nodes=#{a.nodes.map(&:name)}" }.join("\n")
        end
      }
      allocations += Array(relevant_reservations).compact
    end

    allocations
  end
end
