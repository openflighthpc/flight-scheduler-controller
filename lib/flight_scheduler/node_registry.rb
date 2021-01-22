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

class FlightScheduler::NodeRegistry
  class NodeConflictError < RuntimeError; end

  def initialize
    @nodes = {}
    @lock = Concurrent::ReentrantReadWriteLock.new
    @partitions_cache = {}
  end

  def each
    block_given? ? @nodes.each { |_, n| yield(n) } : @nodes.values.each
  end

  def [](node_name)
    @lock.with_read_lock { @nodes[node_name] }
  end

  # Ensures the node exists and is correctly cached
  def register_node(node_name)
    # This update is not atomic and depends on the new state of the node
    # This creates the possibility of a race condition and thus needs a write lock
    @lock.with_write_lock do
      node = @nodes[node_name]
      if node.nil?
        Async.logger.info "Creating node registry entry: '#{node_name}'"
        node = @nodes[node_name] = Node.new(name: node_name)
      end
      update_partition_cache(node)

      # Return the node
      node
    end
  end

  def update_partition_cache(node)
    @lock.with_write_lock do
      @partitions_cache.each do |_, nodes:, partition:|
        match     = partition.node_match?(node)
        existing  = nodes.include?(node)

        # Update the nodes array
        if match && existing
          Async.logger.debug "Retaining node '#{node.name}' within partition '#{partition.name}'"
        elsif match
          Async.logger.info "Adding node '#{node.name}' to partition '#{partition.name}'"
          nodes.push node
        elsif existing
          Async.logger.warn "Removing node '#{node.name}' from partition '#{partition.name}'"
          nodes.delete node
        else
          Async.logger.debug "Ignoring node '#{node.name}' for partition '#{partition.name}'"
        end
      end

      # Return the node
    end
  end

  def for_partition(partition)
    if @partitions_cache.key?(partition.name)
      @lock.with_read_lock { @partitions_cache[partition.name][:nodes] }
    else
      @lock.with_write_lock do
        # Handle a race conditions where multiple threads try and initialise a partition at the same time
        unless @partitions_cache.key?(partition.name)
          nodes = @nodes.select { |_, n| partition.node_match?(n) }.values
          Async.logger.info "Initialising partition '#{partition.name}' with nodes: #{nodes.map(&:name).join(',')}"
          @partitions_cache[partition.name] = { nodes: nodes, partition: partition }
        end
        @partitions_cache[partition.name][:nodes]
      end
    end
  end
end
