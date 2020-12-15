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

class Partition
  PARTITION_SCHEMA = {
    "type" => "object",
    "additionalProperties" => false,
    "required" => ["name"],
    "properties" => {
      "name" => { "type" => 'string' },
      "default" => { "type" => 'boolean' },
      "nodes" => { "type" => "array", "items" => { "type" => "string" } },
      "max_time_limit" => { "type" => ['string', 'integer'] },
      "default_time_limit" => { "type" => ['string', 'integer'] },
      "node_matchers" => { "type" => "array", "items" => FlightScheduler::NodeMatcher::SCHEMA }
    }
  }
  ROOT_SCHEMA = {
    "type" => "object",
    "additionalProperties" => false,
    "properties" => {
      "partitions" => {
        "type" => "array",
        "items" => PARTITION_SCHEMA
      }
    }
  }

  VALIDATOR = JSONSchemer.schema(ROOT_SCHEMA)

  Builder = Struct.new(:specs) do
    def valid?
      errors.empty?
    end

    def errors
      @errors ||= VALIDATOR.validate({ "partitions" => specs }).to_a
    end
  end

  attr_reader :name, :nodes, :max_time_limit, :default_time_limit

  def initialize(
    name:,
    nodes:,
    default: false,
    default_time_limit: nil,
    matchers: {},
    max_time_limit: nil
  )
    @name = name
    @nodes = nodes
    @default = default
    @default_time_limit = default_time_limit
    @matchers = matchers
    @max_time_limit = max_time_limit
  end

  def node_match?(node)
    return false if @matchers.empty?
    @matchers.all? { |m| m.match?(node) }
  end

  def default?
    !!@default
  end

  def ==(other)
    self.class == other.class &&
      name == other.name &&
      nodes == other.nodes
  end
  alias eql? ==

  def hash
    ( [self.class, name] + nodes.map(&:hash) ).hash
  end
end
