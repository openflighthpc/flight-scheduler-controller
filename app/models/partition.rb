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
      "node_matchers" => FlightScheduler::NodeMatcher::SCHEMA
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

  SPEC_KEYS = ['max_time_limit', 'default_time_limit', 'node_matchers', 'nodes']
  VALIDATOR = JSONSchemer.schema(ROOT_SCHEMA)

  Builder = Struct.new(:specs, :node_registry) do
    # NOTE: This is only a syntactic validation of the config. It does not guarantee
    # the resultant partitions are valid semantically
    def valid?
      errors.empty?
    end

    def to_partitions
      specs.map do |spec|
        spec_attrs = spec.slice(*SPEC_KEYS).transform_keys { |k| :"#{k}_spec" }
        other_attrs = spec.dup.tap { |s| SPEC_KEYS.each { |k| s.delete(k) } }
                          .transform_keys(&:to_sym)
        Partition.new(**other_attrs, **spec_attrs,
                      node_registry: node_registry || FlightScheduler.app.nodes)
      end
    end

    def generate_nodes
      specs.each do |spec|
        spec.fetch('nodes', []).each { |n| node_registry.fetch_or_add(n) }
      end
    end

    def errors
      @errors ||= VALIDATOR.validate({ "partitions" => specs }).to_a
    end
  end

  attr_reader :name, :nodes, :max_time_limit, :default_time_limit

  def initialize(
    name:,
    node_registry:,
    default: false,
    nodes_spec: nil,
    default_time_limit_spec: nil,
    max_time_limit_spec: nil,
    node_matchers_spec: nil
  )
    @name = name
    @default = default
    @matchers = matchers
    @max_time_limit_spec = max_time_limit_spec
    @default_time_limit_spec = default_time_limit_spec
    @node_matchers_spec = node_matchers_spec
    @nodes_spec = nodes_spec || []
    @node_registry = node_registry
  end

  # Intentionally not cached to help ensure it remains up to date
  # TODO: Eventually store the partition-node mapping within the NodeRegistry
  #       This should make persistence of dynamic nodes easier
  # NOTE: The `node_matcher_spec` may have changed after a reboot. This needs
  #       to be accounted for during the persistence reload
  def nodes
    @nodes_spec.map { |n| @node_registry[n] }
  end

  def node_match?(node)
    return false if matchers.empty?
    matchers.all? { |m| m.match?(node) }
  end

  # TODO: Port validation code onto Partition with ActiveModel::Validation
  # def parse_times(max, default, name:)
  #   if max && default
  #     t_max = TimeResolver.new(max).resolve
  #     t_default = TimeResolver.new(default).resolve
  #     if t_max.nil?
  #       raise ConfigError, "Partition '#{name}': could not parse max time limit: #{max}"
  #     elsif t_default.nil?
  #       raise ConfigError, "Partition '#{name}': could not parse default time limit: #{default}"
  #     elsif t_default > t_max
  #       raise ConfigError, "Partiitio '#{name}': the default time limit must be less than the maximum"
  #     else
  #       [t_max, t_default]
  #     end
  #   elsif max
  #     t_max = TimeResolver.new(max).resolve
  #     if t_max.nil?
  #       raise ConfigError, "Partition '#{name}': could not parse max time limit: #{max}"
  #     else
  #       [t_max, t_max]
  #     end
  #   elsif default
  #     t_default = TimeResolver.new(default).resolve
  #     if t_default.nil?
  #       raise ConfigError, "Partition '#{name}': could not parse default time limit: #{default}"
  #     else
  #       [nil, t_default]
  #     end
  #   else
  #     [nil, nil]
  #   end
  # end

  # TODO: Validate me!
  def max_time_limit
    @max_time_limit ||= FlightScheduler::TimeResolver.new(@max_time_limit_spec).resolve
  end

  # TODO: Validate me!
  def default_time_limit
    @default_time_limit ||= FlightScheduler::TimeResolver.new(@default_time_limit_spec).resolve
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

  private

  def matchers
    @matchers ||= (@node_matcher_spec || {}).map do |key, spec|
      FlightScheduler::NodeMatcher.new(key, spec)
    end
  end
end
