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
      "node_matchers" => FlightScheduler::NodeMatcher::SCHEMA,
      "dynamic" => {
        "type" => "object",
        "additionalProperties" => false,
        "required" => ["grow_script", "shrink_script", "status_script"],
        "properties" => {
          "grow_script" => { "type" => "string" },
          "shrink_script" => { "type" => "string" },
          "status_script" => { "type" => "string" }
        }
      }
    },
    "if" => { "properties" => { "dynamic" => { "type" => "null" } } },
    "else" => { "required" => ["node_matchers"] }
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

  SPEC_KEYS   = ['max_time_limit', 'default_time_limit', 'node_matchers']
  OTHER_KEYS  = ['default', 'name']
  VALIDATOR = JSONSchemer.schema(ROOT_SCHEMA)

  Builder = Struct.new(:specs) do
    # NOTE: This is only a syntactic validation of the config. It does not guarantee
    # the resultant partitions are valid semantically
    #
    # The partitions themselves can not be validated until after config load. Doing
    # it during config load creates a circular logic as the libexec_dir hasn't been set
    def valid?
      errors.empty?
    end

    def to_partitions
      specs.map do |spec|
        spec_attrs = spec.slice(*SPEC_KEYS).transform_keys { |k| :"#{k}_spec" }
        other_attrs = spec.slice(*OTHER_KEYS).transform_keys(&:to_sym)
        dynamic_attrs = spec.fetch('dynamic', {}).transform_keys(&:to_sym)
        Partition.new(**other_attrs, **spec_attrs, **dynamic_attrs, static_node_names: spec['nodes'])
      end
    end

    def to_node_names
      specs.reduce([]) { |memo, spec| [*memo, *spec.fetch('nodes', [])] }.uniq
    end

    def errors
      @errors ||= VALIDATOR.validate({ "partitions" => specs }).to_a
    end
  end

  include ActiveModel::Validations

  attr_reader :name, :nodes, :max_time_limit, :default_time_limit

  validate if: :grow_script_path do
    next if File.executable?(grow_script_path)
    @errors.add(:grow_script, 'must exist and be executable')
  end
  validate if: :shrink_script_path do
    next if File.executable?(shrink_script_path)
    @errors.add(:shrink_script, 'must exist and be executable')
  end
  validate if: :status_script_path do
    next if File.executable?(status_script_path)
    @errors.add(:status_script, 'must exist and be executable')
  end
  validate do
    case FlightScheduler.app.partitions.select { |p| p.name == name }.length
    when 1
      next
    when 0
      @errors.add(:partition, 'must be registered')
    else
      @errors.add(:name, 'must be unique')
    end
  end

  def initialize(
    name:,
    default: false,
    static_node_names: nil,
    default_time_limit_spec: nil,
    max_time_limit_spec: nil,
    node_matchers_spec: nil,
    grow_script: nil,
    shrink_script: nil,
    status_script: nil
  )
    @name = name
    @default = default
    @matchers = matchers
    @max_time_limit_spec = max_time_limit_spec
    @default_time_limit_spec = default_time_limit_spec
    @node_matchers_spec = node_matchers_spec
    @static_node_names = static_node_names || []
    @grow_script    = grow_script
    @shrink_script  = shrink_script
    @status_script  = status_script
  end

  def dynamic?
    grow_script_path ? true : false
  end

  # Intentionally not cached to help ensure it remains up to date
  def nodes
    FlightScheduler.app.nodes.for_partition(self)
  end

  def node_match?(node)
    return true if @static_node_names.include? node.name
    return false if matchers.empty?
    matchers.all? { |m| m.match?(node) }
  end

  def grow_script_path
    return nil unless @grow_script
    @grow_script_path ||= File.expand_path(@grow_script, FlightScheduler.app.config.libexec_dir)
  end

  def shrink_script_path
    return nil unless @shrink_script
    @shrink_script_path ||= File.expand_path(@shrink_script, FlightScheduler.app.config.libexec_dir)
  end

  def status_script_path
    return nil unless @status_script
    @status_script_path ||= File.expand_path(@status_script, FlightScheduler.app.config.libexec_dir)
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
    self.class == other.class && name == other.name
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
