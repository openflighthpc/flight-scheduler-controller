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

require_relative './partition/builder'
require_relative './partition/script_runner'

class Partition
  Type = Struct.new(:partition, :name, :minimum, :maximum) do
    def nodes
      partition.nodes.select { |n| n.type == name }
    end
  end

  include ActiveModel::Validations

  attr_reader :name, :nodes, :max_time_limit, :default_time_limit

  validate if: :excess_script_path do
    next if File.executable?(excess_script_path)
    @errors.add(:excess_script, 'must exist and be executable')
  end
  validate if: :insufficient_script_path do
    next if File.executable?(insufficient_script_path)
    @errors.add(:insufficient_script, 'must exist and be executable')
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

  validate if: -> { @max_time_limit_spec } do
    next if max_time_limit
    @errors.add(:max_time_limit_spec, 'is not a valid syntax')
  end

  validate if: -> { @default_time_limit_spec } do
    next if default_time_limit
    @errors.add(:default_time_limit_spec, 'is not a valid syntax')
  end

  validate if: :max_time_limit do
    next if max_time_limit >= default_time_limit
    @errors.add(:max_time_limit_spec, 'must be greater than or equal the default')
  end

  validate do
    types.each do |type|
      next if type.minimum.nil? || type.maximum.nil?
      next if type.minimum <= type.maximum
      @errors.add(:type_minimum, 'must be less than or equal to the maximum')
    end
  end

  def initialize(
    name:,
    default: false,
    static_node_names: nil,
    default_time_limit_spec: nil,
    max_time_limit_spec: nil,
    node_matchers_spec: nil,
    types_spec: nil,
    excess_script: nil,
    insufficient_script: nil,
    status_script: nil
  )
    @name = name
    @default = default
    @max_time_limit_spec = max_time_limit_spec
    @default_time_limit_spec = default_time_limit_spec
    @node_matchers_spec = node_matchers_spec
    @types_spec = types_spec || {}
    @static_node_names = static_node_names || []
    @excess_script    = excess_script
    @insufficient_script  = insufficient_script
    @status_script  = status_script
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

  def excess_script_path
    return nil unless @excess_script
    @excess_script_path ||= File.expand_path(@excess_script, FlightScheduler.app.config.libexec_dir)
  end

  def insufficient_script_path
    return nil unless @insufficient_script
    @insufficient_script_path ||= File.expand_path(@insufficient_script, FlightScheduler.app.config.libexec_dir)
  end

  def status_script_path
    return nil unless @status_script
    @status_script_path ||= File.expand_path(@status_script, FlightScheduler.app.config.libexec_dir)
  end

  def script_runner
    @script_runner ||= ScriptRunner.new(self)
  end

  def max_time_limit
    @max_time_limit ||= FlightScheduler::TimeResolver.new(@max_time_limit_spec).resolve
  end

  def default_time_limit
    @default_time_limit ||= if @default_time_limit_spec
      FlightScheduler::TimeResolver.new(@default_time_limit_spec).resolve
    else
      max_time_limit
    end
  end

  def default?
    !!@default
  end

  def types
    @types ||= @types_spec.map do |name, spec|
      Type.new(self, name, spec['minimum'], spec['maximum'])
    end
  end

  def ==(other)
    self.class == other.class && name == other.name
  end
  alias eql? ==

  def hash
    ([self.class, name]).hash
  end

  private

  def matchers
    @matchers ||= (@node_matchers_spec || {}).map do |key, spec|
      FlightScheduler::NodeMatcher.new(key, **spec.symbolize_keys)
    end
  end
end
