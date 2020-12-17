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

  # NOTE: This method considers the partition shrinkable if any nodes are IDLE
  # Some additional handling maybe required for node's which are DOWN but not terminated
  # OR if IDLE static nodes count
  def shrinkable?
    dynamic? && nodes.any? { |n| n.state == 'IDLE' }
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
