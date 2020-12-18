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
  class Builder
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
        "event_scripts" => {
          "type" => "object",
          "additionalProperties" => false,
          "required" => ["excess", "insufficient", "status"],
          "properties" => {
            "excess" => { "type" => "string" },
            "insufficient" => { "type" => "string" },
            "status" => { "type" => "string" }
          }
        },
        "types" => {
          "type" => "object",
          "properties" => {
            "unknown" => { "type" => "null" }
          },
          "patternProperties" => {
            ".*" => {
              "type" => "object",
              "additionalProperties" => false,
              "properties" => {
                "minimum" => { "type" => "integer" },
                "maximum" => { "type" => "integer" }
              }
            }
          }
        }
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

    SPEC_KEYS   = ['max_time_limit', 'default_time_limit', 'node_matchers', 'types']
    OTHER_KEYS  = ['default', 'name']
    VALIDATOR = JSONSchemer.schema(ROOT_SCHEMA)

    attr_reader :specs

    def initialize(specs)
      @specs = specs
    end

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
        script_attrs = spec.fetch('event_scripts', {}).transform_keys { |k| "#{k}_script".to_sym }
        Partition.new(**other_attrs, **spec_attrs, **script_attrs, static_node_names: spec['nodes'])
      end
    end

    def to_node_names
      specs.reduce([]) { |memo, spec| [*memo, *spec.fetch('nodes', [])] }.uniq
    end

    def errors
      @errors ||= VALIDATOR.validate({ "partitions" => specs }).to_a
    end
  end
end
