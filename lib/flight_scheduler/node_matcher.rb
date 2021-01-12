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

require 'json_schemer'

module FlightScheduler
  class NodeMatcher

    KEYS = ['name', 'cpus', 'gpus', 'memory']

    # Used to validate the matcher provided by the user
    SCHEMA = {
      "type" => "object",
      "additionalProperties" => false,
      "patternProperties" => {
        "^#{KEYS.join('|')}$" => {
          "type" => "object",
          "additionalProperties" => false,
          "properties" => {
            "regex" => { "type" => "string" },
            'lt' => { "type" => 'integer' },
            'lte' => { "type" => 'integer' },
            'gt' => { "type" => 'integer' },
            'gte' => { "type" => 'integer' },
            'list' => {
              "type" => 'array',
              'items' => { 'type' => ['integer', 'string'] }
            }
          }
        }
      }
    }

    attr_reader :key, :specs

    def initialize(key, **specs)
      @key = key.to_s
      @specs = specs.transform_keys(&:to_s)
    end

    # DEPRECATED: These methods are used extensively in the specs but should not
    # be used in the code base. They have been replaced by the top level partition
    # config validation.
    #
    # Consider refactoring
    attr_reader :errors
    def valid?
      @errors = JSONSchemer.schema(SCHEMA).validate({ key => specs }).to_a
      @errors.empty?
    end

    def match?(node)
      value = node.send(key)
      specs.keys.all? { |k| self.send(k, value) }
    end

    # NOTE: The following methods are public to facilitate testing
    # The methods are only intended to be called if they have a corresponding spec entry
    # Calling a method without a spec entry is undefined
    def regex(str)
      Regexp.new(specs['regex']).match?(str.to_s)
    end

    def lt(int)
      return false unless int.is_a? Integer
      int < specs['lt']
    end

    def lte(int)
      return false unless int.is_a? Integer
      int <= specs['lte']
    end

    def gt(int)
      return false unless int.is_a? Integer
      int > specs['gt']
    end

    def gte(int)
      return false unless int.is_a? Integer
      int >= specs['gte']
    end

    def list(value)
      @list_hash ||= Hash.new(false).merge!(specs['list'].map { |k| [k, true] }.to_h)
      @list_hash[value]
    end
  end
end
