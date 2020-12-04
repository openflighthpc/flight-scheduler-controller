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

require 'json-schema'

module FlightScheduler
  class NodeMatcher

    KEYS = ['name', 'cpus', 'gpus']

    # Used to validate the matcher provided by the user
    SCHEMA = {
      "type" => "object",
      "required" => ["key"],
      "additionalProperties": false,
      "properties" => {
        "key" => { "type" => "string", "enum" => KEYS },
        "regex" => { "type" => "string" },
        'lt' => { "type" => 'integer' },
        'lte' => { "type" => 'integer' },
        'gt' => { "type" => 'integer' },
        'gte' => { "type" => 'integer' }
      }
    }

    attr_reader :key, :specs, :errors

    def initialize(key, **specs)
      @key = key.to_s
      @specs = specs.transform_keys(&:to_s)
    end

    def valid?
      @errors = JSON::Validator.fully_validate(SCHEMA, { "key" => key }.merge(specs))
      @errors.empty?
    end

    def regex(str)
      Regexp.new(specs['regex']).match?(str.to_s)
    end
  end
end
