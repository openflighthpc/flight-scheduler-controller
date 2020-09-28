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

module FlightScheduler
  class RangeExpander
    DOC_REGEX   = '^(\d+|\d+-\d+(:\d+)?)(,\d+|\d+-\d+(:\d+)?)*$'
    INT_REGEX   = /\A\d+\Z/
    DASH_REGEX  = /\A(\d+)-(\d+)(?::([1-9]\d*))?\Z/

    include Enumerable
    extend Forwardable
    def_delegator :expand, :each

    attr_reader :parts

    def self.split(range)
      new(range.split(','))
    end

    def initialize(parts)
      @parts = parts.map { |p| p.strip }
    end

    def valid?
      parts.reduce(false) do |_, part|
        bool = if INT_REGEX.match?(part)
                 true
              elsif match = DASH_REGEX.match(part)
                match[1].to_i <= match[2].to_i
              else
                false
              end
        break false unless bool
        true
      end
    end

    def expand
      parts.map do |part|
        if match = part.match(DASH_REGEX)
          # Extracts the range components
          alpha = match[1].to_i
          omega = match[2].to_i
          multi = (match[3] || 1).to_i

          # Generates the raw range of numbers
          raw = (alpha..omega)

          # Applies the multiplier filter
          case multi
          when 0
            # This should never be reached but is included to prevent maths errors
            []
          when 1
            raw.to_a
          else
            # Avoid using the modulo function as it is slow for large numbers
            count = 0
            [].tap do |array|
              raw.each do |int|
                # Add multiplies when the count is 0
                array << int if count == 0

                # Increment the counter
                count += 1

                # Reset the counter at the multiplier (this simulates the modulo function)
                count = 0 if count == multi
              end
            end
          end
        else
          [part.to_i]
        end
      end.flatten
    end
  end
end
