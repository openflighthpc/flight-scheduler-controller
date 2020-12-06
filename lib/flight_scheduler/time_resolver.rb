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
  class TimeResolver
    MIN_REGEX               = /\A\d+\Z/
    MIN_SEC_REGEX           = /\A\d+:\d+\Z/
    DAY_HOUR_REGEX          = /\A\d+-\d+\Z/
    HOUR_MIN_SEC_REGEX      = /\A\d+:\d+:\d+\Z/
    DAY_HOUR_MIN_REGEX      = /\A\d+-\d+:\d+\Z/
    DAY_HOUR_MIN_SEC_REGEX  = /\A\d+-\d+:\d+:\d+\Z/

    def initialize(string)
      @string = string
    end

    def resolve
      case @string
      when MIN_REGEX, Integer
        @string.to_i * 60
      when MIN_SEC_REGEX
        m, s = @string.split(':', 2).map(&:to_i)
        m * 60 + s
      when DAY_HOUR_REGEX
        d, h = @string.split('-', 2).map(&:to_i)
        (d * 24 + h) * 3600
      when HOUR_MIN_SEC_REGEX
        h, m, s = @string.split(':', 3).map(&:to_i)
        (h * 60 + m) * 60 + s
      when DAY_HOUR_MIN_REGEX
        d, h, m = @string.split(/[-:]/).map(&:to_i)
        ((d * 24 + h) * 60 + m) * 60
      when DAY_HOUR_MIN_SEC_REGEX
        d, h, m, s = @string.split(/[-:]/).map(&:to_i)
        ((d * 24 + h) * 60 + m) * 60 + s
      end
    end
  end
end
