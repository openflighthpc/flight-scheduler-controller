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

require 'etc'

module FlightScheduler
  PathGenerator = Struct.new(:node, :job, :task) do
    self::NUMERIC_KEYS = {
      'a' => 'The current index when running an array task, otherwise 0',
      # TODO: This can't be implemented affectively as the nodes are allocated
      # to individual array tasks and not the job. This makes determining the
      # node indexing either inconsistent or impossible to determine
      #
      # The allocation behaviour is likely to change TBD. Consider revisiting
      # 'n' => 'The relative ID of the node within the job',
    }
    self::ALPHA_KEYS = {
      'A' => "The ID of the associate job when running an array task, otherwise empty string",
      'j' => 'The ID of the running job/task',
      'N' => 'The current node name',
      'u' => 'The user name',
      'x' => 'The job name',
      '%' => "Escape a literal percent character '%', instead of a special directive"
    }

    self::DESC = [
      'The paths may contain various special characters which will be replaced:',
      '',
      *self::NUMERIC_KEYS.map { |k, d| " * `%#{k}`: #{d}" },
      *self::ALPHA_KEYS.map { |k, d| " * `%#{k}`: #{d}" },
      ' * `%<char>`: All other characters form an invalid replacement'
    ].join("\n")

    def pct_N
      node.name
    end

    # TODO: Eventually make this the user that submitted the job
    def pct_u
      Etc.getlogin
    end

    def pct_x
      job.script_name
    end
  end
end
