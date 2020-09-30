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
  # NOTE: PathGenerator has been deliberately implemented without knowledge of
  # the data model. This is to prevent coupling to the current implementation
  # of Task-Job relationship
  class PathGenerator
    NUMERIC_KEYS = {
      'a' => 'The current index when running an array task, otherwise 0',
      # TODO: This can't be implemented affectively as the nodes are allocated
      # to individual array tasks and not the job. This makes determining the
      # node indexing either inconsistent or impossible to determine
      #
      # The allocation behaviour is likely to change TBD. Consider revisiting
      # 'n' => 'The relative ID of the node within the job',
    }
    ALPHA_KEYS = {
      'A' => "The ID of the associate job when running an array task, otherwise empty string",
      'j' => 'The ID of non array task jobs, otherwise empty string',
      'N' => 'The current node name',
      'u' => 'The user name',
      'x' => 'The job name'
    }

    DESC = [
      'The paths may contain various special characters which will be replaced:',
      '',
      *NUMERIC_KEYS.map { |k, d| " * `%#{k}`: #{d}" },
      *ALPHA_KEYS.map { |k, d| " * `%#{k}`: #{d}" },
      " * `%%`: Escape a literal percent character '%', instead of a special directive",
      ' * `%<char>`: All other characters form an invalid replacement'
    ].join("\n")

    ALL_CHARS = [*ALPHA_KEYS.keys, *NUMERIC_KEYS.keys.join('')]
    # NOTE: The \\d is converted to \d via string interpolation before typecasting to regex
    PCT_REGEX = Regexp.new "%+\\d*[#{ALL_CHARS.join('')}]?"
    PAD_REGEX = /(\d*).\Z/

    attr_reader :node, :job, :task

    def initialize(node:, job:, task: nil)
      @node = node
      @job = job
      @task = task
    end

    def pct_a
      task ? task.array_index : 0
    end

    def pct_A
      task ? job.id : ''
    end

    def pct_j
      task ? '' : job.id
    end

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

    def render(path)
      path.gsub(PCT_REGEX) do |match|
        if match[-1] == '%' || match.count('%').even?
          match
        else
          raw = send("pct_#{match[-1]}").to_s
          diff = PAD_REGEX.match(match).captures[0].to_i - raw.length
          diff = 0 if diff < 0
          value = '0' * diff + raw
          match.sub(/%[^%]*\Z/, value)
        end.gsub('%%', '%')
      end
    end
  end
end
