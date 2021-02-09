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
require 'concurrent'

module FlightScheduler
  class ProcessorRegistry
    class DuplicateConnection < RuntimeError; end
    class UnconnectedError < RuntimeError ; end

    def self.processor_id(processor)
      case processor
      when EventProcessor::DaemonProcessor
        [:daemons, processor.node_name]
      when EventProcessor::JobProcessor
        [:jobs, "#{processor.node_name}-#{processor.job_id}"]
      when EventProcessor::StepProcessor
        [:steps, "#{processor.node_name}-#{processor.job_id}-#{processor.step_id}"]
      else
        raise UnexpectedError, "Not a valid processor: #{processor.inspect}"
      end
    end

    def initialize
      @mutex = Mutex.new
      @processors = {
        daemons: {},
        jobs: {},
        steps: {}
      }
    end

    def add(processor)
      type, id = self.class.processor_id(processor)

      @mutex.synchronize do
        if @processors[type].key?(id)
          raise DuplicateConnection, "#{type} process - #{id}"
        end
        @processors[type][id] = processor
      end

      # TODO: Move somewhere else
      event.node_connected if type == :daemons
    end

    def remove(processor)
      type, id = self.class.processor_id(processor)

      @mutex.synchronize do
        @processors[type].delete(id)
      end
    end

    def connected_nodes
      @mutex.synchronize do
        @processors[:daemons].keys
      end
    end

    def connected?(node_name)
      @mutex.synchronize do
        @processors[:daemons].key?(node_name)
      end
    end

    def event
      EventProcessor
    end

    def daemon_processor_for(node_name)
      @mutex.synchronize do
        @processors[:daemons][node_name]
      end
    end

    def job_processor_for(node_name, job_id)
      # TODO: Use the cached jobs processor
      con = daemon_processor_for(node_name).connection
      FlightScheduler::EventProcessor::JobProcessor.new(con, node_name, job_id)
    end
  end
end
