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
  # Class to store configuration and provide a singleton resource to lookup
  # that configuration.  Similar in nature to `Rails.app`.
  class Application

    attr_reader :allocations
    attr_reader :daemon_connections
    attr_reader :job_registry
    attr_reader :schedulers
    attr_reader :nodes

    def initialize(allocations:, daemon_connections:, job_registry:, schedulers:)
      @allocations = allocations
      @daemon_connections = daemon_connections
      @job_registry = job_registry
      @schedulers = schedulers
    end

    def event_processor
      EventProcessor
    end

    def scheduler
      @scheduler ||= @schedulers.load(:fifo)
    end

    def partitions
      config.partitions
    end

    def nodes
      config.nodes
    end

    def default_partition
      partitions.detect { |p| p.default? }
    end

    def config
      @config ||= Configuration.load(root)
    end
    alias_method :load_configuration, :config

    def root
      @root ||= Pathname.new(__dir__).join('../../').expand_path
    end

    def init_periodic_processor
      Async.logger.info("Initializing periodic processor")
      @timer ||=
        begin
          opts = {
            execution_interval: config.timer_interval,
            timeout_interval: config.timer_timeout,
          }
          timer = Concurrent::TimerTask.new(**opts) do
            Async.logger.debug("Running periodic processor")
            job_registry.remove_old_jobs
            Async.logger.debug("Done running periodic processor")
          end
          timer.execute
          timer
        end
    end
  end
end
