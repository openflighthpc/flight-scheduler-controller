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

require 'active_support/core_ext/module/delegation'
require 'concurrent'

module FlightScheduler
  # Class to store configuration and provide a singleton resource to lookup
  # that configuration.  Similar in nature to `Rails.app`.
  class Application
    def self.build
      allocations = AllocationRegistry.new
      job_registry = JobRegistry.new
      plugins = FlightScheduler::Plugins.new
      connection_registry = FlightScheduler::ConnectionRegistry.new
      schedulers = Schedulers.new

      Application.new(
        allocations: allocations,
        job_registry: job_registry,
        plugins: plugins,
        connection_registry: connection_registry,
        schedulers: schedulers
      )
    end

    delegate :add, :remove,
      to: :@connection_registry,
      prefix: :connections
    delegate :connected?, :connection_for, :connected_nodes,
      to: :@connection_registry,
      prefix: false

    attr_reader :allocations
    attr_reader :connection_registry
    attr_reader :job_registry
    attr_reader :nodes
    attr_reader :plugins
    attr_reader :schedulers

    def initialize(
      allocations:,
      connection_registry:,
      job_registry:,
      plugins:,
      schedulers: 
    )
      @allocations = allocations
      @connection_registry = connection_registry
      @job_registry = job_registry
      @plugins = plugins
      @schedulers = schedulers
      @dispatcher = EventDispatcher.new
    end

    def scheduler
      @scheduler ||=
        begin
          algorithm = config.scheduler_algorithm
          @schedulers.load(algorithm.to_sym)
        end
    end

    def persist_scheduler_state
      @plugins.lookup_type('scheduler_state')&.save
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

    def dispatch_event(event, *args)
      @dispatcher.send(event, *args)
    end

    def setup_connection(connection)
      Connection.process(connection)
    end

    def config
      return @config if @config
      Configuration.load(root).tap do |config|
        @config = config
        partitions.each(&:validate!)
      end
    end
    alias_method :load_configuration, :config

    def root
      @root ||= Pathname.new(__dir__).join('../../').expand_path
    end

    def init_periodic_processors
      Async.logger.info("Initializing cleanup periodic processor")
      opts = {
        execution_interval: config.timer_interval,
        timeout_interval: config.timer_timeout,
      }
      Concurrent::TimerTask.new(**opts) do
        Async.logger.debug("Running cleanup periodic processor")
        job_registry.remove_old_jobs
        job_registry.jobs_in_state(Job::TERMINAL_STATES).each do |job|
          Async.logger.debug("Removing allocation for job in terminal state: id=#{job.display_id} state=#{job.state}")
          FlightScheduler::Deallocation::Job.new(job).call
        end
        persist_scheduler_state
        Async.logger.debug("Done running cleaup periodic processor")
      end.execute

      Async.logger.info("Initializing status periodic processor")
      Concurrent::TimerTask.new(execution_interval: config.status_update_period) do
        partitions.each { |p| p.script_runner.status }
      end.execute
    end
  end
end
