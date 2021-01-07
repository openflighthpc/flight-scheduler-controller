#==============================================================================
# Copyright (C) 2021-present Alces Flight Ltd.
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
require 'async'

require_relative '../../app/models/node'
require_relative '../../app/models/partition'

module FlightScheduler
  class Configuration
    class ConfigError < RuntimeError; end

    PRODUCTION_PATH = 'etc/flight-scheduler-controller.yaml'
    PATH_GENERATOR = ->(env) { "etc/flight-scheduler-controller.#{env}.yaml" }

    autoload(:Loader, 'flight_scheduler/configuration/loader')

    ATTRIBUTES = [
      {
        name: :auth_type,
        env_var: true,
        default: 'munge',
      },
      {
        name: :bind_address,
        env_var: true,
        default: 'http://127.0.0.1:6307',
      },
      {
        name: :cluster_name,
        env_var: true,
        default: '',
      },
      {
        name: :env_var_prefix,
        env_var: true,
        default: '',
      },
      {
        name: :spool_dir,
        env_var: true,
        default: ->(root) { root.join('var/spool') }
      },
      {
        name: :libexec_dir,
        env_var: true,
        default: ->(root) { root.join('libexec').to_s }
      },
      {
        name: :log_level,
        env_var: true,
        default: 'info',
      },
      {
        name: :partitions,
        env_var: false,
        default: [],
      },
      {
        name: :polling_timeout,
        env_var: false,
        default: 30
      },
      {
        name: :timer_interval,
        env_var: false,
        default: 60,
      },
      {
        name: :timer_timeout,
        env_var: false,
        default: 30,
      },
      {
        name: :min_debouncing,
        env_var: false,
        default: 30
      },
      {
        name: :max_debouncing,
        env_var: false,
        default: 600
      },
      {
        name: :status_update_period,
        env_var: false,
        default: 3600
      },
      {
        name: :scheduler_algorithm,
        env_var: true,
        default: 'backfilling',
      },
      {
        name: :scheduler_max_jobs_considered,
        env_var: false,
        default: 50,
      },
      {
        name: :generic_short_sleep,
        env_var: false,
        default: 1
      }
    ]
    attr_accessor(*ATTRIBUTES.map { |a| a[:name] })

    def self.load(root)
      if ENV['RACK_ENV'] == 'production'
        Loader.new(root, root.join(PRODUCTION_PATH)).load
      else
        paths = [
          root.join(PATH_GENERATOR.call(ENV['RACK_ENV'])),
          root.join(PATH_GENERATOR.call("#{ENV['RACK_ENV']}.local")),
        ]
        Loader.new(root, paths).load
      end
    end

    def log_level=(level)
      @log_level = level
      Async.logger.send("#{@log_level}!")
    end

    def nodes
      @nodes ||= NodeRegistry.new
    end

    def partitions=(partition_specs)
      builder = Partition::Builder.new(partition_specs)
      unless builder.valid?
        errors = builder.errors.map(&:dup)
        Async.logger.debug errors.to_json
        errors.each do |e|
          e.delete('root_schema')
          e.delete('schema')
          e.delete('schema_pointer')
        end
        raise ConfigError, <<~ERROR.chomp
          An error occurred when validating the partitions config:
          #{JSON.pretty_generate(errors)}
        ERROR
      end
      builder.to_node_names.each { |n| nodes.register_node(n) }
      @partitions = builder.to_partitions
    end
  end
end
