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
require 'async'

require_relative '../../app/models/node'
require_relative '../../app/models/partition'

module FlightScheduler
  class Configuration
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
    ]
    attr_accessor(*ATTRIBUTES.map { |a| a[:name] })

    def self.load(root)
      Loader.new(root, root.join('etc/flight-scheduler-controller.yaml')).load
    end

    def log_level=(level)
      @log_level = level
      Async.logger.send("#{@log_level}!")
    end

    def nodes
      @nodes ||= NodeRegistry.new
    end

    def partitions=(partition_specs)
      @partitions = partition_specs.map do |spec|
        partition_nodes = spec['nodes'].map { |node_name| nodes.fetch_or_add(node_name) }
        Partition.new(default: spec['default'], name: spec['name'], nodes: partition_nodes, time_limit: spec['time_limit'])
      end
    end
  end
end
