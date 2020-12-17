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
require 'active_support/core_ext/hash/keys'
require 'yaml'

module FlightScheduler
  class Configuration
    class Loader
      def initialize(root, config_files)
        @root = root
        @config_files = Array(config_files)
      end

      def load
        merged = defaults.merge(from_config_files).merge(from_env_vars)
        Configuration.new.tap do |config|
          merged.each do |key, value|
            config.send("#{key}=", value)
          end
        end
      rescue => e
        raise e, "Cannot load configuration:\n#{e.message}", e.backtrace
      end

      def defaults
        Configuration::ATTRIBUTES.reduce({}) do |accum, attr|
          if attr.key?(:default)
            accum[attr[:name]] = attr[:default].respond_to?(:call) ?
              attr[:default].call(@root) :
              attr[:default]
          end
          accum
        end
          .deep_transform_keys(&:to_s)
      end

      def from_config_files
        @config_files.reduce({}) do |accum, config_file|
          if config_file.exist?
            config = ( YAML.load_file(config_file) || {} ).deep_transform_keys(&:to_s)
          end
          accum.merge(config || {})
        end
      rescue ::Psych::SyntaxError => e
        raise "YAML syntax error occurred while parsing #{@config_file}. " \
          "Please note that YAML must be consistently indented using spaces. Tabs are not allowed. " \
          "Error: #{e.message}"
      end

      def from_env_vars
        Configuration::ATTRIBUTES.reduce({}) do |accum, attr|
          if attr[:env_var]
            env_var = "FLIGHT_SCHEDULER_#{attr[:name].upcase}"
            unless ENV[env_var].nil?
              accum[attr[:name]] = ENV[env_var]
            end
          end
          accum
        end
      end
    end
  end
end
