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

require 'timeout'
require 'base64'
require 'open3'

module FlightScheduler
  module Auth
    class AuthenticationError < RuntimeError; end

    def self.user_from_header(auth_header)
      auth_type = lookup(FlightScheduler.app.config.auth_type)
      auth_type.call(auth_header)
    end

    def self.lookup(name)
      const_string = name.classify
      const_get(const_string)
    rescue NameError
      Async.logger.warn("Auth type not found: #{self}::#{const_string}")
      nil
    end

    module Basic
      def self.call(auth_header)
        if match = /Basic (.*)/.match(auth_header)
          Base64.decode64(match.captures[0]).split(':', 2).first
        end
      end
    end

    module Munge
      extend self

      def call(auth_header)
        unmunged_data =
          with_clean_env do
            Timeout.timeout(2) do
              Open3.popen2('unmunge') do |stdin, stdout, wait_thr|
                # We assume here that the writing auth_header to stdin won't
                # block.  For valid auth_headers this is a valid assumption.
                # For non-valid auth headers this may not be true, but the
                # timeout will rescue us.
                stdin.write(auth_header)
                unmunged_data = stdout.read
              end
            end
          end
        parse(unmunged_data)['USERNAME']
      rescue Timeout::Error
        Async.logger.warn("Timeout whilst running `unmunge`")
        nil
      rescue Error::ENOENT
        Async.logger.warn($!.message)
        nil
      end

      private

      def parse(result)
        result = {}
        result.each_line do |line|
          key, value = line.split(':')
          if key == 'UID'
            parts = value.split(' ')
            result['USERNAME'] = parts[0]
            result['UID'] = parts[1].match(/(\d+)/).to_s
          elsif key == 'GID'
            parts = value.split(' ')
            result['GID'] = parts[1].match(/(\d+)/).to_s
          else
            result[key] = value.strip
          end
        end
        Async.logger.debug("Munge data parsed as #{result.inspect}")
        result
      end

      def with_clean_env(&block)
        if Kernel.const_defined?(:OpenFlight) && OpenFlight.respond_to?(:with_standard_env)
          OpenFlight.with_standard_env(&block)
        else
          msg = Bundler.respond_to?(:with_unbundled_env) ? :with_unbundled_env : :with_clean_env
          Bundler.__send__(msg, &block)
        end
      end
    end
  end
end

