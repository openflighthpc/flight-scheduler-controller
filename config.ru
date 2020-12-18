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

require_relative 'config/boot'
require_relative 'config/initializers/persistence'

if FlightScheduler.env.development?
  begin
      Bundler.require(:development)
  rescue StandardError, LoadError
    $stderr.puts "An error occurred when enabling development mode!"
  end
end

API_VERSION = 'v0'

require_relative 'app.rb'
require_relative 'app/websocket_app'
require_relative 'app/swagger_app'

app = Rack::Builder.app do
  map("/#{API_VERSION}/docs") { run SwaggerApp }
  map("/#{API_VERSION}/ws") { run WebsocketApp.new }
  map("/#{API_VERSION}") { run App }
end

run app
