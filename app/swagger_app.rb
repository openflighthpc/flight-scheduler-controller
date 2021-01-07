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

class SwaggerApp < Sinatra::Base
  include Swagger::Blocks

  register Sinatra::Cors

  set :allow_origin, '*'
  set :allow_methods, "GET,HEAD"
  set :allow_headers, "content-type,if-modified-since"

  set :expose_headers, "location,link"

  swagger_root do
    key :swagger, '2.0'
    key :basePath, "/#{::API_VERSION}"
    info do
      key :title, 'Flight Scheduler Controller'
      key :description, 'WIP'
      contact do
        key :name, 'Alces Flight'
      end
      license do
        key :name, 'EPL-2.0'
      end
    end
    security_definition :BasicAuth do
      key :type, :basic
    end
  end

  classes = [*BaseSerializer.subclasses, WebsocketApp, App, self]
  SWAGGER_DOC = Swagger::Blocks.build_root_json(classes).to_json
  get '/' do
    SWAGGER_DOC
  end
end
