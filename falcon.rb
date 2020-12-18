#!/usr/bin/env -S falcon host
# frozen_string_literal: true
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

# Force Falcon to run in threaded mode.  Without this, Falcon will run the
# rack app in a separate process, which currently causes a number of
# headaches.
#
# Addressing these headaches is likely to involve one of 1) adding job
# persistence; 2) abandoning Falcon and Async::WebSocket.
class Falcon::Command::Host
  def container_class
    Async::Container::Threaded
  end
end

# NOTE: Do not require the persistence instances here as the Async event
# loop has not started. This will cause any asynchronous tasks to block the
# main thread. Asynchronous tasks need to be started within the rack config.ru
require_relative 'config/boot'

load :rack, :supervisor

hostname = File.basename(__dir__)
rack hostname do
  endpoint Async::HTTP::Endpoint.parse(FlightScheduler.app.config.bind_address)
  count 1
  verbose false
end

supervisor
