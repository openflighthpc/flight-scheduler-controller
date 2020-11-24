#!/usr/bin/env -S falcon host
# frozen_string_literal: true

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

require_relative 'config/boot'

load :rack, :supervisor

hostname = File.basename(__dir__)
rack hostname do
  endpoint Async::HTTP::Endpoint.parse(FlightScheduler.app.config.bind_address)
  count 1
  verbose false
end

supervisor
