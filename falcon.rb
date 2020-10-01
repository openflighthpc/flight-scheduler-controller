#!/usr/bin/env -S falcon host
# frozen_string_literal: true

require_relative 'config/boot'

load :rack, :supervisor

hostname = File.basename(__dir__)
rack hostname do
  endpoint Async::HTTP::Endpoint.parse(FlightScheduler.app.config.bind_address)
  count 1
  verbose false
end

supervisor
