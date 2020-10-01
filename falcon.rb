#!/usr/bin/env -S falcon host
# frozen_string_literal: true

load :rack, :supervisor

address = ENV.fetch(
  'FLIGHT_SCHEDULER_CONTROLLER_BIND_ADDRESS',
  'http://127.0.0.1:6307'
)

hostname = File.basename(__dir__)
rack hostname do
  endpoint Async::HTTP::Endpoint.parse(address)
  count 1
  verbose false
end

supervisor
