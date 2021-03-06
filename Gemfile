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

source "https://rubygems.org"

git_source(:github) {|repo_name| "https://github.com/#{repo_name}" }

gem 'activemodel', require: 'active_model'
gem 'activesupport', require: 'active_support'
gem 'async-websocket'
gem 'concurrent-ruby', require: 'concurrent'
gem 'falcon'
gem 'json_schemer'
gem 'sinatra'
gem 'sinatra-cors'
gem 'sinja', '>= 1.3.0'
gem 'swagger-blocks'

group :test do
  group :development do
    gem 'pry'
    gem 'pry-byebug'
  end

  gem 'rspec'
  gem 'fakefs', require: 'fakefs/safe'
  gem 'rack-test'
  gem 'factory_bot'
end
