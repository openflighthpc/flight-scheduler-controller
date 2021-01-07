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

require 'spec_helper'

RSpec.describe FlightScheduler::TimeResolver do
  describe '#resolve' do
    def calculate(d: 0, h: 0, m: 0, s: 0)
      ((d * 24 + h) * 60 + m) * 60 + s
    end

    it 'parses intergers as minutes' do
      mins = rand(60)
      expect(described_class.new(mins).resolve).to eq(calculate(m: mins))
    end

    it 'parses MINUTES syntax' do
      mins = rand(60)
      expect(described_class.new(mins.to_s).resolve).to eq(calculate(m: mins))
    end

    it 'parses MINUTES:SECONDS syntax' do
      minutes = rand(60)
      seconds = rand(60)
      str = "#{minutes}:#{seconds}"
      expect(described_class.new(str).resolve).to eq(
        calculate(m: minutes, s: seconds)
      )
    end

    it 'parses HOURS:MINUTES:SECONDS syntax' do
      hours   = rand(24)
      minutes = rand(60)
      seconds = rand(60)
      str = "#{hours}:#{minutes}:#{seconds}"
      expect(described_class.new(str).resolve).to eq(
        calculate(h: hours, m: minutes, s: seconds)
      )
    end

    it 'parses DAYS-HOURS syntax' do
      days  = rand(30)
      hours = rand(24)
      str = "#{days}-#{hours}"
      expect(described_class.new(str).resolve).to eq(
        calculate(d: days, h: hours)
      )
    end

    it 'parses DAYS-HOURS:MINUTES syntax' do
      days    = rand(30)
      hours   = rand(24)
      minutes = rand(60)
      str = "#{days}-#{hours}:#{minutes}"
      expect(described_class.new(str).resolve).to eq(
        calculate(d: days, h: hours, m: minutes)
      )
    end

    it 'parses DAYS-HOURS:MINUTES:SECONDS syntax' do
      days    = rand(30)
      hours   = rand(24)
      minutes = rand(60)
      seconds = rand(60)
      str = "#{days}-#{hours}:#{minutes}:#{seconds}"
      expect(described_class.new(str).resolve).to eq(
        calculate(d: days, h: hours, m: minutes, s: seconds)
      )
    end

    it 'returns nil for non-matching syntax' do
      expect(described_class.new('foobar').resolve).to be_nil
    end
  end
end
