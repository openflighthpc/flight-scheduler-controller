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

require 'spec_helper'

RSpec.describe FlightScheduler::NodeMatcher do
  specify 'unrecognised keys are invalid' do
    expect(described_class.new('foobar')).not_to be_valid
  end

  specify 'recognised strings are valid' do
    expect(described_class.new('name')).to be_valid
  end

  specify 'unrecognised matchers are invalid' do
    expect(build(:node_matcher, foobiz: nil)).not_to be_valid
  end

  ['lt', 'gt', 'gte', 'lte'].each do |type|
    specify "#{type} must be an integer" do
      expect(build(:node_matcher, type.to_sym => '1')).not_to be_valid
      expect(build(:node_matcher, type.to_sym => 1)).to be_valid
    end
  end

  describe '#regex' do
    it 'can match' do
      expect(build(:node_matcher, regex: 'node').regex('node')).to be true
    end

    it 'does not perform a bound match by default' do
      expect(build(:node_matcher, regex: 'node').regex('foo-node-bar')).to be true
    end

    it 'can preform a bound match' do
      expect(build(:node_matcher, regex: '\Anode\Z').regex('node')).to be true
      expect(build(:node_matcher, regex: '\Anode\Z').regex('foo-node-bar')).to be false
    end

    it 'handles integers' do
      expect(build(:node_matcher, regex: '\d+').regex(1)).to be true
    end

    it 'can wild card match' do
      expect(build(:node_matcher, regex: '\Anode\d+\Z').regex('node01')).to be true
    end
  end
end
