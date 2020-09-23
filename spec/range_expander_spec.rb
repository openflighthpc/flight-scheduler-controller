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

RSpec.describe FlightScheduler::RangeExpander do
  let(:range) { raise NotImplementedError }
  let(:expected) { raise NotImplementedError }

  subject { described_class.split(range) }

  shared_examples 'expands-range' do
    it { should be_valid }

    describe '#expand' do
      it 'expands the parts' do
        expect(subject.expand).to contain_exactly(*expected)
      end
    end
  end

  context 'with a single value' do
    let(:range) { '1' }
    let(:expected) { [1] }

    include_examples 'expands-range'
  end

  context 'with a comma separated list' do
    let(:range) { expected.join(',') }
    let(:expected) { [1, 2, 3, 6, 7, 10, 12, 4] }

    include_examples 'expands-range'
  end

  context 'with a dashed range' do
    let(:range) { "#{expected.first}-#{expected.last}" }
    let(:expected) { [1,2,3,4,5,6,7,8,9,10] }

    include_examples 'expands-range'
  end

  context 'with a multiplier dashed range' do
    let(:range) { "#{expected.first}-#{expected.last + 1}:3" }
    let(:expected) { [5, 8, 11, 14, 17, 20] }

    include_examples 'expands-range'
  end

  context 'with multiple comman separated dashed ranges' do
    let(:range1) { "#{expected1.first}-#{expected1.last}" }
    let(:expected1) { [1,2,3,4] }

    let(:range2) { "#{expected2.first}-#{expected2.last}:2" }
    let(:expected2) { [8,10,12,14] }

    # Intentionally non sequential
    let(:range) { "#{range2},#{range1}" }
    let(:expected) { [*expected2, *expected1] }

    include_examples 'expands-range'
  end

  context 'with invalid ranges' do
    context 'with an alpha numeric string' do
      context 'when simple' do
        let(:range) { '64word12' }
        it { should_not be_valid }
      end

      context 'when dashed' do
        let(:range) { 'something-2159' }
        it { should_not be_valid }
      end
    end

    context 'wtih an empty string' do
      let(:range) { '' }
      it { should_not be_valid }
    end

    context 'with an inverted dashed range' do
      let(:range) { "10-1" }
      it { should_not be_valid }
    end

    context 'with a zero multiplier dashed range' do
      let(:range) { "1-10:0" }
      it { should_not be_valid }
    end

    context 'with a double dash' do
      let(:range) { '1-10-20' }
      it { should_not be_valid }
    end

    context 'with a missing alpha in dash' do
      let(:range) { '-10' }
      it { should_not be_valid }
    end

    context 'with a missing omega in dash' do
      let(:range) { '10-' }
      it { should_not be_valid }
    end

    # NOTE: This could default to 1
    context 'with a missing muliplier in dash' do
      let(:range) { '1-10:' }
      it { should_not be_valid }
    end

    # NOTE: These could be ignored
    context 'with missing csv values' do
      let(:range) { '1,,2' }
      it { should_not be_valid }
    end
  end
end
