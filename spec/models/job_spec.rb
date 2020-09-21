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

RSpec.describe Job, type: :model do
  describe '#min_nodes' do
    let(:input_min_nodes) { raise NotImplementedError }

    subject do
      described_class.new(id: SecureRandom.uuid,
                          state: 'PENDING',
                          script_provided: true,
                          min_nodes: input_min_nodes)
    end

    context 'when it is an integer string' do
      let(:input_min_nodes) { '10' }

      it { is_expected.to be_valid }

      it 'returns the integer' do
        expect(subject.min_nodes).to eq(input_min_nodes.to_i)
      end
    end

    context 'when it is a "k" (x1024) multiple' do
      let(:input_min_nodes) { '10k' }

      it { is_expected.to be_valid }

      it 'returns the multiple as an integer' do
        expect(subject.min_nodes).to eq(input_min_nodes.gsub('k', '').to_i * 1024)
      end
    end

    context 'when it is a "m" (x1048576) multiple' do
      let(:input_min_nodes) { '10m' }

      it { is_expected.to be_valid }

      it 'returns the multiple as an integer' do
        expect(subject.min_nodes).to eq(input_min_nodes.gsub('m', '').to_i * 1048576)
      end
    end
  end
end
