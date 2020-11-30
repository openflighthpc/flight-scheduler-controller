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

RSpec.describe Node, type: :model do
  subject { Node.new(name: 'node01') }

  context 'with a dual cpu node' do
    subject { build(:node, cpus: 2) }

    describe '#satisfies' do
      it 'returns 2 for a single cpu job' do
        # NOTE: Ignores the minimum node count
        job = build(:job, cpus_per_node: 1, min_nodes: 10)
        expect(subject.satisfies(job)).to eq(2)
      end

      it 'returns 1 for a dual cpu job' do
        job = build(:job, cpus_per_node: 2)
        expect(subject.satisfies(job)).to eq(1)
      end

      it 'returns 0 for insufficient cpus' do
        job = build(:job, cpus_per_node: 3)
        expect(subject.satisfies(job)).to eq(0)
      end
    end
  end
end
