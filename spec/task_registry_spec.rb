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

RSpec.describe FlightScheduler::TaskRegistry do
  let(:job) { raise NotImplementedError }
  subject { described_class.new(job) }

  context 'with an array job' do
    let(:job) { build(:job, array: '1-10', min_nodes: 4) }

    describe '#pending_task' do
      it 'returns the first pending task' do
        task = subject.pending_task
        expect(task.array_index).to eq(1)
      end

      it 'does not increment past pending tasks' do
        subject.pending_task
        subject.pending_task
        subject.pending_task
        task = subject.pending_task
        expect(task.array_index).to eq(1)
      end
    end
  end
end
