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

    describe '#running_task' do
      it 'returns empty by default' do
        expect(subject.running_tasks).to be_empty
      end
    end

    context 'when the pending task transitions to: RUNNING' do
      let(:first) { subject.pending_task }
      before { first.state = 'RUNNING' }

      describe '#pending_task' do
        it 'moves to the next task' do
          expect(subject.pending_task.array_index).to eq(2)
        end
      end

      describe '#running_tasks' do
        it 'includes the first task' do
          expect(subject.running_tasks).to contain_exactly(first)
        end
      end

      describe '#past_tasks' do
        it 'does not incude the first task' do
          expect(subject.past_tasks).to be_empty
        end
      end
    end

    (Job::STATES.dup - ['RUNNING', 'PENDING']).each do |state|
      context "when the pending tasks transitions to: #{state}" do
        let(:first) { subject.pending_task }
        before { first.state = state }

        describe '#pending_task' do
          it 'returns the next task' do
            expect(subject.pending_task.array_index).to eq(2)
          end
        end

        describe '#running_tasks' do
          it 'does not include the previous task' do
            expect(subject.running_tasks).to be_empty
          end
        end

        describe '#past_tasks' do
          it 'includes the previous task' do
            expect(subject.past_tasks).to contain_exactly(first)
          end
        end
      end
    end
  end
end
