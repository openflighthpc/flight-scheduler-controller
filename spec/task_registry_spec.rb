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
    let(:max) { 10 }
    let(:job) { build(:job, array: "1-#{max}", min_nodes: 4) }

    describe '#next_task' do
      it 'returns the first pending task' do
        task = subject.next_task
        expect(task.array_index).to eq(1)
      end

      it 'does not increment past pending tasks' do
        subject.next_task
        subject.next_task
        subject.next_task
        task = subject.next_task
        expect(task.array_index).to eq(1)
      end

      it 'returns nil after all tasks have been started' do
        (max + 2).times { subject.next_task&.state = 'RUNNING' }
        expect(subject.next_task).to be_nil
      end
    end

    describe '#running_task' do
      it 'returns empty by default' do
        expect(subject.running_tasks).to be_empty
      end
    end

    describe '#limit?' do
      it { should_not be_limit }

      context 'when a job is running on each node' do
        before do
          job.min_nodes.times do
            subject.next_task.state = 'RUNNING'
          end
        end

        it { should be_limit }

        context 'when a job finishes' do
          before do
            subject.running_tasks.first.state = 'FINISHED'
          end

          it { should_not be_limit }
        end
      end
    end

    describe '#finished?' do
      it { should_not be_finished }

      context 'when all the jobs are RUNNING' do
        before do
          max.times { subject.next_task.state = 'RUNNING' }
        end

        it { should_not be_finished }
      end

      (Job::STATES.dup - ['RUNNING', 'PENDING']).each do |state|
        context "when all the jobs are: #{state}" do
          before do
            max.times { subject.next_task.state = state }
          end

          it { should be_finished }
        end
      end
    end

    context 'when the pending task transitions to: RUNNING' do
      let(:first) { subject.next_task }
      before { first.state = 'RUNNING' }

      describe '#next_task' do
        it 'moves to the next task' do
          expect(subject.next_task.array_index).to eq(2)
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

    context 'when the pending task is allocated' do
      let(:first) { subject.next_task }
      before { allow(first).to receive(:allocated?).and_return(true) }

      describe '#next_task' do
        it 'moves to the next task' do
          expect(subject.next_task.array_index).to eq(2)
        end
      end

      describe '#running_tasks' do
        it 'includes the first task' do
          subject.next_task # This test needs a double refresh
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
        let(:first) { subject.next_task }
        before { first.state = state }

        describe '#next_task' do
          it 'returns the next task' do
            expect(subject.next_task.array_index).to eq(2)
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

    context 'when a running task transitions to FINISHED' do
      let(:task) { subject.next_task }
      before do
        allow(task).to receive(:allocated?).and_return(true)
        task.state = 'RUNNING'
        subject.send(:refresh)
        task.state = 'FINISHED'
      end

      describe '#running_task' do
        it 'does not contain the task' do
          expect(subject.running_tasks).to be_empty
        end
      end

      describe '#past_tasks' do
        it 'contains the task' do
          expect(subject.past_tasks).to contain_exactly(task)
        end
      end
    end
  end
end
