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

RSpec.describe FlightScheduler::ArrayTaskGenerator do
  let(:job) { raise NotImplementedError }
  subject { described_class.new(job) }

  context 'with an array job with sequential array indexes' do
    let(:max) { 10 }
    let(:job) {
      build(
        :job,
        array: "1-#{max}",
        min_nodes: 4,
        cpus_per_node: 2,
        gpus_per_node: 3,
        memory_per_node: 2048,
      )
    }

    describe '#next_task' do
      it 'returns the first pending task' do
        task = subject.next_task
        expect(task.array_index).to eq(1)
      end

      it 'does not advance the task' do
        [
          subject.next_task,
          subject.next_task,
          subject.next_task,
          subject.next_task,
        ].permutation(2) do |task1, task2|
          expect(task1).to eq task2
          expect(task1.array_index).to eq(1)
          expect(task2.array_index).to eq(1)
        end
      end

      it 'returns nil once advanced beyond the available tasks' do
        max.times { subject.advance_next_task }
        expect(subject.next_task).to be_nil
      end

      %w(cpus_per_node gpus_per_node memory_per_node).each do |attribute|
        it "returns a task with the correct #{attribute}" do
          expect(subject.next_task.send(attribute)).to eq job.send(attribute)
        end
      end
    end

    describe '#finished?' do
      it { should_not be_finished }

      context 'when there is a next task' do
        before do
          expect(subject.next_task).not_to be_nil
        end

        it { should_not be_finished }
      end

      context 'when there is not a next task' do
        before do
          max.times { subject.advance_next_task }
          expect(subject.next_task).to be_nil
        end

        it { should be_finished }
      end
    end

    describe '#advance_next_task' do
      it 'advances the task returned by #next_task' do
        task1 = subject.next_task
        subject.advance_next_task
        task2 = subject.next_task

        expect(task1).not_to eq task2
        expect(task1.array_index + 1).to eq task2.array_index
      end
    end
  end
end
