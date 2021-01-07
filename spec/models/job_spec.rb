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

RSpec.describe Job, type: :model do
  it 'is valid' do
    expect(build(:job)).to be_valid
  end

  describe '#allocated?' do
    context 'when the job is an array job' do
      let(:job) { build(:job, array: '1,2') }

      it 'returns true if there is an allocation' do
        allow(job).to receive(:allocation).and_return(Object.new)
        expect(job.allocated?).to be true
      end

      it 'returns false if there is not an allocation' do
        allow(job).to receive(:allocation).and_return(nil)
        expect(job.allocated?).to be false
        expect(job.allocated?(include_tasks: true)).to be false
      end

      it 'returns true if a task has an allocation and tasks are interrogated' do
        task = job.task_generator.next_task
        allow(job).to receive(:allocation).and_return(nil)
        allow(task).to receive(:allocation).and_return(Object.new)
        allow(FlightScheduler.app.job_registry).to receive(:tasks_for).and_return([task])

        expect(job.allocated?(include_tasks: true)).to be true
        expect(job.allocated?(include_tasks: false)).to be false
      end
    end

    context 'when the job is not array job' do
      let(:job) { build(:job) }

      it 'returns true if there is an allocation' do
        allow(job).to receive(:allocation).and_return(Object.new)
        expect(job.allocated?).to be true
      end

      it 'returns false if there is not an allocation' do
        allow(job).to receive(:allocation).and_return(nil)
        expect(job.allocated?).to be false
      end

      it 'returns false if tasks are interrogated' do
        allow(job).to receive(:allocation).and_return(nil)
        allow(FlightScheduler.app.job_registry).to receive(:tasks_for).and_return([])

        expect(job.allocated?(include_tasks: true)).to be false
        expect(job.allocated?(include_tasks: false)).to be false
      end
    end
  end

  describe '#min_nodes' do
    # Ensure the min nodes is overridden
    let(:input_min_nodes) { raise NotImplementedError }

    subject do
      build(:job,
        id: SecureRandom.uuid,
        job_type: 'JOB',
        min_nodes: input_min_nodes,
        state: 'PENDING',
        username: 'flight'
      )
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

  describe '#reason_pending' do
    let(:input_min_nodes) { 10 }
    let(:input_state) { 'RUNNING' }

    subject do
      build(:job,
        id: SecureRandom.uuid,
        state: input_state,
        min_nodes: input_min_nodes
      )
    end

    let(:new_reason) { 'Priority' }

    Job::STATES.reject { |r| r == 'PENDING' }.each do |state|
      context "when in the #{state} state" do
        let(:input_state) { state }

        it 'forces the reason to be nil' do
          subject.reason_pending = new_reason
          expect(subject.reason_pending).to be_nil
        end
      end
    end

    context 'when in the PENDING state' do
      let(:input_state) { 'PENDING' }

      it 'returns the set reason' do
        subject.reason_pending = new_reason
        expect(subject.reason_pending).to eq(new_reason)
      end
    end
  end

  describe '#username' do
    it 'is invalid if missing' do
      expect(build(:job, username: nil)).not_to be_valid
    end
  end

  describe 'array jobs' do
    let(:job) do
      build(:job,
        array: input_array,
        id: SecureRandom.uuid,
        min_nodes: 1,
        state: 'PENDING',
        username: 'flight',
      ).tap do |job|
        job.batch_script = build(:batch_script, job: job)
      end
    end

    subject { job }

    context 'when given an array argument' do
      let(:input_array) { '1,2' }

      before(:each) do
        expect(job.job_type).to eq 'ARRAY_JOB'
      end

      describe 'array tasks' do
        it 'array tasks reference the array job' do
          task = subject.task_generator.next_task
          expect(task.array_job).to be job
        end

        it 'array tasks are ARRAY_TASKs' do
          task = subject.task_generator.next_task
          expect(task.job_type).to eq 'ARRAY_TASK'
        end

        it 'creates valid array tasks' do
          task = subject.task_generator.next_task
          expect(task).to be_valid
        end
      end
    end
  end

  describe 'updating state for an ARRAY_TASK' do
    before(:each) do
      expect(job.job_type).to eq 'ARRAY_JOB'
      expect(job.state).to eq 'PENDING'
      FlightScheduler.app.job_registry.send(:clear)
      FlightScheduler.app.job_registry.add(job)
      tasks.each do |task|
        expect(task.job_type).to eq 'ARRAY_TASK'
        expect(task.array_job).to eq job
        FlightScheduler.app.job_registry.add(task)
      end
    end

    let(:job) do
      input_array = "1,2,3"
      build(
        :job,
        array: input_array,
        id: SecureRandom.uuid,
        min_nodes: 1,
        state: 'PENDING',
        username: 'flight',
      ).tap do |job|
        job.batch_script = build(:batch_script, job: job)
      end
    end

    context 'when the ARRAY_JOB has pending tasks' do
      let(:tasks) do
        tasks = []
        2.times do
          task = job.task_generator.next_task
          tasks << task
          job.task_generator.advance_next_task
        end
        tasks
      end

      before(:each) do
        # There should still be at least one pending task left.
        expect(job.task_generator).not_to be_finished
      end

      it 'does not change the ARRAY_JOBs state' do
        expect { tasks.each { |task| task.state = 'COMPLETED' } }
          .not_to change { job.state }
      end

      context 'when the ARRAY_JOB is CANCELLING' do
        it 'cancels the job even though the task generator has not finished' do
          job.state = 'CANCELLING'
          expect { tasks.each { |task| task.state = 'CANCELLED' } }
            .to change { job.state }.to('CANCELLED')
        end
      end
    end

    context 'when the ARRAY_JOB has no pending tasks' do
      let(:tasks) do
        tasks = []
        while task = job.task_generator.next_task
          tasks << task
          job.task_generator.advance_next_task
        end
        tasks
      end

      it 'does nothing if there are running tasks' do
        idx_to_state = {0 => 'RUNNING', 1 => 'COMPLETED', 2 => 'COMPLETED'}
        expect do
          tasks.each_with_index do |task, idx|
            task.state = idx_to_state[idx]
          end
        end.not_to change { job.state }
      end

      it 'completes the job if all tasks are completed' do
        expect { tasks.each { |task| task.state = 'COMPLETED' } }
          .to change { job.state }.to('COMPLETED')
      end

      it 'fails the job if any tasks are failed' do
        idx_to_state = {0 => 'COMPLETED', 1 => 'FAILED', 2 => 'CANCELLED'}
        expect do
          tasks.each_with_index do |task, idx|
            task.state = idx_to_state[idx]
          end
        end.to change { job.state }.to('FAILED')
      end

      it 'cancels the job if any tasks are cancelled and none are failed' do
        idx_to_state = {0 => 'COMPLETED', 1 => 'COMPLETED', 2 => 'CANCELLED'}
        expect do
          tasks.each_with_index do |task, idx|
            task.state = idx_to_state[idx]
          end
        end.to change { job.state }.to('CANCELLED')
      end
    end
  end
end
