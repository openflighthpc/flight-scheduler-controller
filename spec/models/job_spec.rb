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
  it 'is valid' do
    expect(build(:job)).to be_valid
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
end
