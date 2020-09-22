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
    # Ensure the min nodes is overridden
    let(:input_min_nodes) { raise NotImplementedError }

    subject do
      described_class.new(
        id: SecureRandom.uuid,
        job_type: 'JOB',
        min_nodes: input_min_nodes,
        script_name: 'something.sh',
        script_provided: true,
        state: 'PENDING',
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

  describe '#reason' do
    let(:input_min_nodes) { 10 }
    let(:input_state) { 'RUNNING' }

    subject do
      described_class.new(id: SecureRandom.uuid,
                          state: input_state,
                          script_name: 'something.sh',
                          script_provided: true,
                          min_nodes: input_min_nodes)
    end

    let(:new_reason) { 'Priority' }

    Job::STATES.reject { |r| r == 'PENDING' }.each do |state|
      context "when in the #{state} state" do
        let(:input_state) { state }

        it 'forces the reason to be nil' do
          subject.reason = new_reason
          expect(subject.reason).to be_nil
        end
      end
    end

    context 'when in the PENDING state' do
      let(:input_state) { 'PENDING' }

      it 'returns the set reason' do
        subject.reason = new_reason
        expect(subject.reason).to eq(new_reason)
      end
    end
  end

  describe 'array jobs' do
    let(:job) do
      described_class.new(
        array: input_array,
        id: SecureRandom.uuid,
        job_type: input_array ? 'ARRAY_JOB' : 'JOB',
        min_nodes: 1,
        script_name: 'something.sh',
        script_provided: true,
        state: 'PENDING',
      ).tap do |job|
        job.create_array_tasks if job.job_type == 'ARRAY_JOB'
      end
    end

    subject { job }

    context 'when given an array argument' do
      let(:input_array) { '1,2' }

      before(:each) do
        expect(job.job_type).to eq 'ARRAY_JOB'
      end

      describe 'array tasks' do
        subject { super().array_tasks }

        it 'creates the correct number of tasks' do
          expect(subject.length).to eq input_array.split(',').length
        end

        it 'creates tasks with the correct indexes' do
          expect(subject.map(&:array_index)).to contain_exactly('1', '2')
        end

        it 'array tasks reference the array job' do
          subject.each do |task|
            expect(task.array_job).to be job
          end
        end

        it 'creates valid array tasks' do
          subject.each do |task|
            expect(task).to be_valid
          end
        end
      end
    end

    context 'when not given an array' do
      let(:input_array) { nil }

      specify 'a job is a JOB' do
        expect(subject.job_type).to eq 'JOB'
      end

      describe 'array tasks' do
        subject { super().array_tasks }

        specify 'have not been created' do
          expect(subject).to be nil
        end
      end
    end
  end
end
