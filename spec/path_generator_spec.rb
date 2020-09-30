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

RSpec.describe FlightScheduler::PathGenerator do
  let(:job) { raise NotImplementedError }
  let(:task) { raise NotImplementedError }
  let(:node) { build(:node) }
  subject { described_class.new(node, job, task) }

  # TODO: When user + groups are implemented this will need updating
  let(:user_name) { Etc.getlogin }

  shared_examples 'shared-attributes' do
    describe '#pct_N' do
      it 'returns the node name' do
        expect(subject.pct_N).to eq(node.name)
      end
    end

    describe '#pct_u' do
      it 'returns the username' do
        expect(subject.pct_u).to eq(user_name)
      end
    end

    describe '#pct_x' do
      it "returns the job's script name" do
        expect(subject.pct_x).to eq(job.script_name)
      end
    end
  end

  context 'with a regular batch job' do
    let(:task) { nil }
    let(:job) { build(:job) }

    include_examples 'shared-attributes'

    describe '#pct_A' do
      it 'returns empty string' do
        expect(subject.pct_A).to eq('')
      end
    end

    describe '#pct_a' do
      it 'returns 0' do
        expect(subject.pct_a).to eq(0)
      end
    end
  end

  context 'with an array job and task' do
    let(:job_max) { 20 }
    let(:job) { build(:job, array: "1-#{job_max}") }

    # NOTE The task is deliberately detached from the job. This allows the spec
    # to test various attributes are coming from the `job` instead of task.array_job
    #
    # This ensure PathGenerator is decoupled from the data model
    let(:task_max) { 10 }
    let(:task) do
      build(:job, array: "1-#{task_max}", num_started: rand(task_max - 1)).task_registry.next_task
    end

    include_examples 'shared-attributes'

    describe '#pct_A' do
      it 'returns the job ID' do
        expect(subject.pct_A).to eq(job.id)
      end
    end

    describe '#pct_a' do
      it 'returns the task array index' do
        expect(subject.pct_a).to eq(task.array_index)
      end
    end
  end
end
