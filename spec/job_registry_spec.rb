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

RSpec.describe FlightScheduler::JobRegistry do
  let(:job) { build(:job) }
  subject { described_class.new }

  describe '#add' do
    specify 'not adding a job does not allow its retrieval' do
      expect(subject[job.id]).to be_nil
    end

    specify 'adding a job allows its retrieval' do
      subject.add(job)
      expect(subject[job.id]).to eq job
    end
  end

  describe '#delete' do
    specify 'deleting a job prevents its retrieval' do
      subject.add(job)
      expect(subject[job.id]).to eq job
      subject.delete(job.id)
      expect(subject[job.id]).to be_nil
    end

    specify 'deleting an array job prevents retrieval of its tasks' do
      job = build(:job, array: '1-2')
      subject.add(job)
      # The scheduler would normally do this part.
      tasks = 2.times.map do
        task = job.task_generator.next_task
        job.task_generator.advance_array_index
        subject.add(task)
        task.state = 'RUNNING'
        task
      end

      # Check that everything has been setup correctly.
      expect(job.job_type).to eq 'ARRAY_JOB'
      expect(subject.jobs.length).to eq 3
      tasks.each do |task|
        expect(subject[task.id]).to eq task
      end

      # Now let's exercise the registry and test.
      subject.delete(job)
      expect(subject.jobs.length).to eq 0
      tasks.each do |task|
        expect(subject[task.id]).to be_nil
      end
    end
  end

  describe '#jobs_in_state' do
    before(:each) do
      Job::STATES.each do |state|
        2.times do
          subject.add(build(:job, state: state))
        end
      end
    end

    context 'when given a single state' do
      specify 'it returns only jobs in the given state' do
        Job::STATES.each do |state|
          found_states = subject.jobs_in_state(state).map(&:state).uniq
          expect(found_states).to eq [state]
        end
      end

      specify 'it returns all jobs in the given state' do
        Job::STATES.each do |state|
          expect(subject.jobs_in_state(state).length).to eq 2
        end
      end
    end

    context 'when given multiple states' do
      let(:states) { %w(CANCELLING CANCELLED) }

      specify 'it returns only jobs in the correct states' do
        found_states = subject.jobs_in_state(states).map(&:state).uniq
        expect(found_states).to eq states
      end

      specify 'it returns all jobs in any of given states' do
        expect(subject.jobs_in_state(states).length).to eq 4
      end
    end
  end
end
