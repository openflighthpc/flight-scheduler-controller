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

  describe '#remove_old_jobs' do
    before(:each) do
      subject.add(job)
      expect(subject[job.id]).to eq job
    end

    %w(COMPLETED CANCELLED FAILED).each do |state|
      context "for #{state} JOB jobs" do
        let(:job) { super().tap { |j| j.state = state } }

        it 'removes the job' do
          subject.remove_old_jobs
          expect(subject[job.id]).to be_nil
        end
      end

      context "for #{state} ARRAY_TASK jobs" do
        let(:array_job) do
          build(:job, array: '1,2,3').tap do |array_job|
            subject.add(array_job)
          end
        end

        let(:job) do
          array_job.task_generator.next_task.tap do |task|
            array_job.task_generator.advance_next_task
            task.state = state
          end
        end

        it 'does not remove the job' do
          # We don't remove the array task, because the array job is still
          # pending.  Let's check that precondition.
          expect(job.array_job).to be_pending

          subject.remove_old_jobs
          expect(subject[job.id]).to eq job
        end
      end

      context "for #{state} ARRAY_JOB jobs" do
        let(:job) do
          build(:job, array: '1,2,3', state: state)
        end

        let!(:tasks) do
          tasks = []
          while task = job.task_generator.next_task
            job.task_generator.advance_next_task
            tasks << task
            task.state = state
            subject.add(task)
          end
          tasks
        end

        it 'removes the array job' do
          subject.remove_old_jobs
          expect(subject[job.id]).to be_nil
        end

        it 'removes the array tasks' do
          subject.remove_old_jobs
          tasks.each do |task|
            expect(subject[task.id]).to be_nil
          end
          expect(subject.tasks_for(job)).to eq []
        end
      end
    end
  end

  describe '#delete' do
    specify 'deleting a job prevents its retrieval' do
      subject.add(job)
      expect(subject[job.id]).to eq job
      subject.send(:delete, job.id)
      expect(subject[job.id]).to be_nil
    end

    specify 'deleting an array job prevents retrieval of its tasks' do
      job = build(:job, array: '1-2')
      subject.add(job)
      # The scheduler would normally do this part.
      tasks = 2.times.map do
        task = job.task_generator.next_task
        job.task_generator.advance_next_task
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
      subject.send(:delete, job)
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
