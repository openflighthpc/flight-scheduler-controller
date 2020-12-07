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
require_relative '../../lib/flight_scheduler/schedulers/fifo_scheduler'
require_relative 'shared_scheduler_spec'

RSpec.describe FifoScheduler, type: :scheduler do
  let(:partition) { Partition.new(name: 'all', nodes: nodes, max_time_limit: 10, default_time_limit: 5) }
  let(:nodes) {
    [
      Node.new(name: 'node01'),
      Node.new(name: 'node02'),
      Node.new(name: 'node03'),
      Node.new(name: 'node04'),
    ].tap do |a|
      a.each do |node|
        allow(node).to receive(:connected?).and_return true
      end
    end
  }

  let(:scheduler) { subject }
  let(:allocations) { FlightScheduler.app.allocations }
  let(:job_registry) { FlightScheduler.app.job_registry }
  subject { described_class.new }

  # TODO: The allocations and scheduler shouldn't have to be cleared like this
  #       Currently the scheduler contains global lookups and thus their can
  #       only be a single instance. Instead the scheduler should take the
  #       allocation registry as an input.
  #
  #       This will allow a new scheduler to be created for each spec,
  #       consider refactoring
  before(:each) { allocations.send(:clear); job_registry.send(:clear) }

  include_examples 'basic scheduler specs'

  describe '#allocate_jobs' do
    def make_job(job_id, min_nodes, **kwargs)
      build(:job, id: job_id, min_nodes: min_nodes, partition: partition, **kwargs)
    end

    def add_allocation(job, nodes)
      Allocation.new(job: job, nodes: nodes).tap do |allocation|
        allocations.add(allocation)
      end
    end

    context 'when there is an unallocated job that cannot be allocated' do
      let(:jobs) do
        min_node_requirements = [
          # Add two jobs both requiring a single node.
          1,
          1,

          # Add a job requiring three nodes.  The partition currently has only 2
          # nodes available.
          3,

          # Add two jobs both requiring a single node.  The partition has
          # sufficient nodes available for these jobs, but they are blocked by
          # the proceeding one.
          1,
          1,
        ]

        min_node_requirements.each_with_index.map do |min_nodes, job_id|
          make_job(job_id, min_nodes)
        end
      end

      # Only the first two jobs should be allocated.  The third job cannot be
      # allocated as their are not sufficient nodes left and the remaining
      # jobs are blocked by the third.
      let(:expected_allocated_jobs) { jobs[0...2] }
      let(:expected_unallocated_jobs) { jobs - expected_allocated_jobs }

      before do
        jobs.each { |j| job_registry.add(j) }
      end

      it 'creates allocations for the preceding jobs' do
        expect{ scheduler.allocate_jobs }.to \
          change { allocations.size }.by(expected_allocated_jobs.length)
        expected_allocated_jobs.each do |job|
          expect(allocations.for_job(job.id)).to be_truthy
        end
      end

      it 'does not create allocations for the following jobs' do
        scheduler.allocate_jobs

        expected_unallocated_jobs.each do |job|
          expect(allocations.for_job(job.id)).to be_nil
        end
      end

      it 'sets the first unallocated job reason to Resources' do
        first_unallocated = expected_unallocated_jobs.first
        scheduler.allocate_jobs
        expect(first_unallocated.reason_pending).to eq('Resources')
      end

      it 'sets the secondary unallocated job reason to Priority' do
        secondary_unallocated = expected_unallocated_jobs[1]
        scheduler.allocate_jobs
        expect(secondary_unallocated.reason_pending).to eq('Priority')
      end
    end

    context 'multiple calls' do
      context 'for non-array jobs' do
        let(:test_data) {
          [
            { job_id: 1, min_nodes: 1, run_time: 2, allocated_in_round: 1 },
            { job_id: 2, min_nodes: 1, run_time: 1, allocated_in_round: 1 },
            { job_id: 3, min_nodes: 3, run_time: 3, allocated_in_round: 2 },
            { job_id: 4, min_nodes: 2, run_time: 3, allocated_in_round: 5 },
            { job_id: 5, min_nodes: 1, run_time: 2, allocated_in_round: 5 },
            { job_id: 6, min_nodes: 2, run_time: 1, allocated_in_round: 7 },
          ]
        }

        before(:each) {
          test_data.each do |datum|
            job = make_job(datum[:job_id], datum[:min_nodes])
            # datum[:expected_allocation] = Allocation.new(job: job, nodes: datum[:nodes])
            job_registry.add(job)
          end
        }

        it 'allocates the correct nodes to the correct jobs in the correct order' do
          num_rounds = test_data.map { |d| d[:allocated_in_round] }.max
          num_rounds.times.each do |round|
            round += 1

            # Progress any completed jobs.
            allocations.each do |allocation|
              datum = test_data.detect { |d| d[:job_id] == allocation.job.id }
              datum[:run_time] -= 1
              if datum[:run_time] == 0
                allocation.nodes.dup.each do |node|
                  allocations.deallocate_node_from_job(allocation.job.id, node.name)
                end
                allocation.job.state = 'COMPLETED'
              end
            end

            expected_allocations = test_data
              .select { |d| d[:allocated_in_round] == round }

            expect { scheduler.allocate_jobs }.to \
              change { allocations.size }.by(expected_allocations.length)
            expected_allocations.each do |datum|
              allocation = allocations.for_job(datum[:job_id])
              expect(allocation).not_to be_nil
            end
          end
        end
      end

      context 'for array jobs' do
        class TestData < Struct.new(:job_id, :array, :min_nodes, :run_time, :allocations_in_round)
          def initialize(*args)
            super
            @run_time_for_tasks = {}
          end

          def reduce_remaining_runtime(allocation)
            @run_time_for_tasks[allocation] ||= run_time
            @run_time_for_tasks[allocation] -= 1
          end

          def completed_tasks
            @run_time_for_tasks.select { |key, value| value == 0 }.keys
          end
        end

        let(:test_data) {
          [
            #           Job id, array, min_nodes, run_time, allocations_in_round
            TestData.new(1,     '1-2', 1,         2,        { 1 => 2 }),
            TestData.new(2,     '1-2', 3,         3,        { 3 => 1, 6 => 1 }),
            TestData.new(3,     '1-2', 2,         3,        { 9 => 2 }),
            TestData.new(4,     '1-5', 1,         1,        { 12 => 4, 13 => 1 }),
            TestData.new(5,     '1-2', 3,         1,        { 13 => 1, 14 => 1 }),
            TestData.new(6,     '1-2', 2,         1,        { 15 => 2 }),
          ]
        }

        before(:each) {
          test_data.each do |datum|
            job = make_job(datum.job_id, datum.min_nodes, array: datum.array)
            job_registry.add(job)
          end
        }

        it 'allocates the correct nodes to the correct jobs in the correct order' do
          num_rounds = test_data.map { |d| d.allocations_in_round.keys }.flatten.max
          num_rounds.times.each do |round|
            round += 1

            # Progress any completed jobs.
            allocations.each do |allocation|
              datum = test_data.detect { |d| d.job_id == allocation.job.array_job.id }
              datum.reduce_remaining_runtime(allocation)
              datum.completed_tasks.each do |allocation|
                allocation.nodes.dup.each do |node|
                  allocations.deallocate_node_from_job(allocation.job.id, node.name)
                end
                allocation.job.state = 'COMPLETED'
              end
            end

            array_jobs_with_allocations_this_round = test_data
              .select { |d| d.allocations_in_round[round] }
            total_allocations_this_round = array_jobs_with_allocations_this_round
              .map { |d| d.allocations_in_round[round] }
              .sum

            new_allocations = scheduler.allocate_jobs
            expect(new_allocations.length).to eq total_allocations_this_round

            allocations_by_array_job = new_allocations.group_by do |allocation|
              allocation.job.array_job.id
            end

            array_jobs_with_allocations_this_round.each do |datum|
              allocations = allocations_by_array_job[datum.job_id]
              expect(allocations.length).to eq datum.allocations_in_round[round]
            end
          end
        end
      end
    end
  end

  describe '#queue' do
    context 'with a fresh sceduler' do
      it 'is empty' do
        expect(subject.queue).to be_empty
      end
    end

    context 'with multiple batch jobs' do
      let(:jobs) do
        (rand(10) + 1).times.map { build(:job, min_nodes: 1, partition: partition) }
      end

      before { jobs.each { |j| job_registry.add(j) } }

      it 'matches the jobs' do
        expect(subject.queue).to eq(jobs)
      end

      context 'after allocation' do
        before { subject.allocate_jobs }

        it 'matches the jobs' do
          expect(subject.queue).to eq(jobs)
        end
      end
    end

    context 'with a single array job with parity between nodes and tasks' do
      let(:job) do
        build(:job, array: "1-#{nodes.length}", partition: partition, min_nodes: 1)
      end

      before { job_registry.add(job) }

      it 'contains the single job' do
        expect(subject.queue).to contain_exactly(job)
      end

      context 'after allocation' do
        before { subject.allocate_jobs }

        it 'does not contain the main job' do
          expect(subject.queue).not_to include(job)
        end

        it 'does contain the tasks' do
          expect(subject.queue.length).to eq(nodes.length)
          expect(subject.queue.map(&:array_index)).to contain_exactly(*(1..nodes.length))
          subject.queue.each do |task|
            expect(task).to be_a(Job)
            expect(task.job_type).to eq('ARRAY_TASK')
            expect(task.array_job).to eq(job)
          end
        end
      end
    end

    context 'with a single array job with excess tasks' do
      let(:max_index) { nodes.length + 1 + rand(10) }
      let(:job) do
        build(:job, array: "1-#{max_index}", partition: partition, min_nodes: 1)
      end

      before { job_registry.add(job) }

      it 'contains the single job' do
        expect(subject.queue).to contain_exactly(job)
      end

      context 'after allocation' do
        before { subject.allocate_jobs }

        it 'contains the main job in the last position' do
          expect(subject.queue.last).to eq(job)
        end

        it 'contains tasks in the subsequent positions' do
          remaining = subject.queue.dup.tap(&:pop)
          expect(remaining.length).to eq(nodes.length)
          expect(remaining.map(&:array_index)).to contain_exactly(*(1..nodes.length))
          remaining.each do |task|
            expect(task).to be_a(Job)
            expect(task.job_type).to eq('ARRAY_TASK')
            expect(task.array_job).to eq(job)
          end
        end
      end
    end
  end

  describe 'job removal (completion or cancellation)' do
    context 'with multiple batch jobs' do
      let(:jobs) do
        (rand(10) + 1).times.map { build(:job, min_nodes: 1, partition: partition) }
      end
      let(:job) { jobs.sample }
      before { jobs.each { |j| job_registry.add(j) } }

      context 'after completing an allocated job' do
        before do
          subject.allocate_jobs
          job.state = 'COMPLETED'
        end

        it 'does not appear in the queue' do
          expect(subject.queue).not_to include(job)
        end
      end
    end

    context 'with a single allocated array job with an execess of tasks' do
      let(:max_index) { nodes.length + 1 + rand(10) }
      let(:job) do
        build(:job, array: "1-#{max_index}", partition: partition, min_nodes: 1)
      end

      before do
        job_registry.add(job)
        subject.allocate_jobs
      end

      context 'after completing and removing the allocated ARRAY_JOB' do
        before do
          job_registry.tasks_for(job).each do |task|
            task.state = 'COMPLETED'
            allocation = FlightScheduler.app.allocations.for_job(task)
            if allocation
              allocation.nodes.each do |node|
                FlightScheduler.app.allocations.deallocate_node_from_job(task.id, node.name)
              end
            end
          end
          job.state = 'COMPLETED'
          job_registry.remove_old_jobs
        end

        it 'does not appear in the queue' do
          expect(subject.queue).to be_empty
        end
      end

      context 'after removing an ARRAY_TASK' do
        let(:task) do
          subject.queue[0..-2].sample
        end
        let(:other_tasks) { subject.queue[0..-2] - [task] }

        before do
          expect(task.job_type).to eq('ARRAY_TASK')
          other_tasks # Ensure other_tasks is initialized
          task.state = 'COMPLETED'
        end

        it 'includes the main job and other tasks in the queue' do
          expect(subject.queue).to contain_exactly(job, *other_tasks)
        end

        it 'does not include the task in the queue' do
          expect(subject.queue).not_to include(task)
        end
      end

      context 'after completing all ARRAY_TASKs' do
        before do
          subject.queue.dup.each do |job|
            next unless job.job_type == 'ARRAY_TASK'
            job.state = 'COMPLETED'
          end
        end

        it 'only includes the main job in the queue' do
          expect(subject.queue).to contain_exactly(job)
        end
      end
    end

    context 'with a single allocated array job with parity between tasks and nodes' do
      let(:job) do
        build(:job, array: "1-#{nodes.length}", partition: partition, min_nodes: 1)
      end

      before do
        job_registry.add(job)
        subject.allocate_jobs
      end

      context 'after completing all the tasks' do
        before do
          subject.queue.dup.each do |job|
            next unless job.job_type == 'ARRAY_TASK'
            job.state = 'COMPLETED'
          end
        end

        it 'removes the main job' do
          expect(subject.queue).to be_empty
        end
      end
    end
  end
end
