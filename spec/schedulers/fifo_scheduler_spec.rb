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

RSpec.describe Partition, type: :scheduler do
  let(:partition) { Partition.new(name: 'all', nodes: nodes) }
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

  describe '#allocate_jobs' do
    before(:each) { allocations.send(:clear); scheduler.send(:clear) }

    def make_job(job_id, min_nodes)
      build(:job, id: job_id, min_nodes: min_nodes, partition: partition)
    end

    def add_allocation(job, nodes)
      Allocation.new(job: job, nodes: nodes).tap do |allocation|
        allocations.add(allocation)
      end
    end

    let(:scheduler) { FifoScheduler.new }
    let(:allocations) { FlightScheduler.app.allocations }

    context 'when queue is empty' do
      before(:each) { expect(scheduler.queue).to be_empty }

      it 'does not create any allocations' do
        expect{ scheduler.allocate_jobs }.not_to change { allocations.size }
      end
    end

    context 'when all jobs are already allocated' do
      before(:each) {
        2.times.each do |job_id|
          job = make_job(job_id, 1)
          scheduler.add_job(job)
          add_allocation(job, [nodes[job_id]])
        end
      }

      it 'does not create any allocations' do
        expect{ scheduler.allocate_jobs }.not_to change { allocations.size }
      end
    end

    context 'when all unallocated jobs can be allocated' do
      let(:number_jobs) { 2 }

      before(:each) {
        number_jobs.times.each do |job_id|
          job = make_job(job_id, 1)
          scheduler.add_job(job)
        end
      }

      it 'creates an allocation for each job' do
        expect{ scheduler.allocate_jobs }.to change { allocations.size }.by(number_jobs)
      end
    end

    context 'when there is an unallocated job that cannot be allocated' do
      before(:each) {
        [
          # Add two jobs both requiring a single node.
          [1, 1],
          [2, 1],

          # Add a job requiring three nodes.  The partition currently has only 2
          # nodes available.
          [3, 3],

          # Add two jobs both requiring a single node.  The partition has
          # sufficient nodes available for these jobs, but they are blocked by
          # the proceeding one.
          [4, 1],
          [5, 1],
        ].each do |job_id, min_nodes|
          job = make_job(job_id, min_nodes)
          scheduler.add_job(job)
        end
      }

      xit 'creates allocations for the preceding jobs' do
        expected_allocated_jobs = scheduler.queue[0...2]

        expect{ scheduler.allocate_jobs }.to \
          change { allocations.size }.by(expected_allocated_jobs.size)
        expected_allocated_jobs.each do |job|
          expect(allocations.for_job(job.id)).to be_truthy
        end
      end

      xit 'does not create allocations for the following jobs' do
        expected_unallocated_jobs = scheduler.queue[2...]
        scheduler.allocate_jobs

        expected_unallocated_jobs.each do |job|
          expect(allocations.for_job(job.id)).to be_nil
        end
      end

      xit 'sets the first unallocated job reason to Resources' do
        first_unallocated = scheduler.queue[2]
        scheduler.allocate_jobs
        expect(first_unallocated.reason_pending).to eq('Resources')
      end

      xit 'sets the secondary unallocated job reason to Priority' do
        secondary_unallocated = scheduler.queue[3]
        scheduler.allocate_jobs
        expect(secondary_unallocated.reason_pending).to eq('Priority')
      end
    end

    context 'multiple calls' do
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
          scheduler.add_job(job)
        end
      }

      xit 'allocates the correct nodes to the correct jobs in the correct order' do
        num_rounds = test_data.map { |d| d[:allocated_in_round] }.max
        num_rounds.times.each do |round|
          round += 1

          # Remove any completed jobs.
          allocations.each do |allocation|
            datum = test_data.detect { |d| d[:job_id] == allocation.job.id }
            datum[:run_time] -= 1
            if datum[:run_time] == 0
              allocations.delete(allocation)
              scheduler.remove_job(allocation.job)
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
  end
end
