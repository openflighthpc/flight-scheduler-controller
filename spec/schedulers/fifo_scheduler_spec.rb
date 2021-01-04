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
  let(:partitions) { [ partition ] }
  let(:partition) do
    build(:partition, name: 'all', nodes: nodes, max_time_limit_spec: 10, default_time_limit_spec: 5)
  end
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

  include_examples 'common scheduler specs'

  describe '#allocate_jobs' do
    def build_job(**kwargs)
      build(:job, partition: partition, **kwargs)
    end

    def add_allocation(job, nodes)
      build(:allocation, job: job, nodes: nodes).tap do |allocation|
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
          build_job(id: job_id, min_nodes: min_nodes)
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
        expect{ scheduler.allocate_jobs(partitions: partitions) }.to \
          change { allocations.size }.by(expected_allocated_jobs.length)
        expected_allocated_jobs.each do |job|
          expect(allocations.for_job(job.id)).to be_truthy
        end
      end

      it 'does not create allocations for the following jobs' do
        scheduler.allocate_jobs(partitions: partitions)

        expected_unallocated_jobs.each do |job|
          expect(allocations.for_job(job.id)).to be_nil
        end
      end

      it 'sets the first unallocated job reason to Resources' do
        first_unallocated = expected_unallocated_jobs.first
        scheduler.allocate_jobs(partitions: partitions)
        expect(first_unallocated.reason_pending).to eq('Resources')
      end

      it 'sets the secondary unallocated job reason to Priority' do
        secondary_unallocated = expected_unallocated_jobs[1]
        scheduler.allocate_jobs(partitions: partitions)
        expect(secondary_unallocated.reason_pending).to eq('Priority')
      end
    end

    context 'multiple calls' do
      context 'for non-array jobs' do
        include_examples 'allocation specs for non-array jobs'

        let(:test_data) {
          TestData = NonArrayTestData
          [
            TestData.new(
              job: build_job(id: 1, min_nodes: 1, time_limit_spec: 2),
              allocated_in_round: 1,
            ),
            TestData.new(
              job: build_job(id: 2, min_nodes: 1, time_limit_spec: 1),
              allocated_in_round: 1,
            ),
            TestData.new(
              job: build_job(id: 3, min_nodes: 3, time_limit_spec: 3),
              allocated_in_round: 2,
            ),
            TestData.new(
              job: build_job(id: 4, min_nodes: 2, time_limit_spec: 3),
              allocated_in_round: 5,
            ),
            TestData.new(
              job: build_job(id: 5, min_nodes: 1, time_limit_spec: 2),
              allocated_in_round: 5,
            ),
            TestData.new(
              job: build_job(id: 6, min_nodes: 2, time_limit_spec: 1),
              allocated_in_round: 7,
            ),
          ]
        }

        before(:each) {
          test_data.each do |datum|
            job_registry.add(datum.job)
          end
        }
      end

      context 'for array jobs' do
        include_examples 'allocation specs for array jobs'

        let(:test_data) {
        TestData = ArrayTestData
          [
            TestData.new(
              job: build_job(id: 1, array: '1-2', min_nodes: 1, time_limit_spec: 2),
              allocations_in_round: { 1 => 2 },
            ),
            TestData.new(
              job: build_job(id: 2, array: '1-2', min_nodes: 3, time_limit_spec: 3),
              allocations_in_round: { 3 => 1, 6 => 1 },
            ),
            TestData.new(
              job: build_job(id: 3, array: '1-2', min_nodes: 2, time_limit_spec: 3),
              allocations_in_round: { 9 => 2 },
            ),
            TestData.new(
              job: build_job(id: 4, array: '1-5', min_nodes: 1, time_limit_spec: 1),
              allocations_in_round: { 12 => 4, 13 => 1 },
            ),
            TestData.new(
              job: build_job(id: 5, array: '1-2', min_nodes: 3, time_limit_spec: 1),
              allocations_in_round: { 13 => 1, 14 => 1 },
            ),
            TestData.new(
              job: build_job(id: 6, array: '1-2', min_nodes: 2, time_limit_spec: 1),
              allocations_in_round: { 15 => 2 },
            ),
          ]
        }

        before(:each) {
          test_data.each do |datum|
            job_registry.add(datum.job)
          end
        }
      end
    end
  end
end
