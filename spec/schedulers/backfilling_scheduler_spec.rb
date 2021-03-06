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
require_relative '../../lib/flight_scheduler/schedulers/backfilling_scheduler'
require_relative 'shared_scheduler_spec'

RSpec.describe BackfillingScheduler, type: :scheduler do
  let(:partition) {
    build(:partition, name: 'all', nodes: nodes, max_time_limit_spec: 1000, default_time_limit_spec: 5)
  }
  let(:partitions) { [ partition ] }
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

    context 'backfilling' do
      include ActiveSupport::Testing::TimeHelpers

      before(:each) {
        # To aid test readability, the backfilling tests are written in terms
        # of "rounds" rather than time.
        #
        # This introduces an issue where jobs are not being backfilled because
        # they end a fraction of a second after a reservation is due to start.
        # This issue is fixed by fixing the time.
        travel_to Time.now
      }

      context 'non-array jobs on homogenous partition' do
        context 'simplest backfilling works' do
          include_examples 'allocation specs for non-array jobs'

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

          # There will be a non-allocated, non-reserved node available for job
          # 3 in round 1.
          let(:test_data) {
            TestData = NonArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, min_nodes: 2, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 2, min_nodes: 3, time_limit_spec: 3),
                allocated_in_round: 2,
              ),
              TestData.new(
                job: build_job(id: 3, min_nodes: 1, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
            ]
          }

          before(:each) {
            test_data.each do |datum|
              job_registry.add(datum.job)
            end
          }
        end

        context 'reserved nodes are available for short run jobs' do
          include_examples 'allocation specs for non-array jobs'

          let(:nodes) {
            [
              Node.new(name: 'node01'),
              Node.new(name: 'node02'),
              Node.new(name: 'node03'),
            ].tap do |a|
              a.each do |node|
                allow(node).to receive(:connected?).and_return true
              end
            end
          }

          # In round 1, there will a non-allocated, but reserved node
          # available to job 3.
          let(:test_data) {
            TestData = NonArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, min_nodes: 2, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 2, min_nodes: 3, time_limit_spec: 3),
                allocated_in_round: 2,
              ),
              TestData.new(
                job: build_job(id: 3, min_nodes: 1, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
            ]
          }

          before(:each) {
            test_data.each do |datum|
              job_registry.add(datum.job)
            end
          }
        end

        context 'backfilling does not stop at first backfill failure' do
          include_examples 'allocation specs for non-array jobs'

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

          # In round 1, job 3 cannot be backfilled as there are insufficient
          # nodes available to it.
          #
          # In round 1, job 4 can be backfilled as there are sufficient nodes
          # available to it.
          let(:test_data) {
            TestData = NonArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, min_nodes: 2, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 2, min_nodes: 3, time_limit_spec: 3),
                allocated_in_round: 2,
              ),
              TestData.new(
                job: build_job(id: 3, min_nodes: 3, time_limit_spec: 1),
                allocated_in_round: 5,
              ),
              TestData.new(
                job: build_job(id: 4, min_nodes: 1, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
            ]
          }

          before(:each) {
            test_data.each do |datum|
              job_registry.add(datum.job)
            end
          }
        end

        context 'backfilling respects reservations' do
          include_examples 'allocation specs for non-array jobs'

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

          # In round 1, job 3 cannot be allocated as it will not complete
          # before the reservation for job 2 is due.
          #
          # In round 1, job 4 can be allocated as it can run on a
          # non-allocated, non-reserved node.
          #
          # In round 1, job 5 can be allocated. It will run on a non-allocated
          # yet reserved node and will complete before the reservation is due
          # to start.
          let(:test_data) {
            TestData = NonArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, min_nodes: 2, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 2, min_nodes: 3, time_limit_spec: 3),
                allocated_in_round: 2,
              ),
              TestData.new(
                job: build_job(id: 3, min_nodes: 2, time_limit_spec: 2),
                allocated_in_round: 5,
              ),
              TestData.new(
                job: build_job(id: 4, min_nodes: 1, time_limit_spec: 2),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 5, min_nodes: 1, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
            ]
          }

          before(:each) {
            test_data.each do |datum|
              job_registry.add(datum.job)
            end
          }
        end

        context 'jobs requiring more resources than the partition has do not block backfilling' do
          include_examples 'allocation specs for non-array jobs'

          let(:nodes) {
            [
              Node.new(name: 'node01'),
              Node.new(name: 'node02'),
            ].tap do |a|
              a.each do |node|
                allow(node).to receive(:connected?).and_return true
              end
            end
          }

          # There will be a non-allocated, non-reserved node available for job
          # 3 in round 1.
          let(:test_data) {
            TestData = NonArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, min_nodes: 3, time_limit_spec: 1),
                allocated_in_round: nil,
              ),
              TestData.new(
                job: build_job(id: 2, min_nodes: 1, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 3, min_nodes: 2, time_limit_spec: 1),
                allocated_in_round: 2,
              ),
              TestData.new(
                job: build_job(id: 4, min_nodes: 1, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
            ]
          }

          before(:each) {
            test_data.each do |datum|
              job_registry.add(datum.job)
            end
          }
        end

        context 'jobs without a timelimit are not backfilled on reserved slots' do
          include_examples 'allocation specs for non-array jobs'

          let(:partition) {
            build(:partition, name: 'all', nodes: nodes)
          }

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

          # In round 1:
          # 
          # * job 3 will be backfilled as there is a non-reserved node
          # available.
          # * job 4 will not be backfilled as it has no time limit and the
          # only available node has a reservation.
          # * job 5 will be backfilled as it has a time limit and it will
          # complete before the reservation is due to start
          let(:test_data) {
            TestData = NonArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, min_nodes: 2, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 2, min_nodes: 3, time_limit_spec: 1),
                allocated_in_round: 2,
              ),
              TestData.new(
                job: build_job(id: 3, min_nodes: 1),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 4, min_nodes: 1),
                allocated_in_round: 2,
              ),
              TestData.new(
                job: build_job(id: 5, min_nodes: 1, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
            ]
          }

          before(:each) {
            test_data.each do |datum|
              job_registry.add(datum.job)
            end
          }
        end

        context 'resources allocated to jobs without a time limit are not available for reservation' do
          include_examples 'allocation specs for non-array jobs'

          let(:partition) {
            build(:partition, name: 'all', nodes: nodes)
          }

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

          # In round 1 three nodes are allocated.  Two of them are allocated
          # to Job 2, a job without a timelimit.
          #
          # These nodes are therefore not available to be used as part of a
          # reservation, preventing a reservation for Job 3 in round 1.
          #
          # Job 4 cannot be allocated (or backfilled) in round 1 as the
          # reservation for Job 3 could not be created.  If we allowed, Job 4
          # to be backfilled, it could potentially delay the highest priority
          # job, Job 3.
          let(:test_data) {
            TestData = NonArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, min_nodes: 1, time_limit_spec: 1),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 2, min_nodes: 2),
                allocated_in_round: 1,
              ),
              TestData.new(
                job: build_job(id: 3, min_nodes: 3, time_limit_spec: 1),
                allocated_in_round: 2,
              ),
              TestData.new(
                job: build_job(id: 4, min_nodes: 1, time_limit_spec: 2),
                allocated_in_round: 2,
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

      context 'array jobs on homogenous partition' do
        context 'simple backfilling works' do
          include_examples 'allocation specs for array jobs'

          let(:test_data) {
            TestData = ArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, array: '1-2', min_nodes: 1, time_limit_spec: 2),
                allocations_in_round: { 1 => 2 },
              ),
              TestData.new(
                job: build_job(id: 2, array: '1-2', min_nodes: 3, time_limit_spec: 2),
                allocations_in_round: { 3 => 1, 5 => 1 },
              ),
              TestData.new(
                job: build_job(id: 3, array: '1-2', min_nodes: 2, time_limit_spec: 1),
                allocations_in_round: { 1 => 1, 2 => 1 },
              ),
            ]
          }

          before(:each) {
            test_data.each do |datum|
              job_registry.add(datum.job)
            end
          }
        end

        context 'backfilling does not stop at first backfill failure' do
          include_examples 'allocation specs for array jobs'

          let(:test_data) {
            TestData = ArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, array: '1-2', min_nodes: 1, time_limit_spec: 2),
                allocations_in_round: { 1 => 2 },
              ),
              TestData.new(
                job: build_job(id: 2, array: '1-2', min_nodes: 3, time_limit_spec: 2),
                allocations_in_round: { 3 => 1, 5 => 1 },
              ),
              TestData.new(
                job: build_job(id: 3, array: '1-2', min_nodes: 3, time_limit_spec: 1),
                allocations_in_round: { 7 => 1, 8 => 1 },
              ),
              TestData.new(
                job: build_job(id: 4, array: '1-2', min_nodes: 2, time_limit_spec: 1),
                allocations_in_round: { 1 => 1, 2 => 1 },
              ),
            ]
          }

          before(:each) {
            test_data.each do |datum|
              job_registry.add(datum.job)
            end
          }
        end

        context 'backfilling respects reservations' do
          include_examples 'allocation specs for array jobs'

          let(:test_data) {
            TestData = ArrayTestData
            [
              TestData.new(
                job: build_job(id: 1, array: '1-2', min_nodes: 1, time_limit_spec: 1),
                allocations_in_round: { 1 => 2 },
              ),
              TestData.new(
                job: build_job(id: 2, array: '1-2', min_nodes: 3, time_limit_spec: 2),
                allocations_in_round: { 2 => 1, 4 => 1 },
              ),
              TestData.new(
                job: build_job(id: 3, array: '1-2', min_nodes: 2, time_limit_spec: 2),
                allocations_in_round: { 6 => 2 },
              ),
              TestData.new(
                job: build_job(id: 4, array: '1-2', min_nodes: 1, time_limit_spec: 1),
                allocations_in_round: { 1 => 2 },
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
end
