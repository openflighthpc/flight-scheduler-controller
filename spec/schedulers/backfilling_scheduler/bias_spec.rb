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
require_relative '../../../lib/flight_scheduler/schedulers/backfilling_scheduler'
require_relative '../shared_scheduler_spec'

RSpec.describe BackfillingScheduler, type: :scheduler do
  describe 'backfilling bias' do
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

    let(:partition) {
      build(:partition, name: 'all', nodes: nodes, max_time_limit_spec: 10, default_time_limit_spec: 5)
    }
    let(:partitions) { [ partition ] }
    let(:scheduler) { subject }
    let(:allocations) { FlightScheduler.app.allocations }
    let(:job_registry) { FlightScheduler.app.job_registry }
    subject { described_class.new }

    before(:each) { allocations.send(:clear); job_registry.send(:clear) }

    def build_job(**kwargs)
      build(:job, partition: partition, **kwargs)
    end

    context 'reservations are biased towards allocated nodes' do
      before(:each) {
        test_data.each do |datum|
          job_registry.add(datum.job)
        end
      }

      context 'when the nodes are fully allocated' do
        let(:nodes) {
          [
            build(:node, name: 'node01', cpus: 1),
            build(:node, name: 'node02', cpus: 1),
            build(:node, name: 'node03', cpus: 1),
            build(:node, name: 'node04', cpus: 1),
          ].tap do |a|
            a.each do |node|
              allow(node).to receive(:connected?).and_return true
            end
          end
        }

        # In round 1 a reservation for job 3 will be created.  It will be
        # created to run once job 2 has completed.  This allows job 4 to be
        # backfilled in round 1.
        let(:test_data) {
          TestData = NonArrayTestData
          [
            TestData.new(
              job: build_job(id: 1, min_nodes: 1, time_limit_spec: 1),
              allocated_in_round: 1,
            ),
            TestData.new(
              job: build_job(id: 2, min_nodes: 1, time_limit_spec: 2),
              allocated_in_round: 1,
            ),
            TestData.new(
              job: build_job(id: 3, min_nodes: 3, time_limit_spec: 1),
              allocated_in_round: 3,
            ),
            TestData.new(
              job: build_job(id: 4, min_nodes: 2, time_limit_spec: 2),
              allocated_in_round: 1,
            ),
          ]
        }

        include_examples 'allocation specs for non-array jobs'
      end

      context 'when the nodes are not fully allocated' do
        let(:nodes) {
          [
            build(:node, name: 'node01', cpus: 3),
          ].tap do |a|
            a.each do |node|
              allow(node).to receive(:connected?).and_return true
            end
          end
        }

        # In round 1 a reservation for job 3 will be created.  It will be
        # created to run once job 2 has completed.  This allows job 4 to be
        # backfilled in round 1.
        let(:test_data) {
          TestData = NonArrayTestData
          [
            TestData.new(
              job: build_job(id: 1, min_nodes: 1, cpus_per_node: 1, time_limit_spec: 1),
              allocated_in_round: 1,
            ),
            TestData.new(
              job: build_job(id: 2, min_nodes: 1, cpus_per_node: 1, time_limit_spec: 2),
              allocated_in_round: 1,
            ),
            TestData.new(
              job: build_job(id: 3, min_nodes: 1, cpus_per_node: 2, time_limit_spec: 1),
              allocated_in_round: 2,
            ),
            TestData.new(
              job: build_job(id: 4, min_nodes: 1, cpus_per_node: 1, time_limit_spec: 2),
              allocated_in_round: 3,
            ),
          ]
        }

        include_examples 'allocation specs for non-array jobs'
      end
    end
  end
end
