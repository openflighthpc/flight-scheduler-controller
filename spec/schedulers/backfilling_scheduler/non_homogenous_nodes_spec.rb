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
  describe '#allocate_jobs' do
    context 'non-homogenous nodes' do
      let(:partition) {
        build(:partition, name: 'all', nodes: nodes, max_time_limit_spec: 10, default_time_limit_spec: 5)
      }
      let(:partitions) { [ partition ] }
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

      let(:scheduler) { subject }
      let(:allocations) { FlightScheduler.app.allocations }
      let(:job_registry) { FlightScheduler.app.job_registry }
      subject { described_class.new }

      before(:each) { allocations.send(:clear); job_registry.send(:clear) }

      def build_job(**kwargs)
        build(:job, partition: partition, **kwargs)
      end

      context 'backfilling with array jobs' do
        include_examples 'allocation specs for array jobs'

        let(:test_data) {
          TestData = ArrayTestData
          [
            TestData.new(
              job: build_job(id: 1, array: '1-2', min_nodes: 2),
              run_time: 2,
              allocations_in_round: { 1 => 2 },
            ),
            TestData.new(
              job: build_job(id: 2, array: '1-2', min_nodes: 2),
              run_time: 3,
              allocations_in_round: { 3 => 2 },
            ),
            TestData.new(
              job: build_job(id: 3, array: '1-4', min_nodes: 1),
              run_time: 1,
              allocations_in_round: { 6 => 4 },
            ),
            TestData.new(
              job: build_job(id: 4, array: '1-20', min_nodes: 1),
              run_time: 1,
              allocations_in_round: { 7 => 4, 8 => 4, 9 => 4, 10 => 4, 11 => 4},
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
