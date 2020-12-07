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

RSpec.shared_examples 'basic scheduler specs' do
  describe 'basic #allocate_jobs' do
    def make_job(job_id, min_nodes, **kwargs)
      build(:job, id: job_id, min_nodes: min_nodes, partition: partition, **kwargs)
    end

    def add_allocation(job, nodes)
      Allocation.new(job: job, nodes: nodes).tap do |allocation|
        allocations.add(allocation)
      end
    end

    context 'with the initial empty scheduler' do
      it 'does not create any allocations' do
        expect(scheduler.queue).to be_empty
        expect{ scheduler.allocate_jobs }.not_to change { allocations.size }
      end
    end

    context 'when all jobs are already allocated' do
      before(:each) {
        2.times.each do |job_id|
          job = make_job(job_id, 1)
          job_registry.add(job)
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
          job_registry.add(job)
        end
      }

      it 'creates an allocation for each job' do
        expect{ scheduler.allocate_jobs }.to change { allocations.size }.by(number_jobs)
      end
    end
  end
end
