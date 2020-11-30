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

RSpec.describe FlightScheduler::AllocationRegistry, type: :model do
  subject { FlightScheduler::AllocationRegistry.new }

  it 'is initially empty' do
    # Jump through hoops to create a new AllocationRegistry so as to isolate from
    # the other specs without calling `clear`.
    expect(FlightScheduler::AllocationRegistry.new.send(:empty?)).to be true
  end

  let(:partition) { Partition.new(name: 'all', nodes: nodes) }
  let(:nodes) {
    [
      Node.new(name: 'node01'),
      Node.new(name: 'node02'),
      Node.new(name: 'node03'),
      Node.new(name: 'node04'),
    ]
  }

  def make_job(job_id, min_nodes)
    Job.new(
      id: job_id,
      min_nodes: min_nodes,
      partition: partition,
    )
  end

  def add_allocation(job, nodes, exclusive: true)
    Allocation.new(job: job, nodes: nodes).tap do |allocation|
      subject.add(allocation)
    end
  end


  describe 'adding an allocation' do
    before(:each) { subject.send(:clear) }

    specify 'allows retrieval by job id' do
      job = make_job(1, 2)
      allocation = add_allocation(job, nodes[0...2])
      expect(subject.for_job(job.id)).to eq allocation
    end

    specify 'does not allow retrieval by another job id' do
      job = make_job(1, 2)
      add_allocation(job, nodes[0...2])
      expect(subject.for_job(2)).to eq nil
    end

    specify 'allows retrieval by node name' do
      job = make_job(1, 2)
      allocation = add_allocation(job, nodes[0...2])
      nodes[0...2].each do |node|
        expect(subject.for_node(node.name)).to eq [allocation]
      end
    end

    specify 'returns empty array for  missing node names' do
      job = make_job(1, 2)
      add_allocation(job, nodes[0...2])
      ( nodes - nodes[0...2] ).each do |node|
        expect(subject.for_node(node.name)).to eq []
      end
    end
  end

  describe 'deleting an allocation' do
    before(:each) { subject.send(:clear) }

    specify 'prevents retrieval by job id' do
      job = make_job(1, 2)
      allocation = add_allocation(job, nodes[0...2])
      expect(subject.for_job(job.id)).to eq allocation
      subject.delete(allocation)
      expect(subject.for_job(job.id)).to eq nil
    end

    specify 'removes the node name entry' do
      job = make_job(1, 2)
      allocation = add_allocation(job, nodes[0...2])
      nodes[0...2].each do |node|
        expect(subject.for_node(node.name)).to eq [allocation]
      end
      subject.delete(allocation)
      nodes[0...2].each do |node|
        expect(subject.for_node(node.name)).to eq []
      end
    end
  end

  describe 'allocation conflict' do
    specify 'allocating an already allocated node raises an AllocationConflict' do
      job1 = make_job(1, 1)
      job2 = make_job(2, 1)
      add_allocation(job1, nodes[0...1])

      expect { add_allocation(job2, nodes[0...1]) }.to raise_exception \
        FlightScheduler::AllocationRegistry::AllocationConflict
    end

    specify 'allocating an already allocated job raises an AllocationConflict' do
      job = make_job(1, 1)
      add_allocation(job, nodes[0...1])

      expect { add_allocation(job, nodes[1...2]) }.to raise_exception \
        FlightScheduler::AllocationRegistry::AllocationConflict
    end
  end

  describe 'max_parallel_per_node' do
    context 'with a dual cpu node' do
      let(:node) { build(:node, cpus: 2) }

      it 'returns 2 for a single cpu job' do
        # NOTE: Ignores the minimum node count
        job = build(:job, cpus_per_node: 1, min_nodes: 10)
        expect(subject.max_parallel_per_node(job, node)).to eq(2)
      end

      it 'returns 1 for a dual cpu job' do
        job = build(:job, cpus_per_node: 2)
        expect(subject.max_parallel_per_node(job, node)).to eq(1)
      end

      it 'returns 0 for insufficient cpus' do
        job = build(:job, cpus_per_node: 3)
        expect(subject.max_parallel_per_node(job, node)).to eq(0)
      end
    end

    context 'with a dual cpu node with one allocated cpu' do
      let(:node) { build(:node, cpus: 2) }
      let(:other_job) { build(:job, cpus_per_node: 1) }

      before do
        subject.add Allocation.new(job: other_job, nodes: [node])
      end

      it 'returns 1 for a single cpu job' do
        job = build(:job, cpus_per_node: 1, min_nodes: 10)
        expect(subject.max_parallel_per_node(job, node)).to eq(1)
      end

      it 'returns 0 for a dual cpu job' do
        job = build(:job, cpus_per_node: 2)
        expect(subject.max_parallel_per_node(job, node)).to eq(0)
      end

      it 'does not return negative numbers' do
        job = build(:job, cpus_per_node: 3)
        expect(subject.max_parallel_per_node(job, node)).to eq(0)
      end
    end
  end
end
