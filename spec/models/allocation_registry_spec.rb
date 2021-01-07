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

  let(:partition) { build(:partition, nodes: nodes) }
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

  shared_examples 'add does error' do
    describe '#add' do
      it 'raises AllocationConflict' do
        expect { subject.add(allocation) }.to raise_error(FlightScheduler::AllocationRegistry::AllocationConflict)
      end
    end
  end

  shared_examples 'add does not error' do
    describe '#add' do
      it 'does not error' do
        expect { subject.add(allocation) }.not_to raise_error
      end
    end
  end

  shared_examples 'all the cpus' do
    context 'with a job requesting all the cpus and excess gpus' do
      let(:job) {
        build(
          :job,
          cpus_per_node: node.cpus,
          gpus_per_node: ( node.gpus || 0 ) + 1
        )
      }

      describe '#max_parallel_per_node' do
        it 'returns 0' do
          expect(subject.max_parallel_per_node(job, node)).to eq(0)
        end
      end

      include_examples 'add does error'
    end
  end

  describe 'adding an allocation' do
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

  describe '#deallocate_node_from_job' do
    context 'with a single node allocation' do
      let(:job) { build(:job) }
      let(:node) { build(:node) }
      let(:allocation) do
        alloc = Allocation.new(job: job, nodes: [node])
        subject.add(alloc)
        # Retrieve the duplicate
        subject.for_job(job.id)
      end

      before do
        allocation # Ensure the allocation is generated
        subject.deallocate_node_from_job(job.id, node.name)
      end

      it 'removes the node allocation' do
        expect(subject.for_node(node.name)).to be_empty
      end

      it 'removes the job allocation' do
        expect(subject.for_job(job.id)).to be_nil
      end

      it 'removes the node from within the allocation' do
        expect(allocation.nodes).not_to include(node)
      end
    end

    context 'with a dual node allocation' do
      let(:job) { build(:job) }
      let(:nodes) { [build(:node), build(:node)] }
      let(:allocation) do
        alloc = Allocation.new(job: job, nodes: nodes)
        subject.add(alloc)
        # Retrieve the duplicate
        subject.for_job(job.id)
      end

      before do
        allocation
        subject.deallocate_node_from_job(job.id, nodes.first.name)
      end

      it 'removes the first node allocation' do
        expect(subject.for_node(nodes.first.name)).to be_empty
      end

      it 'removes the first node from the allocation' do
        expect(allocation.nodes).not_to include(nodes.first)
      end

      it 'does not remove second node allocation' do
        expect(subject.for_node(nodes.last.name)).to contain_exactly(allocation)
        expect(allocation.nodes).to contain_exactly(nodes.last)
      end

      it 'does not remove the job allocation' do
        expect(subject.for_job(job.id)).to eq(allocation)
      end
    end

    context 'when removing a missing job' do
      let(:other_job) { build(:job) }
      let(:node) { build(:node) }
      let(:other_allocation) do
        alloc = Allocation.new(job: other_job, nodes: [node])
        subject.add(alloc)
        # Retrieve the duplicate
        subject.for_job(other_job.id)
      end

      before do
        other_allocation
        subject.deallocate_node_from_job(build(:job).id, node.name)
      end

      it 'does not remove the node allocation' do
        expect(subject.for_node(node.name)).to contain_exactly(other_allocation)
        expect(other_allocation.nodes).to contain_exactly(node)
      end

      it 'does not remove the other job allocation' do
        expect(subject.for_job(other_job.id)).to eq(other_allocation)
      end
    end

    context 'when removing a missing node' do
      let(:job) { build(:job) }
      let(:other_node) { build(:node) }
      let(:other_allocation) do
        alloc = Allocation.new(job: job, nodes: [other_node])
        subject.add(alloc)
        # Retrieve the duplicate
        subject.for_job(job.id)
      end

      before do
        other_allocation
        subject.deallocate_node_from_job(job.id, build(:node).name)
      end

      it 'does not remove the other node allocation' do
        expect(subject.for_node(other_node.name)).to contain_exactly(other_allocation)
        expect(other_allocation.nodes).to contain_exactly(other_node)
      end

      it 'does not remove the other job allocation' do
        expect(subject.for_job(job.id)).to eq(other_allocation)
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

    specify 'invalid allocations raises an AllocationConflict' do
      allocation = Allocation.new(job: build(:job), nodes: [build(:node)])
      allow(allocation).to receive(:valid?).and_return(false)
      expect do
        subject.add(allocation)
      end.to raise_exception FlightScheduler::AllocationRegistry::AllocationConflict
    end
  end

  context 'with a dual cpu node' do
    let(:node) { build(:node, cpus: 2, gpus: 1) }
    let(:allocation) { Allocation.new(job: job, nodes: [node]) }

    include_examples 'all the cpus'

    context 'with a single cpu job' do
      # NOTE: Ignores the minimum node count
      let(:job) { build(:job, cpus_per_node: 1, min_nodes: 10) }

      describe '#max_parallel_per_node' do
        it 'returns 2' do
          expect(subject.max_parallel_per_node(job, node)).to eq(2)
        end
      end

      include_examples 'add does not error'
    end

    context 'with a single cpu exclusive job' do
      let(:job) { build(:job, cpus_per_node: 1, exclusive: true) }

      describe '#max_parallel_per_node' do
        it 'returns 2' do
          expect(subject.max_parallel_per_node(job, node)).to eq(2)
        end
      end

      include_examples 'add does not error'
    end

    context 'with a dual cpu job' do
      let(:job) { build(:job, cpus_per_node: 2) }

      describe '#max_parallel_per_node' do
        it 'returns 1' do
          expect(subject.max_parallel_per_node(job, node)).to eq(1)
        end
      end

      include_examples 'add does not error'
    end

    context 'with insufficent cpus' do
      let(:job) { build(:job, cpus_per_node: 3) }

      describe '#max_parallel_per_node' do
        it 'returns 0' do
          expect(subject.max_parallel_per_node(job, node)).to eq(0)
        end
      end

      include_examples 'add does error'
    end
  end

  context 'with a dual cpu node with one allocated cpu' do
    let(:node) { build(:node, cpus: 2) }
    let(:other_job) { build(:job, cpus_per_node: 1) }

    let(:allocation) { Allocation.new(job: job, nodes: [node]) }

    before do
      subject.add Allocation.new(job: other_job, nodes: [node])
    end

    include_examples 'all the cpus'

    context 'with a single cpu job' do
      let(:job) { build(:job, cpus_per_node: 1, min_nodes: 10) }

      describe '#max_parallel_per_node' do
        it 'returns 1' do
          expect(subject.max_parallel_per_node(job, node)).to eq(1)
        end
      end

      include_examples 'add does not error'
    end

    context 'with a single cpu exclusive job' do
      let(:job) { build(:job, cpus_per_node: 1, exclusive: 0) }

      describe '#max_parallel_per_node' do
        it 'returns 0' do
          expect(subject.max_parallel_per_node(job, node)).to eq(0)
        end
      end

      include_examples 'add does error'
    end

    context 'with a dual cpu job' do
      let(:job) { build(:job, cpus_per_node: 2) }

      describe '#max_parallel_per_node' do
        it 'returns 0' do
          expect(subject.max_parallel_per_node(job, node)).to eq(0)
        end
      end

      include_examples 'add does error'
    end

    describe '#max_parallel_per_node' do
      it 'does not return negative numbers' do
        job = build(:job, cpus_per_node: 3)
        expect(subject.max_parallel_per_node(job, node)).to eq(0)
      end
    end
  end

  context 'with a quad cpu node with two allocated cpus' do
    let(:node) { build(:node, cpus: 4) }
    let(:other_job) { build(:job, cpus_per_node: 2) }

    let(:allocation) { Allocation.new(job: job, nodes: [node]) }

    before do
      subject.add Allocation.new(job: other_job, nodes: [node])
    end

    include_examples 'all the cpus'

    context 'with a dual cpu exclusive job' do
      let(:job) { build(:job, cpus_per_node: 2, exclusive: 0) }

      describe '#max_parallel_per_node' do
        it 'returns 0' do
          expect(subject.max_parallel_per_node(job, node)).to eq(0)
        end
      end

      include_examples 'add does error'
    end

    context 'with a dual cpu job' do
      let(:job) { build(:job, cpus_per_node: 2) }

      describe '#max_parallel_per_node' do
        it 'returns 1' do
          expect(subject.max_parallel_per_node(job, node)).to eq(1)
        end
      end

      include_examples 'add does not error'
    end

    context 'with a dual cpu array job' do
      let(:array_job) { build(:job, cpus_per_node: 2, array: '1-3') }
      let(:job) { array_job.task_generator.next_task }

      describe '#max_parallel_per_node' do
        it 'returns 1' do
          expect(subject.max_parallel_per_node(job, node)).to eq(1)
        end
      end

      include_examples 'add does not error'
    end

    describe '#max_parallel_per_node' do
      it 'does not return negative numbers' do
        job = build(:job, cpus_per_node: 3)
        expect(subject.max_parallel_per_node(job, node)).to eq(0)
      end
    end
  end

  context 'with a dual cpu node with an exclusive single cpu job' do
    let(:node) { build(:node, cpus: 2) }
    let(:other_job) { build(:job, cpus_per_node: 1, exclusive: true) }
    let(:job) { build(:job, cpus_per_node: 1) }
    let(:allocation) { Allocation.new(job: job, nodes: [node]) }

    before do
      subject.add Allocation.new(job: other_job, nodes: [node])
    end

    describe '#max_parallel_per_node' do
      it 'returns 0 for a single cpu job' do
        expect(subject.max_parallel_per_node(job, node)).to eq(0)
      end
    end

    include_examples 'all the cpus'
    include_examples 'add does error'
  end
end
