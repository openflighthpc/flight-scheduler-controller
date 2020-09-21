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
      arguments: [],
      partition: partition,
    )
  end

  def add_allocation(job, nodes)
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
        expect(subject.for_node(node.name)).to eq allocation
      end
    end

    specify 'does not allow retrieval by other node names' do
      job = make_job(1, 2)
      add_allocation(job, nodes[0...2])
      ( nodes - nodes[0...2] ).each do |node|
        expect(subject.for_node(node.name)).to eq nil
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

    specify 'prevents retrieval by node name' do
      job = make_job(1, 2)
      allocation = add_allocation(job, nodes[0...2])
      nodes[0...2].each do |node|
        expect(subject.for_node(node.name)).to eq allocation
      end
      subject.delete(allocation)
      nodes[0...2].each do |node|
        expect(subject.for_node(node.name)).to eq nil
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
end
