require 'spec_helper'
require_relative '../../app/schedulers/fifo_scheduler'

RSpec.describe Partition, type: :scheduler do
  let(:partition) { Partition.new(name: 'all', nodes: nodes) }
  let(:nodes) {
    [
      Node.new(name: 'node01'),
      Node.new(name: 'node02'),
      Node.new(name: 'node03'),
      Node.new(name: 'node04'),
    ]
  }

  describe '#allocate_jobs' do
    before(:each) { allocations.send(:clear); scheduler.send(:clear) }

    def make_job(job_id, min_nodes)
      Job.new(
        id: job_id,
        min_nodes: min_nodes,
        script_path: '/some/path',
        arguments: [],
        partition: partition,
      )
    end

    def add_allocation(job, nodes)
      Allocation.new(job: job, nodes: nodes).tap do |allocation|
        allocations.add(allocation)
      end
    end

    let(:scheduler) { FifoScheduler.new }
    let(:allocations) { AllocationSet.instance }

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
          add_allocation(job, nodes[0...1])
        end
      }

      it 'does not create any allocations' do
        expect{ scheduler.allocate_jobs }.not_to change { allocations.size }
      end
    end

    context 'when all unallocated jobs can be allocated' do
      before(:each) {
        2.times.each do |job_id|
          job = make_job(job_id, 1)
          scheduler.add_job(job)
        end
      }

      let(:unallocated_jobs) {
        scheduler.queue.select { |node| node.allocation.nil? }
      }

      it 'creates an allocation for each job' do
        expect{ scheduler.allocate_jobs }.to \
          change { allocations.size }.by(unallocated_jobs.size)
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

      it 'creates allocations for the preceding jobs' do
        expected_allocated_jobs = scheduler.queue[0...2]

        expect{ scheduler.allocate_jobs }.to \
          change { allocations.size }.by(expected_allocated_jobs.size)
        expected_allocated_jobs.each do |job|
          expect(allocations.for_job(job.id)).to be_truthy
        end
      end

      it 'does not create allocations for the following jobs' do
        expected_unallocated_jobs = scheduler.queue[2...]

        scheduler.allocate_jobs

        expected_unallocated_jobs.each do |job|
          expect(allocations.for_job(job.id)).to be_nil
        end
      end
    end
  end
end
