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
  describe '#allocate_jobs (common)' do
    def make_job(job_id, min_nodes, **kwargs)
      build(:job, id: job_id, min_nodes: min_nodes, partition: partition, **kwargs)
    end

    def add_allocation(job, nodes)
      build(:allocation, job: job, nodes: nodes).tap do |allocation|
        allocations.add(allocation)
      end
    end

    context 'with the initial empty scheduler' do
      it 'does not create any allocations' do
        expect(scheduler.queue).to be_empty
        expect{
          scheduler.allocate_jobs(partitions: partitions)
        }.not_to change { allocations.size }
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
        expect{
          scheduler.allocate_jobs(partitions: partitions)
        }.not_to change { allocations.size }
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
        expect{
          scheduler.allocate_jobs(partitions: partitions)
        }.to change { allocations.size }.by(number_jobs)
      end
    end
  end
end

RSpec.shared_examples '(basic) #queue specs' do
  describe '#queue (common)' do
    context 'with a fresh sceduler' do
      it 'is empty' do
        expect(subject.queue).to be_empty
      end
    end

    context 'with multiple batch jobs' do
      let(:jobs) do
        (rand(10) + 1).times.map { build(:job, min_nodes: 1, partition: partition) }
      end

      before { jobs.each { |j| job_registry.add(j) } }

      it 'matches the jobs' do
        expect(subject.queue).to eq(jobs)
      end

      context 'after allocation' do
        before { subject.allocate_jobs(partitions: partitions) }

        it 'matches the jobs' do
          expect(subject.queue).to eq(jobs)
        end
      end
    end

    context 'with a single array job with parity between nodes and tasks' do
      let(:job) do
        build(:job, array: "1-#{nodes.length}", partition: partition, min_nodes: 1)
      end

      before { job_registry.add(job) }

      it 'contains the single job' do
        expect(subject.queue).to contain_exactly(job)
      end

      context 'after allocation' do
        before { subject.allocate_jobs(partitions: partitions) }

        it 'does not contain the main job' do
          expect(subject.queue).not_to include(job)
        end

        it 'does contain the tasks' do
          expect(subject.queue.length).to eq(nodes.length)
          expect(subject.queue.map(&:array_index)).to contain_exactly(*(1..nodes.length))
          subject.queue.each do |task|
            expect(task).to be_a(Job)
            expect(task.job_type).to eq('ARRAY_TASK')
            expect(task.array_job).to eq(job)
          end
        end
      end
    end

    context 'with a single array job with excess tasks' do
      let(:max_index) { nodes.length + 1 + rand(10) }
      let(:job) do
        build(:job, array: "1-#{max_index}", partition: partition, min_nodes: 1)
      end

      before { job_registry.add(job) }

      it 'contains the single job' do
        expect(subject.queue).to contain_exactly(job)
      end

      context 'after allocation' do
        before { subject.allocate_jobs(partitions: partitions) }

        it 'contains the main job in the last position' do
          expect(subject.queue.last).to eq(job)
        end

        it 'contains tasks in the subsequent positions' do
          remaining = subject.queue.dup.tap(&:pop)
          expect(remaining.length).to eq(nodes.length)
          expect(remaining.map(&:array_index)).to contain_exactly(*(1..nodes.length))
          remaining.each do |task|
            expect(task).to be_a(Job)
            expect(task.job_type).to eq('ARRAY_TASK')
            expect(task.array_job).to eq(job)
          end
        end
      end
    end
  end
end

RSpec.shared_examples '(basic) job completion or cancellation specs' do
  describe 'job removal (completion or cancellation) (common)' do
    context 'with multiple batch jobs' do
      let(:jobs) do
        (rand(10) + 1).times.map { build(:job, min_nodes: 1, partition: partition) }
      end
      let(:job) { jobs.sample }
      before { jobs.each { |j| job_registry.add(j) } }

      context 'after completing an allocated job' do
        before do
          subject.allocate_jobs(partitions: partitions)
          job.state = 'COMPLETED'
        end

        it 'does not appear in the queue' do
          expect(subject.queue).not_to include(job)
        end
      end
    end

    context 'with a single allocated array job with an execess of tasks' do
      let(:max_index) { nodes.length + 1 + rand(10) }
      let(:job) do
        build(:job, array: "1-#{max_index}", partition: partition, min_nodes: 1)
      end

      before do
        job_registry.add(job)
        subject.allocate_jobs(partitions: partitions)
      end

      context 'after completing and removing the allocated ARRAY_JOB' do
        before do
          job_registry.tasks_for(job).each do |task|
            task.state = 'COMPLETED'
            allocation = FlightScheduler.app.allocations.for_job(task)
            if allocation
              allocation.nodes.each do |node|
                FlightScheduler.app.allocations.deallocate_node_from_job(task.id, node.name)
              end
            end
          end
          job.state = 'COMPLETED'
          job_registry.remove_old_jobs
        end

        it 'does not appear in the queue' do
          expect(subject.queue).to be_empty
        end
      end

      context 'after removing an ARRAY_TASK' do
        let(:task) do
          subject.queue[0..-2].sample
        end
        let(:other_tasks) { subject.queue[0..-2] - [task] }

        before do
          expect(task.job_type).to eq('ARRAY_TASK')
          other_tasks # Ensure other_tasks is initialized
          task.state = 'COMPLETED'
        end

        it 'includes the main job and other tasks in the queue' do
          expect(subject.queue).to contain_exactly(job, *other_tasks)
        end

        it 'does not include the task in the queue' do
          expect(subject.queue).not_to include(task)
        end
      end

      context 'after completing all ARRAY_TASKs' do
        before do
          subject.queue.dup.each do |job|
            next unless job.job_type == 'ARRAY_TASK'
            job.state = 'COMPLETED'
          end
        end

        it 'only includes the main job in the queue' do
          expect(subject.queue).to contain_exactly(job)
        end
      end
    end

    context 'with a single allocated array job with parity between tasks and nodes' do
      let(:job) do
        build(:job, array: "1-#{nodes.length}", partition: partition, min_nodes: 1)
      end

      before do
        job_registry.add(job)
        subject.allocate_jobs(partitions: partitions)
      end

      context 'after completing all the tasks' do
        before do
          subject.queue.dup.each do |job|
            next unless job.job_type == 'ARRAY_TASK'
            job.state = 'COMPLETED'
          end
        end

        it 'removes the main job' do
          expect(subject.queue).to be_empty
        end
      end
    end
  end
end
