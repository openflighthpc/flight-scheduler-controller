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

# Generate the environment for running a job
module FlightScheduler::Submission
  module EnvGenerator
    def self.prefix
      FlightScheduler::EventProcessor.env_var_prefix
    end

    def self.add_env(group, raw_name, **swagger_opts, &block)
      name = "#{prefix}#{raw_name}"
      envs[group][name] = block
      swagger_envs[group][name] = swagger_opts
    end

    def self.envs
      @envs ||= Hash.new { |h, group| h[group] = {} }
    end

    ##
    # Used to configure the swagger docs with the environment variables
    # This helps reduce the documentation burden
    def self.swagger_envs
      @swagger_opts ||= Hash.new { |h, group| h[group] = {} }
    end

    # Define the batch env vars
    # TODO: Add a description to all the env vars
    add_env(:batch, 'CLUSTER_NAME',
            enum: [FlightScheduler::EventProcessor.cluster_name.to_s]) do |*_|
      FlightScheduler::EventProcessor.cluster_name.to_s
    end
    add_env(:batch, 'JOB_ID') { |_, j| j.id }
    add_env(:batch, 'JOB_NAME') { |_, j| j.script_name }
    # TODO: Correctly set the env when --ntasks is implemented
    add_env(:batch, 'JOB_NTASKS') { 1 }
    add_env(:batch, 'JOB_PARTITION') { |_, j| j.partition.name }
    add_env(:batch, 'JOB_NUM_NODES', pattern: '^\d+$',
            description: 'The total number of nodes assigned to the job') do |_, job|
      job.allocation.nodes.length
    end
    add_env(:batch, 'JOB_NODELIST', format: 'csv',
            description: 'The node names as a comma spearated list') do |_, job|
      job.allocation.nodes.map(&:name).join(',')
    end
    add_env(:batch, 'NODENAME') { |n| n.name }

    # Define the array env vars
    # TODO: Add a description to all the env vars
    add_env(:array, 'ARRAY_JOB_ID') { |j| j.id }
    add_env(:array, 'ARRAY_TASK_ID') { |_, t| t.array_index }
    add_env(:array, 'ARRAY_TASK_COUNT') { |_j, _t, indices| indices.length }
    add_env(:array, 'ARRAY_TASK_MAX') { |_j, _t, indices| indices.max }
    add_env(:array, 'ARRAY_TASK_min') { |_j, _t, indices| indices.min }

    def self.for_batch(node, job)
      envs[:batch].map { |k, block| [k, block.call(node, job).to_s] }.to_h
    end

    def self.for_array_task(node, array_job, array_task)
      base_env = EnvGenerator.for_batch(node, array_job)
      task_indexes = array_job.array_tasks.map(&:array_index).map(&:to_i)

      array_env = envs[:array].map do |key, block|
        [key, block.call(array_job, array_task, task_indexes).to_s]
      end.to_h

      base_env.merge(array_env)
    end
  end
end
