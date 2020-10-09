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
    # TODO: Add a description to all the env vars
    # NOTE: Procs need to be used over lambdas as they are implicitly variadic
    BATCH_ENV_VARS = {
      'CLUSTER_NAME'  => {
        swagger: {  enum: [FlightScheduler.app.config.cluster_name.to_s] },
        block: proc { FlightScheduler.app.config.cluster_name.to_s }
      },
      'JOB_ID'        => { block: proc { |_, j| j.id } },
      'JOB_NAME'      => { block: proc { |_, j| j.batch_script&.name } },
      # TODO: Correctly set the env when --ntasks is implemented
      'JOB_NTASKS'    => { block: proc { 1 } },
      'JOB_PARTITION' => { block: proc { |_, j| j.partition.name } },
      'JOB_NUM_NODES' => {
        swagger: { pattern: '^\d+$', description: 'The total number of nodes assigned to the job' },
        block: proc { |_, _, a| a.nodes.length }
      },
      'JOB_NODELIST'  => {
        swagger: { format: 'csv', description: 'The node names as a comma spearated list' },
        block: proc { |_, _, a| a.nodes.map(&:name).join(',') }
      },
      'NODENAME'      => { block: proc { |n| n.name } },
      'CPUS_ON_NODE'  => { block: proc { 1 } },
      'JOB_CPUS_ON_NODE'  => { block: proc { 1 } },
      'NTASKS'  => { block: proc { 1 } },
      'HET_SIZE'  => { block: proc { 1 } },
      'TASKS_PER_NODE'  => { block: proc { 1 } },
    }

    ARRAY_ENV_VARS = {
      'ARRAY_JOB_ID'      => { block: proc { |j| j.id } },
      'ARRAY_TASK_ID'     => { block: proc { |_, t| t.array_index } },
      'ARRAY_TASK_COUNT'  => { block: proc { |_j, _t, indices| indices.length } },
      'ARRAY_TASK_MAX'    => { block: proc { |_j, _t, indices| indices.max } },
      'ARRAY_TASK_MIN'    => { block: proc { |_j, _t, indices| indices.min } }
    }

    def self.prefix_key(key)
      "#{FlightScheduler.app.config.env_var_prefix}#{key}"
    end

    def self.for_batch(node, job, allocation: nil)
      allocation ||= job.allocation
      BATCH_ENV_VARS.map do |key, block:, **_|
        [prefix_key(key), block.call(node, job, allocation).to_s]
      end.to_h
    end

    def self.for_array_task(node, array_job, array_task)
      base_env = EnvGenerator.for_batch(node, array_job, allocation: array_task.allocation)
      task_indexes = array_job.array_range.expanded

      array_env = ARRAY_ENV_VARS.map do |key, block:, **_|
        [prefix_key(key), block.call(array_job, array_task, task_indexes).to_s]
      end.to_h

      base_env.merge(array_env)
    end
  end
end
