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
    def for_batch(node, job, allocated_nodes: nil)
      allocated_nodes ||= job.allocation.nodes.map(&:name)
      {
        "#{prefix}CLUSTER_NAME"  => FlightScheduler::EventProcessor.cluster_name.to_s,
        "#{prefix}JOB_ID"        => job.id,
        "#{prefix}JOB_PARTITION" => job.partition.name,
        "#{prefix}JOB_NODES"     => allocated_nodes.length.to_s, # Must be a string
        "#{prefix}JOB_NODELIST"  => allocated_nodes.join(','),
        "#{prefix}NODENAME"      => node.name,
      }
    end
    module_function :for_batch

    def for_array_task(node, array_job, array_task)
      nodes = array_job.task_registry.running_tasks.map do |task|
        task.allocation.nodes.map(&:name)
      end.flatten
      base_env = EnvGenerator.for_batch(node, array_task, allocated_nodes: nodes)
      task_indexes = array_job.array_tasks.map(&:array_index).map(&:to_i)
      base_env.merge({
        "#{prefix}ARRAY_JOB_ID"     => array_job.id,
        "#{prefix}ARRAY_TASK_ID"    => array_task.array_index.to_s,
        "#{prefix}ARRAY_TASK_COUNT" => task_indexes.length.to_s,
        "#{prefix}ARRAY_TASK_MAX"   => task_indexes.max.to_s,
        "#{prefix}ARRAY_TASK_MIN"   => task_indexes.min.to_s,
      })
    end
    module_function :for_array_task

    def prefix
      FlightScheduler::EventProcessor.env_var_prefix
    end
    module_function :prefix
  end
end
