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
    def for_batch(node, job)
      allocated_node_names = job.allocation.nodes.map(&:name).join(',')
      {
        "#{prefix}CLUSTER_NAME"  => FlightScheduler::EventProcessor.cluster_name.to_s,
        "#{prefix}JOB_ID"        => job.id,
        "#{prefix}JOB_PARTITION" => job.partition.name,
        "#{prefix}JOB_NODES"     => job.allocation.nodes.length.to_s, # Must be a string
        "#{prefix}JOB_NODELIST"  => allocated_node_names,
        "#{prefix}NODENAME"      => node.name,
      }
    end
    module_function :for_batch

    def prefix
      FlightScheduler::EventProcessor.env_var_prefix
    end
    module_function :prefix
  end
end
