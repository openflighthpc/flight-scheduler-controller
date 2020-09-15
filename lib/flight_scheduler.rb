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

module FlightScheduler
  autoload(:AllocationRegistry, 'flight_scheduler/allocation_registry')
  autoload(:Application, 'flight_scheduler/application')
  autoload(:Schedulers, 'flight_scheduler/schedulers')

  def app
    standard_nodes = %w(node01 node02 node03 node04).map { |name| Node.new(name: name) }
    gpu_nodes = %w(gpu01 gpu02).map { |name| Node.new(name: name) }
    partitions = [
      Partition.new(name: 'standard', nodes: standard_nodes),
      Partition.new(name: 'gpu', nodes: gpu_nodes),
      Partition.new(name: 'all', nodes: standard_nodes + gpu_nodes, default: true),
    ]

    @app ||= Application.new(
      allocations: AllocationRegistry.new,
      partitions: partitions,
      schedulers: Schedulers.new,
    )
  end
  module_function :app

  def add_lib_to_load_path
    root = File.expand_path(File.dirname(File.dirname(__FILE__)))
    lib = File.join(root, 'lib')
    $LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
  end
  module_function :add_lib_to_load_path
end
