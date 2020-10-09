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

require "active_support/string_inquirer"

module FlightScheduler
  autoload(:AllocationRegistry, 'flight_scheduler/allocation_registry')
  autoload(:Application, 'flight_scheduler/application')
  autoload(:Auth, 'flight_scheduler/auth')
  autoload(:Configuration, 'flight_scheduler/configuration')
  autoload(:DaemonConnections, 'flight_scheduler/daemon_connections')
  autoload(:EventProcessor, 'flight_scheduler/event_processor')
  autoload(:PathGenerator, 'flight_scheduler/path_generator')
  autoload(:RangeExpander, 'flight_scheduler/range_expander')
  autoload(:Schedulers, 'flight_scheduler/schedulers')
  autoload(:TaskRegistry, 'flight_scheduler/task_registry')

  module Cancellation
    autoload(:ArrayJob, 'flight_scheduler/cancellation/array_job')
    autoload(:BatchJob, 'flight_scheduler/cancellation/batch_job')
  end

  module Deallocation
    autoload(:Job, 'flight_scheduler/deallocation/job')
  end

  module Submission
    autoload(:ArrayTask, 'flight_scheduler/submission/array_task')
    autoload(:BatchJob, 'flight_scheduler/submission/batch_job')
    autoload(:EnvGenerator, 'flight_scheduler/submission/env_generator')
    autoload(:JobStep, 'flight_scheduler/submission/job_step')
  end

  def app
    @app ||= Application.new(
      allocations: AllocationRegistry.new,
      daemon_connections: DaemonConnections.new,
      schedulers: Schedulers.new,
    )
  end
  module_function :app

  def env
    @env ||= ActiveSupport::StringInquirer.new(
      ENV["RACK_ENV"].presence || "development"
    )
  end
  module_function :env

  def env=(environment)
    @env = ActiveSupport::StringInquirer.new(environment)
  end
  module_function :env=
end
