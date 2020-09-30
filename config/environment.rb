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

FlightScheduler.app.configure do
  config.cluster_name = ENV.fetch('FLIGHT_SCHEDULER_CLUSTER_NAME', '')
  config.env_var_prefix = ENV.fetch('FLIGHT_SCHEDULER_ENV_VAR_PREFIX', '')
  config.job_dir = ENV.fetch('FLIGHT_SCHEDULER_JOB_DIR', root.join('../var/jobs/'))
  config.log_level = :debug

  config.partitions = [
    { name: 'standard', nodes: %w(node01 node02 node03 node04) },
    { name: 'gpu',      nodes: %w(gpu01 gpu02) },
    { name: 'all',      nodes: %w(node01 node02 node03 node04 gpu01 gpu02), default: true },
  ]
end
