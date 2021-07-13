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
  class SchedulerState
    attr_reader :jobs, :allocations

    def initialize
      @lock = Concurrent::ReadWriteLock.new
      @jobs = JobRegistry.new(lock: @lock)
      @allocations = AllocationRegistry.new(lock: @lock)
    end

    def load
      data = persistence.load
      return if data.nil?
      jobs.load(data['jobs'])
      allocations.load(data['allocations'])
    end

    def save
      @lock.with_read_lock do
        data = {
          'allocations' => allocations.serializable_data,
          'jobs'        => jobs.serializable_data
        }
        persistence.save(data)
      end
    end

    private

    def persistence
      @persistence ||= FlightScheduler::Persistence.new(
        'scheduler state',
        'scheduler_state',
      )
    end
  end
end
