#==============================================================================
# Copyright (C) 2021-present Alces Flight Ltd.
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

# Plugin providing persistence of the scheduler's state.
#
# Currently this plugin synchronously saves the state in response to every
# event which might change the state.  A less naive plugin might save the
# state asynchronously or have some mechanism to determine if the state needs
# changing.
#
# It is expected that alternate plugins might be developed which save/load the
# state somewhere other than a text file, e.g., a MySQL database.
class SchedulerState
  def self.plugin_name
    'scheduler_state/filetxt'
  end

  class EventProcessor < Struct.new(:scheduler_state)
    def job_created(job)
      scheduler_state.save
    end

    def job_cancelled(*args)
      scheduler_state.save
    end

    def jobd_connected(*args)
      scheduler_state.save
    end

    def job_step_started(*args)
      scheduler_state.save
    end

    def job_step_completed(*args)
      scheduler_state.save
    end

    def job_step_failed(*args)
      scheduler_state.save
    end

    def resources_allocated(*args)
      scheduler_state.save
    end

    def resource_deallocated(*args)
      scheduler_state.save
    end
  end

  def initialize
    @jobs = FlightScheduler.app.job_registry
    @allocations = FlightScheduler.app.allocations
    @lock = @jobs.lock = @allocations.lock = Concurrent::ReadWriteLock.new
  end

  def event_processor
    @event_processor ||= EventProcessor.new(self)
  end

  def load
    data = persistence.load
    return if data.nil?
    @jobs.load(data['jobs'])
    @allocations.load(data['allocations'])
  end

  def save
    @lock.with_read_lock do
      data = {
        'allocations' => @allocations.serializable_data,
        'jobs'        => @jobs.serializable_data
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
