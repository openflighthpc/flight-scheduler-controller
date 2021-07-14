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
