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

# A well documented dummy plugin which does nothing.
#
# Each plugin file must define a single class.  When the class is loaded, it
# will be evaled into an anonymous module, so it does not matter what that
# class is called.
class Dummy

  # A plugin must provide a class method `plugin_name`.  It is the only
  # mandatory method.
  #
  # The plugin name has the format `TYPE/VARIETY`.
  #
  # With the exception of `core` there can be only a single plugin of each
  # TYPE.
  #
  # So here we have a `demo/dummy` plugin which does nothing.  We could
  # instead have, say a, `demo/job_counter` plugin which, say, keeps a count
  # the number of jobs created, but we couldn't have both loaded at the same
  # time.
  #
  # This TYPE uniqueness can be used to implement varient strategies for the
  # same basic functionality.  For instance, the `scheduler_state/filetxt` and
  # the `scheduler_state/mysql` plugins would both save and load the scheduler
  # state, but would use different mechanisms to do so.
  def self.plugin_name
    'demo/dummy'
  end

  # A plugin may optionally provide an `init` method.  It will be called once.
  #
  # It could be used to start timers or augment core classes.
  def self.init
  end

  # Optionally define an EventProcessor class, if the plugin is to respond to
  # life cycle events.
  #
  # The methods that can be defined along with their parameters can be found
  # in lib/flight_scheduler/event_processor.rb.  All methods are optional.
  class EventProcessor
    def job_created(job)
      # Do something with the job.
    end
  end

  # Setup the EventProcessor.
  def event_processor
    @event_processor ||= EventProcessor.new
  end

  # Export an API for the system and other plugins to call.
  def dummy
  end
end

