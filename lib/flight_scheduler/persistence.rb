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

class FlightScheduler::Persistence
  def initialize(registry_name, filename)
    @registry_name = registry_name
    @path = File.join(FlightScheduler.app.config.spool_dir, filename)
    @old_path = File.join(FlightScheduler.app.config.spool_dir, "#{filename}.old")
  end

  def load
    unless File.exist?(@path) || File.exist?(@old_path)
      Async.logger.info("No saved data for #{@registry_name}")
      return nil 
    end
    failed = false
    path = failed ? @old_path : @path
    begin
      Async.logger.info("Loading #{@registry_name} from #{path}")
      data = JSON.load(File.open(path))
      Async.logger.debug("Loaded data") { data }
      data
    rescue
      if failed
        raise
      else
        failed = true
        retry
      end
    end
  rescue
    Async.logger.warn("Error loading #{@registry_name}: #{$!.message}")
    raise
  end

  def save(data)
    Async.logger.info("Saving #{@registry_name}")
    Async.logger.debug("Serializable data") { data }

    block = lambda do
      # We jump through some hoops to make writing the save state atomic and
      # consistent.
      #
      # 1. Create a copy of the original state, by creating a hard-link to it.
      # 2. Create a tempfile, being careful to make sure we create one that
      #    isn't automatically removed.
      # 3. Write the content to the tempfile.
      # 4. If all the content is written, move the tempfile to the correct
      #    path.
      FileUtils.mkdir_p(File.dirname(@path))
      if File.exist?(@path) && !FlightScheduler.env.test?
        FileUtils.cp_lr(@path, @old_path, remove_destination: true)
      end
      begin
        tmpfile = Tempfile.create(File.basename(@path), File.dirname(@path))
        content = data.to_json
        if tmpfile.write(content) == content.length
          FileUtils.mv(tmpfile.path, @path)
        end
      ensure
        tmpfile.close
      end
    end

    if Async::Task.current?
      Async::IO::Threads.new.async do
        block.call
      end.wait
    else
      block.call
    end
  rescue
    Async.logger.warn("Error saving #{@registry_name}: #{$!.message}")
    raise
  end
end