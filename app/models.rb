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

require 'securerandom'

class App
  class BaseModel
    include ActiveModel::Model
    include ActiveModel::Attributes
  end

  class Partition < BaseModel
    def self.load_all
      Config::CACHE.fifo_queues.map do |name|
        new(name: name)
      end
    end

    attribute :name
    attribute :nodes

    def jobs
      @jobs ||= []
    end
  end

  class Schedular < SimpleDelegator
    def initialize(*a, **opts)
      if a.first.is_a? Partition
        super(a.first)
      else
        super(Partition.new(*a, **opts))
      end
    end
  end

  class Job < BaseModel
    def id
      @id ||= SecureRandom.uuid
    end

    attribute :min_nodes
    attribute :schedular
    attribute :script

    def ensure_scheduled
      @ensure_scheduled ||= begin
        schedular.jobs << self
        true
      end
    end

    def clear
      schedular.jobs.delete(self)
    end
  end
end
