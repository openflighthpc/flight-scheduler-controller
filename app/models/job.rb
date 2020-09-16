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
require 'active_model'

class Job
  include ActiveModel::Model

  attr_accessor :arguments
  attr_accessor :id
  attr_accessor :partition
  attr_accessor :script
  attr_accessor :state
  attr_accessor :args

  # Handle the k and m suffix
  attr_reader :min_nodes

  def min_nodes=(raw)
    str = raw.to_s
    @min_nodes = if /\A\d+k\Z/.match?(str)
      str.sub('k', '').to_i * 1024
    elsif /\A\d+m\Z/.match(str)
      str.sub('m', '').to_i * 1048576
    elsif /\d+/.match?(str)
      str.to_i
    else
      # This will error during validation with an appropriate error message
      str
    end
  end

  validates :id, presence: true
  validates :min_nodes,
    presence: true,
    numericality: { allow_blank: false, only_integer: true, greater_than_or_equal_to: 1 }
  validates :script, presence: true
  validates :state,
    presence: true,
    inclusion: { within: %w( pending running cancelled completed failed ) }

  def allocation
    FlightScheduler.app.allocations.for_job(self.id)
  end

  def hash
    [self.class, id].hash
  end
end
