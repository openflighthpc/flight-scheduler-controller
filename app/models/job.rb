#==============================================================================
# Copyright (C) 2020-present Alces Flight Ltd.
#
# This file is part of FlurmAPI.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License 2.0 which is available at
# <https://www.eclipse.org/legal/epl-2.0>, or alternative license
# terms made available by Alces Flight Ltd - please direct inquiries
# about licensing to licensing@alces-flight.com.
#
# FlurmAPI is distributed in the hope that it will be useful, but
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR
# IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR CONDITIONS
# OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A
# PARTICULAR PURPOSE. See the Eclipse Public License 2.0 for more
# details.
#
# You should have received a copy of the Eclipse Public License 2.0
# along with FlurmAPI. If not, see:
#
#  https://opensource.org/licenses/EPL-2.0
#
# For more information on FlurmAPI, please visit:
# https://github.com/openflighthpc/flurm-api
#==============================================================================
require 'active_model'

class Job
  include ActiveModel::Model
  include ActiveModel::Attributes

  attr_accessor :id,
    :min_nodes,
    :script_path,
    :arguments

  # The partition that this job has been allocated to.
  attr_accessor :partition

  validates :id, presence: true
  validates :min_nodes,
    presence: true,
    numericality: { allow_blank: false, only_integer: true, greater_than_or_equal_to: 1 }
  validates :script_path, presence: true

  def allocation
    AllocationSet.instance.for_job(self.id)
  end

  def hash
    [self.class, id].hash
  end
end
