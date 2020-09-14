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

class Allocation
  # The job that this allocation has been made for.
  attr_reader :job

  # The nodes that have been allocated to this job.
  attr_reader :nodes

  def initialize(job:, nodes:)
    @job = job
    @nodes = nodes
  end

  def partition
    @job.partition
  end

  def ==(other)
    self.class == other.class &&
      job == other.job &&
      nodes == other.nodes
  end
  alias :eql? :==

  def hash
    ( [self.class, job.hash] + nodes.map(&:hash) ).hash
  end
end
