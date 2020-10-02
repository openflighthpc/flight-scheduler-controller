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

require 'spec_helper'

RSpec.describe Partition, type: :model do
  let(:job) {
    Job.new(
      id: 1,
      min_nodes: '2',
    )
  }

  describe '#available_nodes_for' do
    def make_node(name, satisfies)
      Node.new(name: name).tap do |node|
        allow(node).to receive(:satisfies?).and_return satisfies
      end
    end

    it 'returns nil if there are no available resources' do
      partition = Partition.new(name: 'all', nodes: [])
      expect(partition.available_nodes_for(job)).to be_nil
    end

    it 'returns nil if available resources are insufficient' do
      nodes = [
        make_node('node01', true),
        make_node('node02', false),
      ]
      partition = Partition.new(name: 'all', nodes: nodes)
      expect(partition.available_nodes_for(job)).to be_nil
    end

    it 'returns sufficient resources if they exist' do
      nodes = [
        make_node('node01', true),
        make_node('node02', true),
      ]
      partition = Partition.new(name: 'all', nodes: nodes)
      expect(partition.available_nodes_for(job)).to eq nodes
    end

    it 'does not return excessive resources' do
      sufficient_nodes = [
        make_node('node01', true),
        make_node('node02', true),
      ]
      extra_nodes = [
        make_node('node03', true),
      ]
      nodes = sufficient_nodes + extra_nodes
      partition = Partition.new(name: 'all', nodes: nodes)
      expect(partition.available_nodes_for(job)).to eq sufficient_nodes
    end
  end
end
