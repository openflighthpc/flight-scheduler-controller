require 'spec_helper'

RSpec.describe Partition, type: :model do
  let(:job) {
    Job.new(
      id: 1,
      min_nodes: 2,
      script_path: '/some/path',
      arguments: [],
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
