require 'spec_helper'

RSpec.describe Node, type: :model do
  subject { Node.new(name: 'node01') }
  let(:job) {
    Job.new(
      id: 1,
      min_nodes: 1,
      script_path: '/some/path',
      arguments: [],
    )
  }

  describe 'job satisfaction' do
    it 'satisfies a job if it is not allocated' do
      allow(subject).to receive(:allocation).and_return nil
      expect(subject.satisfies?(job)).to be true
    end

    it 'does not satisfy a job if it is allocated' do
      allow(subject).to receive(:allocation).and_return Object.new
      expect(subject.satisfies?(job)).to be false
    end
  end
end
