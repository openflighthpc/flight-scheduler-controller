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

RSpec.describe Partition::Builder do
  # Guaranteed to be valid when called without arguments
  # NOTE: This means it can not set nil values, this needs to be done manually
  def generate_spec(name: nil, default: nil, max_time_limit: nil, default_time_limit: nil, nodes: nil, node_matchers: nil)
    name = SecureRandom.hex if name.nil?
    { "name" => name }.tap do |s|
      s["default"] = default unless default.nil?
      s['max_time_limit'] = max_time_limit unless max_time_limit.nil?
      s['default_time_limit'] = default_time_limit unless default_time_limit.nil?
      s['nodes'] = nodes unless nodes.nil?
      s['node_matchers'] = node_matchers unless node_matchers.nil?
    end
  end

  specify 'empty arrays are valid' do
    expect(described_class.new([])).to be_valid
  end

  specify 'empty hashes are not valid' do
    expect(described_class.new({})).not_to be_valid
  end

  specify 'a simple partition is valid' do
    specs = [generate_spec]
    expect(described_class.new(specs)).to be_valid
  end

  specify 'missing names are invalid' do
    specs = [generate_spec]
    specs.first.delete("name")
    expect(described_class.new(specs)).not_to be_valid
  end

  specify 'integer names are invalid' do
    specs = [generate_spec(name: 1)]
    expect(described_class.new(specs)).not_to be_valid
  end

  specify 'default may be a boolean' do
    specs = [generate_spec(default: true), generate_spec(default: false)]
    expect(described_class.new(specs)).to be_valid
  end

  specify 'integer defaults are invalid' do
    specs = [generate_spec(default: 1)]
    expect(described_class.new(specs)).not_to be_valid
  end

  [:max_time_limit, :default_time_limit].each do |key|
    specify "#{key} can be an integer or string" do
      specs = [
        generate_spec(key => 1),
        generate_spec(key => '1:1')
      ]
      expect(described_class.new(specs)).to be_valid
    end

    specify "object #{key} are invalid" do
      specs = [generate_spec(key => {})]
      expect(described_class.new(specs)).not_to be_valid
    end
  end

  specify 'nodes can be an array of strings' do
    specs = [generate_spec(nodes: ['foo', 'bar'])]
    expect(described_class.new(specs)).to be_valid
  end

  specify 'nodes can not be a string' do
    specs = [generate_spec(nodes: 'foobar')]
    expect(described_class.new(specs)).not_to be_valid
  end

  specify 'nodes can not be an array of integers' do
    specs = [generate_spec(nodes: [1,2,3])]
    expect(described_class.new(specs)).not_to be_valid
  end

  specify 'node_matchers can be an object of matchers' do
    specs = [
      generate_spec(node_matchers: {}),
      generate_spec(node_matchers: { 'cpus' => {} }),
      generate_spec(node_matchers: { 'name' => { 'regex' => '.*' } })
    ]
    expect(described_class.new(specs)).to be_valid
  end

  specify 'node_matchers can not be an array' do
    specs = [generate_spec(node_matchers: [])]
    expect(described_class.new(specs)).not_to be_valid
  end
end
