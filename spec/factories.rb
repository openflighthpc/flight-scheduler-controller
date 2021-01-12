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

FactoryBot.define do
  factory :partition do
    sequence(:name) { |n| "demo-partition#{n}" }
    nodes { [] }
    default { false }

    # XXX: The build method of the partition implicitly adds its nodes to the
    #      registry but does not cache the partition. The following should
    #      probably occur:
    #      1. build does not cache the partition/nodes. That is up to the user
    #      2. create does cache the partition/nodes
    initialize_with do
      attrs = attributes.dup
      nodes = attrs.delete(:nodes) || []
      nodes.each do |node|
        FlightScheduler.app.nodes.instance_variable_get(:@nodes)[node.name] = node
        FlightScheduler.app.nodes.instance_variable_set(:@partitions_cache, {})
      end
      raise NotImplementedError if attrs.key?(:static_node_names)
      new static_node_names: nodes.map(&:name), **attrs
    end
  end

  factory :job do
    id { SecureRandom.uuid }
    partition
    min_nodes { 1 }
    state { 'PENDING' }
    reason_pending { 'WaitingForScheduling' }
    array { nil }
    username { 'flight' }

    transient do
      num_started { nil }
      started_state { 'RUNNING' }
    end

    after(:build) do |job, evaluator|
      if evaluator.num_started
        evaluator.num_started.times do
          job.task_generator.next_task.state = evaluator.started_state
        end
      end
    end
  end

  factory :allocation do
    transient do
      nodes { [build(:node)] }
    end

    job
    node_names { nodes.map(&:name) }

    initialize_with { Allocation.new(**attributes) }
  end

  factory :batch_script do
    arguments { [] }
    content {
      <<~EOF
        #!/bin/bash
        echo "A batch script"
      EOF
    }
    env { {} }
    name { 'my-batch-script.sh' }

    association :job

    after(:build) do |script, evaluator|
      script.job.batch_script = script
    end
  end

  # XXX: Currently the build method permanently adds the nodes to the
  #      NodeRegistry. This should probably occur on create instead
  factory :node do
    sequence(:name) { |n| "demo#{n}" }
    cpus { 1 }
    memory { 1048576 }

    initialize_with do
      delegates = attributes.slice(*Node::NodeAttributes::DELEGATES)
      attributes = Node::NodeAttributes.new(**delegates)
      FlightScheduler.app.nodes.register_node(name).tap do |node|
        node.attributes = attributes
      end
    end
  end

  factory :node_matcher, class: 'FlightScheduler::NodeMatcher' do
    key { 'name' }
    initialize_with do
      attr = attributes.dup
      new(attr.delete(:key), **attr)
    end
  end
end
