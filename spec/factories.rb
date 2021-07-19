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
    default { false }
    node_matchers_spec do
      if nodes.nil?
        nil
      else
        # Handling for defining partitions with a nodes list
        nodes.each do |n|
          FlightScheduler.app.nodes.instance_variable_get(:@nodes)[n.name] = n
        end
        FlightScheduler.app.nodes.instance_variable_set(:@partitions_cache, {})
        { 'name' => { 'list' => nodes.map(&:name) } }
      end
    end

    # Syntactic shortcut for defining partitions with predefined nodes
    # NOTE: Can not be used with a custom node_matchers_spec
    # NOTE: This will cause the nodes to be added to NodeRegistry as a side
    #       effect of build. This is find as it is intentionally a shortcut 
    #       The partition still does not appear in the partitions list
    transient do
      nodes { nil }
    end

    initialize_with { new(**attributes) }
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
      FlightScheduler.app.nodes.register_node(name).tap do |node|
        delegates = attributes.slice(*node.attributes.class::DELEGATES)
        node.attributes = node.attributes.class.new(**delegates)
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
