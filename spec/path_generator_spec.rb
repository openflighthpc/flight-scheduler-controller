#==============================================================================
# Copyright (C) 2021-present Alces Flight Ltd.
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

RSpec.describe FlightScheduler::PathGenerator do
  let(:all_chars) do
    described_class::ALL_CHARS
  end

  let(:job) { raise NotImplementedError }
  let(:task) { raise NotImplementedError }
  let(:node) { build(:node) }

  subject { raise NotImplementedError }

  shared_examples 'shared-attributes' do
    describe '#pct_A' do
      it 'returns the job ID' do
        expect(subject.pct_A).to eq(job.id)
      end
    end

    describe '#pct_N' do
      it 'returns the node name' do
        expect(subject.pct_N).to eq(node.name)
      end
    end

    describe '#pct_u' do
      it 'returns the username' do
        expect(subject.pct_u).to eq(job.username)
      end
    end

    describe '#pct_x' do
      it "returns the job's script name" do
        expect(subject.pct_x).to eq(job.batch_script.name)
      end
    end
  end

  shared_examples 'render-engine' do
    describe '#render' do
      alpha = (('a'..'z').to_a - described_class::ALL_CHARS).sample

      ['', '.', '&', rand(20).to_s, alpha, "#{rand(20)}#{alpha}"].each do |str|
        it "preserves %#{str}" do
          expect(subject.render("%#{str}")).to eq("%#{str}")
        end

        it "escapes %%#{str}" do
          expect(subject.render("%%#{str}")).to eq("%#{str}")
        end

        it "escapes %%%#{str}" do
          expect(subject.render("%%%#{str}")).to eq("%%#{str}")
        end
      end

      it 'does not render the basic characters' do
        all_chars.each do |char|
          expect(subject.render(char)).to eq(char)
        end
      end

      it 'replaces the percent delimted chars' do
        all_chars.each do |char|
          expect(subject.render("%#{char}")).to eq(subject.send("pct_#{char}").to_s)
        end
      end

      it 'escapes double percented chars' do
        all_chars.each do |char|
          expect(subject.render("%%#{char}")).to eq("%#{char}")
        end
      end

      it 'escapes and renders triple perecented chars' do
        all_chars.each do |char|
          expect(subject.render("%%%#{char}")).to eq('%' + subject.send("pct_#{char}").to_s)
        end
      end

      it 'can pad numeric chars the requested number of 0' do
        described_class::NUMERIC_KEYS.keys.each do |char|
          # Randomly set the lengths
          value_length = rand(5)
          pad_length = rand(4) + 1
          total_length = value_length + pad_length

          # Set the string values
          method_value = 'A' * value_length
          padding = '0' * pad_length
          allow(subject).to receive("pct_#{char}").and_return(method_value)

          # test the requested path
          path = "%#{total_length}#{char}"
          final_value = "#{padding}#{method_value}"
          expect(subject.render(path)).to eq(final_value)
        end
      end

      it 'ignores the padding directive for alpha chars' do
        described_class::ALPHA_KEYS.keys.each do |char|
          # Randomly set the lengths
          value_length = rand(5)

          # Set the string values
          method_value = 'A' * value_length
          allow(subject).to receive("pct_#{char}").and_return(method_value)

          # test the requested path
          path = "%#{value_length + rand(4) + 1}#{char}"
          expect(subject.render(path)).to eq(method_value)
        end
      end

      it 'escapes double percented and preserves the integer' do
        all_chars.each do |char|
          int = rand(5)
          path = "%%#{int}#{char}"
          expect(subject.render(path)).to eq("%#{int}#{char}")
        end
      end

      it 'escapes, renders, and pads tripple percented numeric chars' do
        described_class::NUMERIC_KEYS.keys.each do |char|
          # Randomly set the lengths
          value_length = rand(5)
          pad_length = rand(4) + 1
          total_length = value_length + pad_length

          # Set the string values
          method_value = 'A' * value_length
          padding = '0' * pad_length
          allow(subject).to receive("pct_#{char}").and_return(method_value)

          # test the requested path
          path = "%%%#{total_length}#{char}"
          final_value = "%#{padding}#{method_value}"
          expect(subject.render(path)).to eq(final_value)
        end
      end

      it 'escapes, and renders but ignores pads for tripple percented alpha chars' do
        described_class::ALPHA_KEYS.keys.each do |char|
          # Randomly set the lengths
          value_length = rand(5)

          # Set the string values
          method_value = 'A' * value_length
          allow(subject).to receive("pct_#{char}").and_return(method_value)

          # test the requested path
          path = "%%%#{value_length + rand(4) + 1}#{char}"
          expect(subject.render(path)).to eq('%' + method_value)
        end
      end

      it 'does not pad if the value is sufficiently long' do
        all_chars.each do |char|
          # Randomly set the lengths
          pad_length = rand(5)
          value_length = rand(5) + pad_length + 1

          # Set the string values
          value = 'A' * value_length
          allow(subject).to receive("pct_#{char}").and_return(value)

          # test the requested path
          expect(subject.render("%#{pad_length}#{char}")).to eq(value)
        end
      end
    end
  end

  context 'with a regular batch job' do
    let(:job) { build(:batch_script).job }
    subject { described_class.new(node: node, job: job) }

    include_examples 'shared-attributes'
    include_examples 'render-engine'

    describe '#pct_a' do
      it 'returns 0' do
        expect(subject.pct_a).to eq(0)
      end
    end

    describe '#pct_j' do
      it 'returns the ID' do
        expect(subject.pct_j).to eq(job.id)
      end
    end
  end

  context 'with an array job and task' do
    let(:job_max) { 20 }
    let(:job) {
      build(:job, array: "1-#{job_max}").tap do |job|
        job.batch_script = build(:batch_script, job: job)
        job.batch_script.job = job
      end
    }

    # NOTE The task is deliberately detached from the job. This allows the spec
    # to test various attributes are coming from the `job` instead of task.array_job
    #
    # This ensures PathGenerator is decoupled from the data model
    let(:task_max) { 10 }
    let(:task) do
      build(:job, array: "1-#{task_max}", num_started: rand(task_max - 1))
        .task_generator.next_task
    end

    subject { described_class.new(node: node, job: job, task: task) }

    include_examples 'shared-attributes'
    include_examples 'render-engine'

    describe '#pct_a' do
      it 'returns the task array index' do
        expect(subject.pct_a).to eq(task.array_index)
      end
    end

    describe '#pct_j' do
      it 'returns empty string' do
        expect(subject.pct_j).to eq(task.id)
      end
    end
  end
end

