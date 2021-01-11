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

RSpec.describe Allocation, type: :model do
  let(:default_job) { build(:job) }
  let(:default_nodes) { [build(:node), build(:node)] }

  def build_allocation(**opts)
    described_class.new(
      job: opts.fetch(:job) { default_job },
      nodes: opts.fetch(:nodes) { default_nodes }
    )
  end

  context 'with valid inputs' do
    subject { build(:allocation) }

    it { should be_valid }
  end

  context 'without a node_names input' do
    subject { build(:allocation, node_names: nil) }

    it { should_not be_valid }
  end

  context 'without a job input' do
    subject { build(:allocation, job: nil) }

    it { should_not be_valid }
  end

  context 'with an invalid job' do
    subject do
      job = build(:job)
      allow(job).to receive(:valid?).and_return(false)
      build(:allocation, job: job)
    end

    it { should_not be_valid }
  end

  context 'with an empty node_names input' do
    subject { build(:allocation, node_names: []) }

    it { should_not be_valid }
  end

  context 'with a missing node_name' do
    subject { build(:allocation, node_names: ['missing-foobar']) }

    it { should_not be_valid }
  end
end
