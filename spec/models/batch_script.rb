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

RSpec.describe BatchScript, type: :model do
  it 'is valid' do
    expect(build(:batch_script)).to be_valid
  end

  describe '#stdout_path' do
    it 'has a default' do
      # XXX
      expect(build(:batch_script).stdout_path).to eq(described_class::DEFAULT_PATH)
    end

    it 'uses the default instead of empty string' do
      expect(build(:batch_script, stdout_path: '').stderr_path).to eq(described_class::DEFAULT_PATH)
    end

    it 'toggles the default for array jobs' do
      expect(build(:batch_script, array: '1-2').stdout_path).to eq(described_class::ARRAY_DEFAULT_PATH)
    end

    it 'can be overridden' do
      path = 'some-new-path'
      expect(build(:batch_script, stdout_path: path).stdout_path).to eq(path)
    end
  end

  describe '#stderr_path' do
    it 'has a default' do
      expect(build(:batch_script).stderr_path).to eq(described_class::DEFAULT_PATH)
    end

    it 'uses the default instead of empty string' do
      expect(build(:batch_script, stderr_path: '').stderr_path).to eq(described_class::DEFAULT_PATH)
    end

    it 'can be overridden' do
      out = 'some-incorrect-path'
      path = 'some-new-path'
      expect(build(:batch_script, stdout_path: out, stderr_path: path).stderr_path).to eq(path)
    end

    it 'can default to stdout_path' do
      path = 'some-new-path'
      expect(build(:batch_script, stdout_path: path).stderr_path).to eq(path)
    end
  end
end
