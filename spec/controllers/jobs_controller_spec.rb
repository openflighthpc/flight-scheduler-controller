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

RSpec.describe '/jobs', type: :controller do
  include SpecApp

  describe 'POST - Create' do
    around(:each) do |e|
      FakeFS.with_fresh do
        I18n.load_path.each { |p| FakeFS::FileSystem.clone(p) }
        e.call
      end
    end

    context 'without an auth header' do
      before(:each) do
        post '/jobs', '{ "data" : { "type" : "jobs" } }'
      end

      it 'returns 403' do
        expect(last_response).to be_forbidden
      end
    end

    context 'with a bogus basic auth header' do
      before(:each) do
        header 'Authorization', 'Basic bogus'
        post '/jobs', '{ "data" : { "type" : "jobs" } }'
      end

      # Whilst this is obvious wrong to a human, the same can not be said for a
      # machine. Base64.decode64('bogus) resolves to "n\x88." which is currently
      # considered a "valid" username. This can't be reject without further
      # authentication (e.g. pam)
      xit 'returns 403' do
        expect(last_response).to be_forbidden
      end
    end

    context 'with an empty username field in auth header' do
      before(:each) do
        header 'Authorization', "Basic #{Base64.encode64(':')}"
        post '/jobs', '{ "data" : { "type" : "jobs" } }'
      end

      it 'returns 403' do
        expect(last_response).to be_forbidden
      end
    end

    context 'when the script is missing' do
      let(:payload) do
        {
          data: {
            type: 'jobs',
            attributes: {
              min_nodes: 1,
              arguments: [],
              script_name: 'something.sh'
            }
          }
        }
      end

      before(:each) do
        header 'Authorization', "Basic #{Base64.encode64('flight:')}"
        post '/jobs', payload.to_json
      end

      # TODO: Harden up the error type here
      it 'errors' do
        expect(last_response.status).to be  >= 400
        expect(last_response.status).to be < 500
      end
    end

    context 'with a valid request' do
      let(:script) do
        <<~BASH
          #!/bin/bash
          echo 'Start script'
          sleep 10
          echo 'Finished script'
        BASH
      end

      let(:payload) do
        {
          data: {
            type: 'jobs',
            attributes: {
              min_nodes: 1,
              arguments: [],
              script: script,
              script_name: 'something.sh'
            }
          }
        }
      end

      attr_reader :response_id, :response_job

      before(:each) do
        header 'Authorization', "Basic #{Base64.encode64('flight:')}"
        post '/jobs', payload.to_json

        @response_id = JSON.parse(last_response.body).fetch('data', {}).fetch('id', nil)
        @response_job = FlightScheduler.app.scheduler.queue.find { |j| j.id == response_id }
      end

      it 'returned 201 CREATE' do
        expect(last_response).to be_created
      end

      # Ensures the job can actually be located
      it 'creates the job object' do
        expect(response_job).to be_a ::Job
      end

      it 'writes the script to disk' do
        expect(File.read response_job.batch_script.send(:script_path)).to eq(script)
      end

      describe 'DELETE /{id}' do
        before do
          delete "/jobs/#{response_id}"
        end

        it 'returns 204 no-content' do
          expect(last_response.status).to be 204
        end
      end
    end

    context 'creating an array job' do
      let(:script) do
        <<~BASH
          #!/bin/bash
          echo 'Start script'
          sleep 10
          echo 'Finished script'
        BASH
      end

      let(:payload) do
        {
          data: {
            type: 'jobs',
            attributes: {
              arguments: [],
              array: '2,4,6',
              min_nodes: 1,
              script: script,
              script_name: 'something.sh'
            }
          }
        }
      end

      attr_reader :response_id, :response_job
      before(:each) do
        header 'Authorization', "Basic #{Base64.encode64('flight:')}"
        post '/jobs', payload.to_json

        @response_id = JSON.parse(last_response.body).fetch('data', {}).fetch('id', '').sub(/\[.*/, '')
        @response_job = FlightScheduler.app.scheduler.queue.find { |j| j.id == response_id }
      end

      it 'returned 201 CREATE' do
        expect(last_response).to be_created
      end

      it 'creates an ARRAY_JOB job' do
        expect(response_job).to be_a ::Job
        expect(response_job.job_type).to eq 'ARRAY_JOB'
      end
    end
  end
end

