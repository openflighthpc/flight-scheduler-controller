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

# Patch sinja and falcon so that sinja can correctly determine if there is a
# request body present.  See https://github.com/mwpastore/sinja/issues/19 for
# some details.

module SinjaContentPatch
  def content?
    return request.body.size > 0 if request.body.respond_to?(:size)
    return !request.body.empty? if request.body.respond_to?(:empty?)
    request.body.rewind
    request.body.read(1)
  end
end

module FalconAdaptersInputPatch
  def empty?
    return true if @body.nil?
    @body.respond_to?(:empty?) ? @body.empty? : @body.size
  end
end
require 'falcon'
Falcon::Adapters::Input.include FalconAdaptersInputPatch
