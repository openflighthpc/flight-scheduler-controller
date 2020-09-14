#==============================================================================
# Copyright (C) 2020-present Alces Flight Ltd.
#
# This file is part of FlurmAPI.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License 2.0 which is available at
# <https://www.eclipse.org/legal/epl-2.0>, or alternative license
# terms made available by Alces Flight Ltd - please direct inquiries
# about licensing to licensing@alces-flight.com.
#
# FlurmAPI is distributed in the hope that it will be useful, but
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR
# IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR CONDITIONS
# OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A
# PARTICULAR PURPOSE. See the Eclipse Public License 2.0 for more
# details.
#
# You should have received a copy of the Eclipse Public License 2.0
# along with FlurmAPI. If not, see:
#
#  https://opensource.org/licenses/EPL-2.0
#
# For more information on FlurmAPI, please visit:
# https://github.com/openflighthpc/flurm-api
#==============================================================================

# Includes the Siantra::Base inheritance so it can be loaded before app.rb
class App < Sinatra::Base
  class Config < Hashie::Trash
    include Hashie::Extensions::IgnoreUndeclared
    include Hashie::Extensions::Dash::IndifferentAccess

    REFERENCE_PATH  = File.expand_path('../config/application.reference', __dir__)
    CONFIG_PATH     = File.expand_path('../config/application.yaml', __dir__)

    def self.load_reference(path)
      self.instance_eval(File.read(path), path, 0) if File.exists?(path)
    end

    def self.config(sym, **input_opts)
      opts = input_opts.dup

      # Make keys with defaults required by default
      opts[:required] = true if opts.key? :default && !opts.key?(:required)

      # Defines the underlining property
      property(sym, **opts)

      # Define the truthiness method
      # NOTE: Empty values are not considered truthy
      define_method(:"#{sym}?") do
        value = send(sym)
        if value.respond_to?(:empty?)
          !value.empty?
        else
          send(sym) ? true : false
        end
      end
    end

    # Loads the reference file
    load_reference REFERENCE_PATH

    config :development, default: ENV['RACK_ENV'] != 'production'
  end

  # Caches the config
  Config::CACHE = if File.exists? Config::CONFIG_PATH
    data = YAML.load(File.read(Config::CONFIG_PATH), symbolize_names: true)
    Config.new(data)
  else
    Config.new({})
  end
end
