#!/bin/bash
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

# Standard Input
# NOTE: The input will be condensed into a single line. The following uses
#       multiple lines for clarity
#
# {
#   "partition":  "STRING",           # The name of the partition
#   "action":     "STRING",           # The action being ran
#   "jobs": {                         # All the jobs on the partition
#     "<id>": {                       # The ID of the job as the key
#       "id": "STRING",               # The Id of the job
#       "min_nodes": INTEGER,         # The minimum number of nodes for the job
#       "cpus_per_node": INTEGER,     # The required cpus per node
#       "gpus_per_node": INTEGER,     # The required gpus per node
#       "memory_per_node": INTEGER,   # The required memory per node
#       "state": "STRING",            # The state of the job
#       "reason": "STRING"            # The reason the job is pending
#     },
#     ...
#   }
#   "nodes": {                        # All the nodes on the partition
#     "<name>": {                     # The name of the node as the key
#       "type": "STRING",             # The node's type
#       "state": "STRING",            # The node's state
#       "jobs": ["STRING", ...],        # Job IDs within the partition that are running on the node
#       "other_jobs": ["STRING", ...],  # Job IDs on other partitions that are running on the node
#       "cpus": INTEGER,              # The number of cpus
#       "gpus": INTEGER,              # The number of gpus
#       "memory": INTEGER             # The amount of memory
#     }
#   },
#   "types": {
#     <name>: {                       # The name of the node type as the key
#       "name": "STRING",             # The name of the node type
#       "nodes": ["STRING", ...],     # The names of the nodes with the type
#       "count": INTEGER,             # The number of "active" nodes
#       "known_count": INTEGER,       # The number of all known nodes including down
#       "maximum": INTEGER | NULL,    # The recommended maximum of this type
#       "minimum": INTEGER | NULL,    # The recommended minimum of this type
#       "recognized": BOOLEAN,        # True iff the type is defined for the partition
#       "oversubscribed": BOOLEAN| NULL,  # True if the count exceeds the maximum
#       "undersubscribed": BOOLEAN| NULL, # True if the count is below the minimum
#     }
#   }
# }

# Ensure jq is on the path and all commands exit correctly
set -e
which "jq" >/dev/null

# Load the .slackrc file which is within the working directory. By default this will be
# the directory containing this file, however this may change depending on configuration
rc="./slackrc"
if [[ -f "$rc" ]]; then
  source "$rc"
else
  echo "Could not locate the rc file: $rc" >&2
  exit 1
fi

# Ensure the required environment variables have been set
missing=
if [ -z "$SLACK_TOKEN" ]; then
    missing="SLACK_TOKEN"
elif [ -z "$SLACK_CHANNEL" ]; then
    missing="SLACK_CHANNEL"
fi
if [ -n "$missing" ]; then
    cat 1>&2 <<ERROR
Error: Could not complete the request due to an internal configuration error ($missing).
Please contact your system administrator for further assistance.
ERROR
    exit 1
fi

# Unpacks the JSON input contained within STDIN
json=$(cat <&0)
running=$(echo "$json" | jq '.jobs | with_entries(select(.value | (.state == "RUNNING") or (.state == "CONFIGURING")))')
pending=$(echo "$json" | jq '.jobs | with_entries(select(.value | .state == "PENDING"))')
resources=$(echo "$json" | jq '.jobs | with_entries(select(.value | .reason == "Resources"))')

# Define the template for creating the JSON payload that is sent to slack
# NOTE: This is intentionally a string literal, jq will be used to preform the
#       parameter substitution
# PS: read successfully exits non-zero
read -r -d '' template <<'TEMPLATE' || true
{
  channel: ($channel),
  as_user: true,
  text: ("Scheduler Update (`\(.partition)`): `\(.action)` nodes"),
  attachments: [
    {
      pretext: "Summary of jobs which are blocked due to insufficient `resources`:",
      fields: [
        { short: true, title: "*Number of Jobs*", "value": ($resources | length) },
        { short: true, title: "*Max Requested Nodes per Job*", "value": ([$resources[].min_nodes] | max) },
        { short: true, title: "*Max Requested CPUs per Node*", "value": ([$resources[].cpus_per_node] | max) },
        { short: true, title: "*Max Requested GPUs per Node*", "value": ([$resources[].gpus_per_node] | max) },
        { short: true, title: "*Max Requested Memory per Node*", "value": ([$resources[].memory_per_node] | max) }
      ]
    },
    {
      pretext: "Summary of all `pending` jobs:",
      fields: [
        { short: true, title: "*Number of Jobs*", "value": ($pending | length) },
        { short: true, title: "*Max Requested Nodes per Job*", "value": ([$pending[].min_nodes] | max) },
        { short: true, title: "*Max Requested CPUs per Node*", "value": ([$pending[].cpus_per_node] | max) },
        { short: true, title: "*Max Requested GPUs per Node*", "value": ([$pending[].gpus_per_node] | max) },
        { short: true, title: "*Max Requested Memory per Node*", "value": ([$pending[].memory_per_node] | max) }
      ]
    },
    {
      pretext: "Summary of all jobs:",
      fields: [
        { short: true, title: "*Total Jobs*", "value": (.jobs | length) },
        { short: true, title: "*Running Jobs*", "value": ($running | length) },
        { short: true, title: "*Pending Jobs*", "value": ($pending | length) }
      ]
    },
    (.types | to_entries | map({
      pretext: "Summary of `\(.key)` Node Type",
      fields: [
        { short: true, title: "*Number of Nodes*", "value": .value.count},
        { short: true, title: "*Min/Max Nodes*", "value": "\(.value.minimum)/\(.value.maximum)"},
        { short: true, title: "*Status*", value: (
          if .value.undersubscribed or .value.oversubscribed then
            if .value.oversubscribed then
              "Oversubscribed"
            else
              "Undersubscribed"
            end
          else
            if .value.recognized then
              "OK"
            else
              "Unrecognized"
            end
          end)
        }
      ]
    }))[],
    {
      pretext: "Overall Partition Status",
      fields: [
        { short: true, title: "Total Nodes", value: (.nodes | length) }
      ]
    }
  ]
}
TEMPLATE

# Generate the payload
payload=$( echo "$json" | jq \
  --arg     channel "$SLACK_CHANNEL" \
  --argjson root "$json" \
  --argjson running "$running" \
  --argjson pending "$pending" \
  --argjson resources "$resources" \
  "$template"
)

# Send it to slack
curl -v -H 'Content-Type: application/json; charset=UTF-8' \
  -H "Authorization: Bearer $SLACK_TOKEN" \
  -d @- \
  "https://slack.com/api/chat.postMessage" \
  <<< $payload
