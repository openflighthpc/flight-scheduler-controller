#!/bin/bash

set -e

source ~/.secretrc

read -r -d '' template <<'TEMPLATE' || true
{
  channel: "workshop",
  as_user: true,
  text: ("Scheduler Update: `" + .action + "` nodes"),
  attachments: [
    {
      pretext: "Summary of jobs which are blocked due to insufficient `resources`:",
      fields: [
        { short: true, title: "*Number of Jobs*", "value": (.resource_jobs | length) },
        { short: true, title: "*Number of Exclusive Nodes*", "value": .resource_aggregate.exclusive_nodes_count },
        { short: true, title: "*Max Requested Nodes per Job*", "value": .resource_aggregate.nodes_per_job },
        { short: true, title: "*Max Requested CPUs per Node*", "value": .resource_aggregate.cpus_per_node },
        { short: true, title: "*Max Requested GPUs per Node*", "value": .resource_aggregate.gpus_per_node },
        { short: true, title: "*Max Requested Memory per Node*", "value": .resource_aggregate.exclusive_nodes_count },
        { short: true, title: "*Total Required CPUs*", "value": .resource_aggregate.cpus_count },
        { short: true, title: "*Total Required GPUs*", "value": .resource_aggregate.gpus_count },
        { short: true, title: "*Total Required Memory*", "value": .resource_aggregate.memory_count }
      ]
    },
    {
      pretext: "Summary of all `pending` jobs:",
      fields: [
        { short: true, title: "*Number of Jobs*", "value": (.pending_jobs | length) },
        { short: true, title: "*Number of Exclusive Nodes*", "value": .pending_aggregate.exclusive_nodes_count },
        { short: true, title: "*Max Requested Nodes per Job*", "value": .pending_aggregate.nodes_per_job },
        { short: true, title: "*Max Requested CPUs per Node*", "value": .pending_aggregate.cpus_per_node },
        { short: true, title: "*Max Requested GPUs per Node*", "value": .pending_aggregate.gpus_per_node },
        { short: true, title: "*Max Requested Memory per Node*", "value": .pending_aggregate.exclusive_nodes_count },
        { short: true, title: "*Total Required CPUs*", "value": .pending_aggregate.cpus_count },
        { short: true, title: "*Total Required GPUs*", "value": .pending_aggregate.gpus_count },
        { short: true, title: "*Total Required Memory*", "value": .pending_aggregate.memory_count }
      ]
    },
    {
      pretext: "Overall Cluster Status",
      fields: [
        { short: true, title: "Total Nodes", value: (.nodes | length) },
        { short: true, title: "Total CPUs", value: ([.nodes[].cpus] | add) },
        { short: true, title: "Total GPUs", value: ([.nodes[].gpus] | add) },
        { short: true, title: "Total Memory", value: ([.nodes[].memory] | add) },
        { short: true, title:  }
      ]
    }
  ]
}
TEMPLATE

payload=$(jq "$template" <&0)
echo $payload

curl -v -H 'Content-Type: application/json; charset=UTF-8' \
                  -H "Authorization: Bearer $SLACK_TOKEN" \
                  -d @- \
                  "https://slack.com/api/chat.postMessage" \
                  2>/dev/null <<< $payload
