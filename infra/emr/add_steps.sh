#!/usr/bin/env bash
# Re-run the convert + train steps on an already running cluster.
#   ./infra/emr/add_steps.sh j-XXXXXXXXXXXX
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
cluster_id="${1:?usage: add_steps.sh <cluster-id>}"

steps_file="$(mktemp)"
trap 'rm -f "$steps_file"' EXIT
ACTION_ON_FAILURE=CONTINUE steps_json > "$steps_file"

aws emr add-steps --cluster-id "$cluster_id" --steps "file://$steps_file"
