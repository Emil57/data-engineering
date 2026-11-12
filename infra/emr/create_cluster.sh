#!/usr/bin/env bash
# Launch an EMR-on-EC2 cluster that runs the convert + train steps.
# With AUTO_TERMINATE=true the cluster shuts itself down when the steps finish.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

steps_file="$(mktemp)"
trap 'rm -f "$steps_file"' EXIT
steps_json > "$steps_file"

core_market="ON_DEMAND"
[[ "$USE_SPOT_FOR_CORE" == "true" ]] && core_market="SPOT"

termination_args=(--auto-termination-policy "IdleTimeout=$IDLE_TIMEOUT_SECONDS")
if [[ "$AUTO_TERMINATE" == "true" ]]; then
  termination_args=(--auto-terminate)
else
  export ACTION_ON_FAILURE=CONTINUE
  steps_json > "$steps_file"
fi

cluster_id=$(aws emr create-cluster \
  --name "amazon-reviews-recsys" \
  --release-label "$RELEASE_LABEL" \
  --applications Name=Spark \
  --instance-groups \
    "InstanceGroupType=MASTER,InstanceCount=1,InstanceType=$PRIMARY_INSTANCE_TYPE" \
    "InstanceGroupType=CORE,InstanceCount=$CORE_INSTANCE_COUNT,InstanceType=$CORE_INSTANCE_TYPE,Market=$core_market" \
  --use-default-roles \
  --log-uri "$S3_LOGS/" \
  --bootstrap-actions "Path=$S3_CODE/bootstrap.sh,Name=install-python-deps" \
  --steps "file://$steps_file" \
  "${termination_args[@]}" \
  --query ClusterId --output text)

echo "Cluster started: $cluster_id"
echo "Watch it:  aws emr describe-cluster --cluster-id $cluster_id --query Cluster.Status"
echo "Steps:     aws emr list-steps --cluster-id $cluster_id"
echo "Console:   https://console.aws.amazon.com/emr/home?region=$AWS_REGION#/clusterDetails/$cluster_id"
