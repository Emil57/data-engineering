#!/usr/bin/env bash
# Shared settings for the EMR scripts. Sourced, not executed.
set -euo pipefail

EMR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$EMR_DIR/../.." && pwd)"

if [[ ! -f "$EMR_DIR/config.env" ]]; then
  echo "Missing $EMR_DIR/config.env. Run: cp $EMR_DIR/config.env.example $EMR_DIR/config.env" >&2
  exit 1
fi
# shellcheck disable=SC1091
source "$EMR_DIR/config.env"
export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION"

S3_ROOT="s3://$BUCKET"
S3_CODE="$S3_ROOT/code"
S3_RAW="$S3_ROOT/raw/$CATEGORY"
S3_BRONZE="$S3_ROOT/bronze/reviews"
S3_OUTPUT="$S3_ROOT/output"
S3_LOGS="$S3_ROOT/logs"

# Writes EMR step definitions (convert + train) as JSON to stdout.
steps_json() {
  local sample_args=""
  if [[ -n "${SAMPLE_FRACTION:-}" ]]; then
    sample_args=", \"--sample-fraction\", \"$SAMPLE_FRACTION\""
  fi
  cat <<JSON
[
  {
    "Type": "CUSTOM_JAR", "Name": "Convert raw JSONL to Parquet", "ActionOnFailure": "${ACTION_ON_FAILURE:-TERMINATE_CLUSTER}",
    "Jar": "command-runner.jar",
    "Args": ["spark-submit", "--deploy-mode", "cluster",
             "--py-files", "$S3_CODE/recsys.zip",
             "$S3_CODE/jobs/convert_to_parquet.py",
             "--input", "$S3_RAW/", "--output", "$S3_BRONZE"]
  },
  {
    "Type": "CUSTOM_JAR", "Name": "Train ALS recommender", "ActionOnFailure": "${ACTION_ON_FAILURE:-TERMINATE_CLUSTER}",
    "Jar": "command-runner.jar",
    "Args": ["spark-submit", "--deploy-mode", "cluster",
             "--py-files", "$S3_CODE/recsys.zip",
             "$S3_CODE/jobs/train_als.py",
             "--input", "$S3_BRONZE", "--output", "$S3_OUTPUT",
             "--rank", "$ALS_RANK", "--max-iter", "$ALS_MAX_ITER",
             "--reg-param", "$ALS_REG_PARAM"$sample_args]
  }
]
JSON
}
