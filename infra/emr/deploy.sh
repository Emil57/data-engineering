#!/usr/bin/env bash
# Package the code and upload code + bootstrap script to S3.
#   ./infra/emr/deploy.sh                       code only
#   ./infra/emr/deploy.sh --data data/raw/X.jsonl.gz   also upload a raw data file
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "Creating bucket $BUCKET in $AWS_REGION"
  aws s3 mb "$S3_ROOT" --region "$AWS_REGION"
fi

make -C "$REPO_ROOT" package
aws s3 cp "$REPO_ROOT/dist/recsys.zip" "$S3_CODE/recsys.zip"
aws s3 cp "$REPO_ROOT/jobs/" "$S3_CODE/jobs/" --recursive --exclude "*" --include "*.py"
aws s3 cp "$EMR_DIR/bootstrap.sh" "$S3_CODE/bootstrap.sh"

if [[ "${1:-}" == "--data" && -n "${2:-}" ]]; then
  aws s3 cp "$2" "$S3_RAW/"
fi
echo "Deployed to $S3_CODE"
