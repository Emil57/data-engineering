# Running the recommender on AWS EMR

## 1. One-time setup

1. Install the AWS CLI v2 and run `aws configure` with an IAM user or SSO profile that can
   use EMR, EC2 and S3.
2. Create the default EMR roles (`EMR_DefaultRole`, `EMR_EC2_DefaultRole`):
   ```bash
   aws emr create-default-roles
   ```
3. Create your config:
   ```bash
   cp infra/emr/config.env.example infra/emr/config.env
   # edit BUCKET (globally unique), AWS_REGION, RELEASE_LABEL, instance sizes
   ```

## 2. Get the data into S3

The full `Movies_and_TV` reviews file is several GB. Choose one option:

- **From your machine:**
  ```bash
  make sample SAMPLE=1000000     # or download the full file:
  .venv/bin/python -m recsys.ingestion.download --category Movies_and_TV
  ./infra/emr/deploy.sh --data data/raw/Movies_and_TV.jsonl.gz
  ```
- **Straight into S3 from an EC2 machine or CloudShell** (faster, no local disk needed):
  ```bash
  curl -L https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/Movies_and_TV.jsonl \
    | gzip | aws s3 cp - s3://$BUCKET/raw/Movies_and_TV/Movies_and_TV.jsonl.gz
  ```

The S3 layout used by the scripts:

```
s3://BUCKET/
  code/      recsys.zip, jobs/*.py, bootstrap.sh
  raw/       <CATEGORY>/*.jsonl.gz
  bronze/    reviews/ (Parquet, partitioned by review_year)
  output/    model/, indexer/, metrics/, recommendations/
  logs/      EMR and Spark logs
```

## 3. Deploy the code and run

```bash
./infra/emr/deploy.sh          # zips src/recsys and uploads jobs + bootstrap script
./infra/emr/create_cluster.sh  # creates the cluster and runs 2 steps
```

The cluster runs two **steps**, each a `spark-submit --deploy-mode cluster`:

1. `convert_to_parquet.py`: raw → bronze Parquet
2. `train_als.py`: bronze → model, metrics, recommendations

With `AUTO_TERMINATE=true` the cluster shuts down when the steps finish. Set it to `false`
to keep the cluster running for experiments; it will then stop after `IDLE_TIMEOUT_SECONDS`
of inactivity. Re-run the steps on a running cluster with:

```bash
./infra/emr/add_steps.sh j-XXXXXXXXXXXXX
```

For your first run, set `SAMPLE_FRACTION=0.1` in `config.env` to train on 10% of the data.

## 4. Monitor

- **Console → EMR → Clusters → your cluster → Steps:** status and `stdout` of each step.
  The metrics JSON appears in the training step's stdout.
- **Applications → Spark History Server:** the Spark UI, with jobs, stages, shuffle sizes
  and executor memory. Use it to understand where time is spent.
- **Logs** in `s3://BUCKET/logs/<cluster-id>/`.
- CLI:
  ```bash
  aws emr list-steps --cluster-id j-XXX --query 'Steps[].{name:Name,state:Status.State}'
  aws s3 cp s3://$BUCKET/output/metrics/ - --recursive
  ```

## 5. Use the results

`output/recommendations` is Parquet. You can:
- query it with **Athena**: run a Glue crawler on the path, then
  `SELECT * FROM recommendations WHERE user_id = '...' ORDER BY rank`
- load it back into Spark with `spark.read.parquet("s3://BUCKET/output/recommendations")`

## Sizing and cost tips

| Data | Suggested cluster |
|---|---|
| Sample (≤ 1M reviews) | 1 × m5.xlarge primary + 2 × r5.xlarge core |
| Full Movies_and_TV (~17M) | 1 × m5.xlarge + 4 × r5.2xlarge core |

- Keep `AUTO_TERMINATE=true` while learning. An idle cluster still costs money.
- Set `USE_SPOT_FOR_CORE=true` to save roughly 60–70%. A Spot interruption can fail a step.
- Convert to Parquet once and point training at `bronze/`.
- Set up an **AWS Budget** alert before your first run.

## Alternative: EMR Serverless

You don't manage a cluster here; you pay only while the job runs. You need an IAM *execution role*
that can read and write your bucket.

```bash
APP_ID=$(aws emr-serverless create-application --type SPARK \
  --release-label emr-7.5.0 --name recsys --query applicationId --output text)

aws emr-serverless start-job-run --application-id "$APP_ID" \
  --execution-role-arn arn:aws:iam::<ACCOUNT_ID>:role/<EMRServerlessS3Role> \
  --job-driver '{"sparkSubmit": {
    "entryPoint": "s3://BUCKET/code/jobs/train_als.py",
    "entryPointArguments": ["--input", "s3://BUCKET/bronze/reviews", "--output", "s3://BUCKET/output"],
    "sparkSubmitParameters": "--py-files s3://BUCKET/code/recsys.zip --conf spark.executor.memory=8g --conf spark.executor.cores=4"
  }}' \
  --configuration-overrides '{"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": "s3://BUCKET/logs/serverless/"}}}'
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ModuleNotFoundError: recsys` | Re-run `deploy.sh`; check that `--py-files .../recsys.zip` is in the step |
| `ModuleNotFoundError: numpy` | Make sure the bootstrap action ran (`logs/<id>/node/*/bootstrap-actions`) |
| Executors lost / OOM in ALS | Use bigger `r5` nodes, add nodes, or lower `ALS_RANK` |
| `Access Denied` on S3 | `EMR_EC2_DefaultRole` must be able to read and write your bucket |
