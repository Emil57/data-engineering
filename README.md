# Amazon Reviews Recommender (PySpark + AWS EMR)

A movie/product recommender trained on the **Amazon Reviews 2023** dataset with
**Apache Spark MLlib ALS** (collaborative filtering). It runs on a laptop for development
and on **AWS EMR** for the full dataset.

```
 raw JSONL (.gz)  ──convert──►  bronze Parquet  ──clean / k-core / index──►  ALS training
   data/raw  or                  data/bronze  or                                  │
   s3://…/raw                    s3://…/bronze             ┌──────────────────────┼─────────────────┐
                                                           ▼                      ▼                 ▼
                                                     output/model        output/metrics   output/recommendations
```

## Quickstart (local)

You need **Python 3.9+** and **Java 17 or 21**. Check with `java -version`. On macOS, run
`brew install openjdk@21`. On Ubuntu, run `sudo apt install openjdk-21-jdk`.

```bash
make setup        # create .venv and install PySpark, numpy, pytest, ruff
make test         # unit + end-to-end tests on a local Spark

# Option A: synthetic data (no download)
make synthetic

# Option B: real data, first 100k Movies & TV reviews
make sample                         # or: make sample CATEGORY=Books SAMPLE=500000

make pipeline     # convert to Parquet, then train ALS
```

Outputs land in `data/output/`:

| Path | Contents |
|---|---|
| `model/` | Trained `ALSModel` (load with `ALSModel.load(path)`) |
| `indexer/` | `StringIndexer` pipeline that maps Amazon IDs to integers |
| `metrics/` | JSON with RMSE, precision@k, recall@k, NDCG@k |
| `recommendations/` | Parquet: `user_id, rank, parent_asin, predicted_rating` |

Run `make help` to list every task.

## Project layout

```
src/recsys/
  spark.py                 SparkSession factory (local vs cluster)
  schema.py                explicit schema of the review files
  ingestion/download.py    download (a sample of) a category from Hugging Face
  ingestion/synthetic.py   synthetic reviews with a learnable signal
  ingestion/to_parquet.py  raw JSONL -> partitioned Parquet (bronze)
  transform/prepare.py     clean, dedupe, k-core filter, index IDs
  ml/als.py                ALS training, RMSE + ranking metrics, top-k recs
jobs/                      spark-submit entrypoints (local and EMR)
tests/                     pytest suite with a local SparkSession
infra/emr/                 scripts to deploy and run on AWS EMR
docs/aws-emr.md            step-by-step EMR guide
```

## How the model works

1. **Clean.** Keep `(user_id, parent_asin, rating)`. If a user reviewed an item twice, keep the latest review.
2. **k-core filter.** Repeatedly drop users and items with fewer than 5 ratings. Very sparse data gives a poor model.
3. **Index.** ALS needs integer IDs, so `StringIndexer` maps the string IDs to `0..N-1`.
4. **Train ALS.** Each user and each item gets a vector of `rank` hidden "taste factors". The predicted rating is their dot product.
5. **Evaluate.** RMSE on held-out ratings, plus precision, recall and NDCG at k. These check whether the top-k recommendations include items the user actually liked.
6. **Recommend.** Produce the top-k items per user, excluding items they have already rated.

Tune it with flags, for example:
`python jobs/train_als.py --input data/bronze/reviews --output data/output --local --rank 64 --reg-param 0.05`

## Running on AWS EMR

```bash
cp infra/emr/config.env.example infra/emr/config.env   # set BUCKET, region, sizes
aws emr create-default-roles                           # one-time
./infra/emr/deploy.sh --data data/raw/Movies_and_TV.jsonl.gz
./infra/emr/create_cluster.sh                          # runs convert + train, then terminates
```

See [docs/aws-emr.md](docs/aws-emr.md) for the full walkthrough: how to get the data into S3,
monitoring, the Spark UI, costs, and EMR Serverless.

## Versions

Local development uses PySpark 4.x. EMR 7.x ships Spark 3.5. The code uses only APIs that
work on both and keeps Python 3.9 compatibility for the EMR nodes.
