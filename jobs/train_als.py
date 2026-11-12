"""Spark job: train the ALS recommender and write model, metrics and top-k recommendations.

Local:  python jobs/train_als.py --input data/bronze/reviews --output data/output --local
EMR:    spark-submit --py-files recsys.zip train_als.py --input s3://... --output s3://...
"""

from __future__ import annotations

import argparse
import json

from recsys.ml.als import ALSParams, ranking_metrics, rmse, top_k_recommendations, train
from recsys.spark import get_spark
from recsys.transform.prepare import clean_ratings, filter_k_core, fit_indexer, index_ratings


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train ALS recommender")
    p.add_argument("--input", required=True, help="bronze Parquet dir")
    p.add_argument("--output", required=True, help="output dir for model/metrics/recs")
    p.add_argument("--min-user-reviews", type=int, default=5)
    p.add_argument("--min-item-reviews", type=int, default=5)
    p.add_argument("--sample-fraction", type=float, default=None, help="e.g. 0.1 for 10%%")
    p.add_argument("--rank", type=int, default=32)
    p.add_argument("--max-iter", type=int, default=10)
    p.add_argument("--reg-param", type=float, default=0.1)
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--local", action="store_true", help="run with local[*] master")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("train-als", local=args.local)
    # ALS is iterative; checkpointing keeps its lineage short on large data
    spark.sparkContext.setCheckpointDir(f"{args.output}/_checkpoints")

    reviews = spark.read.parquet(args.input)
    if args.sample_fraction:
        reviews = reviews.sample(fraction=args.sample_fraction, seed=42)

    ratings = filter_k_core(clean_ratings(reviews), args.min_user_reviews, args.min_item_reviews)
    indexer = fit_indexer(ratings)
    indexed = index_ratings(ratings, indexer).cache()

    train_df, test_df = indexed.randomSplit([0.8, 0.2], seed=42)
    params = ALSParams(rank=args.rank, max_iter=args.max_iter, reg_param=args.reg_param)
    model = train(train_df, params)

    metrics = {
        "n_ratings": indexed.count(),
        "n_users": indexed.select("user_idx").distinct().count(),
        "n_items": indexed.select("item_idx").distinct().count(),
        "rmse": rmse(model, test_df),
        **ranking_metrics(model, train_df, test_df, k=args.top_k),
        "params": params.to_dict(),
    }
    print(json.dumps(metrics, indent=2))

    model.write().overwrite().save(f"{args.output}/model")
    indexer.write().overwrite().save(f"{args.output}/indexer")
    spark.createDataFrame([(json.dumps(metrics),)], ["metrics"]).coalesce(1).write.mode(
        "overwrite"
    ).text(f"{args.output}/metrics")
    top_k_recommendations(model, indexed, k=args.top_k).write.mode("overwrite").parquet(
        f"{args.output}/recommendations"
    )
    print(f"Outputs written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
