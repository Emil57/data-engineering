"""Train and evaluate an ALS collaborative-filtering model."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.ml.evaluation import RankingEvaluator, RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from recsys.transform.prepare import ITEM_COL, ITEM_IDX, RATING_COL, USER_COL, USER_IDX


@dataclass
class ALSParams:
    rank: int = 32  # number of hidden "taste" factors
    max_iter: int = 10
    reg_param: float = 0.1  # regularisation, fights overfitting
    implicit_prefs: bool = False
    seed: int = 42

    def to_dict(self) -> dict:
        return asdict(self)


def build_als(params: ALSParams) -> ALS:
    return ALS(
        userCol=USER_IDX,
        itemCol=ITEM_IDX,
        ratingCol=RATING_COL,
        rank=params.rank,
        maxIter=params.max_iter,
        regParam=params.reg_param,
        implicitPrefs=params.implicit_prefs,
        nonnegative=True,
        coldStartStrategy="drop",  # skip users/items unseen during training
        seed=params.seed,
    )


def train(train_df: DataFrame, params: ALSParams) -> ALSModel:
    return build_als(params).fit(train_df)


def rmse(model: ALSModel, test_df: DataFrame) -> float:
    evaluator = RegressionEvaluator(
        metricName="rmse", labelCol=RATING_COL, predictionCol="prediction"
    )
    return evaluator.evaluate(model.transform(test_df))


def recommend_unseen(
    model: ALSModel, users: DataFrame, seen: DataFrame, k: int = 10, buffer: int = 50
) -> DataFrame:
    """Top-k items per user, excluding items the user already rated.

    ALS scores every item, including ones the user has already reviewed, so we ask
    for k + buffer candidates and drop the seen ones.

    Returns columns: user_idx, rank, item_idx, predicted_rating.
    """
    by_score = Window.partitionBy(USER_IDX).orderBy(F.col("predicted_rating").desc())
    return (
        model.recommendForUserSubset(users.select(USER_IDX).distinct(), k + buffer)
        .select(USER_IDX, F.explode("recommendations").alias("rec"))
        .select(
            USER_IDX,
            F.col("rec." + ITEM_IDX).alias(ITEM_IDX),
            F.col("rec.rating").alias("predicted_rating"),
        )
        .join(seen.select(USER_IDX, ITEM_IDX), [USER_IDX, ITEM_IDX], "left_anti")
        .withColumn("rank", F.row_number().over(by_score))
        .filter(F.col("rank") <= k)
        .select(USER_IDX, "rank", ITEM_IDX, "predicted_rating")
    )


def ranking_metrics(
    model: ALSModel,
    train_df: DataFrame,
    test_df: DataFrame,
    k: int = 10,
    relevant_threshold: float = 4.0,
) -> dict:
    """Precision/Recall/NDCG@k: do the top-k unseen recommendations hit items the user
    rated >= relevant_threshold in the test set?"""
    relevant = (
        test_df.filter(F.col(RATING_COL) >= relevant_threshold)
        .groupBy(USER_IDX)
        .agg(F.collect_list(F.col(ITEM_IDX).cast("double")).alias("label"))
    )
    recs = (
        recommend_unseen(model, relevant, seen=train_df, k=k)
        .groupBy(USER_IDX)
        .agg(F.sort_array(F.collect_list(F.struct("rank", ITEM_IDX))).alias("recs"))
        .select(USER_IDX, F.col("recs." + ITEM_IDX).cast("array<double>").alias("prediction"))
    )
    joined = recs.join(relevant, USER_IDX)
    results = {}
    for metric in ("precisionAtK", "recallAtK", "ndcgAtK"):
        evaluator = RankingEvaluator(
            metricName=metric, k=k, labelCol="label", predictionCol="prediction"
        )
        results[f"{metric}@{k}"] = evaluator.evaluate(joined)
    return results


def top_k_recommendations(model: ALSModel, indexed: DataFrame, k: int = 10) -> DataFrame:
    """Top-k unseen items per user, mapped back to the original Amazon IDs."""
    users = indexed.select(USER_COL, USER_IDX).distinct()
    items = indexed.select(ITEM_COL, ITEM_IDX).distinct()
    return (
        recommend_unseen(model, users, seen=indexed, k=k)
        .join(users, USER_IDX)
        .join(items, ITEM_IDX)
        .select(USER_COL, "rank", ITEM_COL, "predicted_rating")
    )
