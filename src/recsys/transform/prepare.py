"""Silver/gold steps: clean ratings, filter sparse users/items, index IDs for ALS."""

from __future__ import annotations

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

USER_COL, ITEM_COL, RATING_COL = "user_id", "parent_asin", "rating"
USER_IDX, ITEM_IDX = "user_idx", "item_idx"


def clean_ratings(reviews: DataFrame) -> DataFrame:
    """Keep valid (user, item, rating) rows; if a user reviewed an item twice keep the latest."""
    latest_first = Window.partitionBy(USER_COL, ITEM_COL).orderBy(F.col("timestamp").desc())
    return (
        reviews.select(USER_COL, ITEM_COL, RATING_COL, "timestamp")
        .dropna(subset=[USER_COL, ITEM_COL, RATING_COL])
        .filter(F.col(RATING_COL).between(1, 5))
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter("_rn = 1")
        .drop("_rn")
    )


def filter_k_core(
    ratings: DataFrame, min_user: int = 5, min_item: int = 5, max_iter: int = 10
) -> DataFrame:
    """Repeatedly drop users with < min_user ratings and items with < min_item ratings.

    Removing items can push users below the threshold (and vice versa), so we
    iterate until the row count stops changing ("k-core" filtering).
    """
    current = ratings
    prev_count = current.count()
    for _ in range(max_iter):
        users = current.groupBy(USER_COL).count().filter(F.col("count") >= min_user)
        items = current.groupBy(ITEM_COL).count().filter(F.col("count") >= min_item)
        current = (
            current.join(users.select(USER_COL), USER_COL, "left_semi")
            .join(items.select(ITEM_COL), ITEM_COL, "left_semi")
            .localCheckpoint()  # cut the growing query plan between iterations
        )
        count = current.count()
        if count == prev_count:
            break
        prev_count = count
    return current


def fit_indexer(ratings: DataFrame) -> PipelineModel:
    """ALS needs integer IDs: map string user/item IDs to 0..N-1."""
    return Pipeline(
        stages=[
            StringIndexer(inputCol=USER_COL, outputCol=USER_IDX),
            StringIndexer(inputCol=ITEM_COL, outputCol=ITEM_IDX),
        ]
    ).fit(ratings)


def index_ratings(ratings: DataFrame, indexer: PipelineModel) -> DataFrame:
    return indexer.transform(ratings).select(
        USER_COL,
        ITEM_COL,
        F.col(USER_IDX).cast("int").alias(USER_IDX),
        F.col(ITEM_IDX).cast("int").alias(ITEM_IDX),
        F.col(RATING_COL).cast("float").alias(RATING_COL),
    )
