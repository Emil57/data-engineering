"""Bronze step: raw JSONL reviews -> typed Parquet.

Parquet is columnar and compressed, so later jobs read only the columns they need
and run many times faster than re-parsing JSON.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recsys.schema import REVIEW_SCHEMA


def read_raw_reviews(spark: SparkSession, path: str) -> DataFrame:
    # .json() transparently handles .gz files and whole directories
    return spark.read.schema(REVIEW_SCHEMA).json(path)


def to_bronze(raw: DataFrame) -> DataFrame:
    return raw.withColumn(
        "review_date", F.to_date(F.from_unixtime((F.col("timestamp") / 1000).cast("long")))
    ).withColumn("review_year", F.year("review_date"))


def convert(spark: SparkSession, input_path: str, output_path: str) -> None:
    (
        to_bronze(read_raw_reviews(spark, input_path))
        .write.mode("overwrite")
        .partitionBy("review_year")
        .parquet(output_path)
    )
