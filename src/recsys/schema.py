"""Explicit schema for the Amazon Reviews 2023 review files.

Declaring the schema up front avoids a full extra pass over the data that Spark
would otherwise need to infer it from JSON.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

REVIEW_SCHEMA = StructType(
    [
        StructField("rating", DoubleType()),
        StructField("title", StringType()),
        StructField("text", StringType()),
        StructField("asin", StringType()),
        StructField("parent_asin", StringType()),
        StructField("user_id", StringType()),
        StructField("timestamp", LongType()),  # milliseconds since epoch
        StructField("helpful_vote", LongType()),
        StructField("verified_purchase", BooleanType()),
    ]
)
