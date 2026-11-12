"""SparkSession factory shared by all jobs."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "amazon-reviews-recsys", local: bool = False) -> SparkSession:
    """Create (or reuse) a SparkSession.

    On EMR, ``spark-submit`` already provides the master (YARN) and resources, so
    ``local`` must be False. For laptop runs, ``local=True`` uses every CPU core and
    a small number of shuffle partitions, which is much faster on small data.
    """
    builder = SparkSession.builder.appName(app_name)
    if local:
        builder = (
            builder.master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "4g")
            .config("spark.ui.showConsoleProgress", "false")
        )
    return builder.getOrCreate()
