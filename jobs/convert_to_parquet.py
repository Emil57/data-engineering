"""Spark job: raw Amazon Reviews JSONL(.gz) -> Parquet (bronze layer).

Local:  python jobs/convert_to_parquet.py --input data/raw --output data/bronze/reviews --local
EMR:    spark-submit --py-files recsys.zip convert_to_parquet.py --input s3://... --output s3://...
"""

from __future__ import annotations

import argparse

from recsys.ingestion.to_parquet import convert
from recsys.spark import get_spark


def main() -> None:
    p = argparse.ArgumentParser(description="Convert raw reviews to Parquet")
    p.add_argument("--input", required=True, help="JSONL file/dir (local path or s3://)")
    p.add_argument("--output", required=True, help="Parquet output dir (local path or s3://)")
    p.add_argument("--local", action="store_true", help="run with local[*] master")
    args = p.parse_args()

    spark = get_spark("reviews-to-parquet", local=args.local)
    convert(spark, args.input, args.output)
    print(f"Wrote Parquet to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
