#!/usr/bin/env bash
# EMR bootstrap action: runs on every node before Spark starts.
# pyspark.ml needs numpy on the executors' Python.
set -euo pipefail
sudo python3 -m pip install --quiet "numpy>=1.24"
