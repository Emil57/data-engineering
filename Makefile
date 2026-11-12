# Common tasks. Run `make help` to list them.
PY      ?= .venv/bin/python
DATA    ?= data
INPUT   ?= $(DATA)/raw
CATEGORY ?= Movies_and_TV
SAMPLE  ?= 100000

.PHONY: help setup test lint fmt synthetic sample bronze train pipeline package clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-12s %s\n", $$1, $$2}'

setup:  ## Create .venv and install dependencies (needs Java 17+)
	python3 -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -r requirements-dev.txt -e .

test:  ## Run unit + end-to-end tests
	$(PY) -m pytest

lint:  ## Lint with ruff
	$(PY) -m ruff check src jobs tests
	$(PY) -m ruff format --check src jobs tests

fmt:  ## Auto-format with ruff
	$(PY) -m ruff check --fix src jobs tests
	$(PY) -m ruff format src jobs tests

synthetic:  ## Generate synthetic reviews in data/raw (no download needed)
	$(PY) -m recsys.ingestion.synthetic --out $(INPUT)/synthetic.jsonl.gz

sample:  ## Download the first $(SAMPLE) real reviews of $(CATEGORY)
	$(PY) -m recsys.ingestion.download --category $(CATEGORY) --max-reviews $(SAMPLE) --out-dir $(INPUT)

bronze:  ## Convert raw JSONL in data/raw to Parquet in data/bronze
	$(PY) jobs/convert_to_parquet.py --input $(INPUT) --output $(DATA)/bronze/reviews --local

train:  ## Train ALS locally on data/bronze, write to data/output
	$(PY) jobs/train_als.py --input $(DATA)/bronze/reviews --output $(DATA)/output --local

pipeline: bronze train  ## bronze + train

package:  ## Zip the recsys package for spark-submit --py-files
	rm -rf dist && mkdir -p dist
	cd src && zip -qr ../dist/recsys.zip recsys -x '*__pycache__*'
	@echo "Built dist/recsys.zip"

clean:  ## Remove generated data and build files
	rm -rf dist $(DATA)/bronze $(DATA)/output spark-warehouse metastore_db derby.log
