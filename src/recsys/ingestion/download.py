"""Download Amazon Reviews 2023 review files (McAuley Lab, UCSD).

Examples:
    # first 100k reviews only (good for local development)
    python -m recsys.ingestion.download --category Movies_and_TV --max-reviews 100000

    # full file
    python -m recsys.ingestion.download --category Movies_and_TV
"""

from __future__ import annotations

import argparse
import gzip
import shutil
import sys
import urllib.request
from pathlib import Path

BASE_URL = (
    "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/"
    "raw/review_categories/{category}.jsonl"
)


def download(category: str, out_dir: Path, max_reviews: int | None = None) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = f".sample{max_reviews}" if max_reviews else ""
    out_path = out_dir / f"{category}{suffix}.jsonl.gz"
    url = BASE_URL.format(category=category)
    print(f"Downloading {url}\n        -> {out_path}")

    with urllib.request.urlopen(url) as resp, gzip.open(out_path, "wb") as out:
        if max_reviews is None:
            shutil.copyfileobj(resp, out, length=1 << 20)
        else:
            for i, line in enumerate(resp):
                if i >= max_reviews:
                    break
                out.write(line)
    print("Done.")
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--category", default="Movies_and_TV")
    parser.add_argument("--out-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--max-reviews", type=int, default=None)
    args = parser.parse_args(argv)
    download(args.category, args.out_dir, args.max_reviews)
    return 0


if __name__ == "__main__":
    sys.exit(main())
