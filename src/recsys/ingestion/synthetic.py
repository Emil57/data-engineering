"""Generate a synthetic reviews file with the same shape as Amazon Reviews 2023.

Useful when you cannot download the real data (offline, CI, quick experiments).
Users belong to hidden "taste groups" that prefer certain item groups, so ALS has
a real signal to learn.

    python -m recsys.ingestion.synthetic --users 2000 --items 500 --out data/raw/synthetic.jsonl.gz
"""

from __future__ import annotations

import argparse
import gzip
import json
import random
import sys
from pathlib import Path


def generate(
    out: Path,
    n_users: int = 2000,
    n_items: int = 500,
    reviews_per_user: int = 20,
    n_groups: int = 5,
    seed: int = 42,
) -> Path:
    rng = random.Random(seed)
    out.parent.mkdir(parents=True, exist_ok=True)
    item_group = {i: rng.randrange(n_groups) for i in range(n_items)}
    base_ts = 1_600_000_000_000

    with gzip.open(out, "wt", encoding="utf-8") as f:
        for u in range(n_users):
            group = rng.randrange(n_groups)
            items = rng.sample(range(n_items), k=min(reviews_per_user, n_items))
            for i in items:
                liked = item_group[i] == group
                mean = 4.5 if liked else 2.0
                rating = float(min(5, max(1, round(rng.gauss(mean, 0.8)))))
                record = {
                    "rating": rating,
                    "title": "Great" if rating >= 4 else "Meh",
                    "text": "synthetic review",
                    "asin": f"B{i:09d}",
                    "parent_asin": f"B{i:09d}",
                    "user_id": f"U{u:08d}",
                    "timestamp": base_ts + rng.randrange(10**10),
                    "helpful_vote": rng.randrange(5),
                    "verified_purchase": rng.random() < 0.9,
                }
                f.write(json.dumps(record) + "\n")
    print(f"Wrote {n_users * reviews_per_user} synthetic reviews to {out}")
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--out", type=Path, default=Path("data/raw/synthetic.jsonl.gz"))
    p.add_argument("--users", type=int, default=2000)
    p.add_argument("--items", type=int, default=500)
    p.add_argument("--reviews-per-user", type=int, default=20)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(argv)
    generate(a.out, a.users, a.items, a.reviews_per_user, seed=a.seed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
