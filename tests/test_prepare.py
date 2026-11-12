from recsys.transform.prepare import clean_ratings, filter_k_core, fit_indexer, index_ratings

COLS = ["user_id", "parent_asin", "rating", "timestamp"]


def test_clean_ratings_keeps_latest_and_drops_invalid(spark):
    df = spark.createDataFrame(
        [
            ("u1", "i1", 2.0, 1),
            ("u1", "i1", 5.0, 2),  # newer duplicate wins
            ("u2", "i1", 9.0, 1),  # invalid rating
            ("u3", None, 4.0, 1),  # missing item
        ],
        COLS,
    )
    rows = clean_ratings(df).collect()
    assert len(rows) == 1
    assert rows[0]["rating"] == 5.0


def test_filter_k_core_is_iterative(spark):
    # u1,u2 rate i1,i2 (dense core). u3 rates i1 and i3 only; i3 has 1 rating.
    data = [
        ("u1", "i1", 5.0, 1), ("u1", "i2", 4.0, 1),
        ("u2", "i1", 3.0, 1), ("u2", "i2", 2.0, 1),
        ("u3", "i1", 4.0, 1), ("u3", "i3", 1.0, 1),
    ]  # fmt: skip
    df = spark.createDataFrame(data, COLS)
    result = filter_k_core(df, min_user=2, min_item=2)
    # i3 is dropped -> u3 left with 1 rating -> u3 dropped too
    assert {r["user_id"] for r in result.collect()} == {"u1", "u2"}
    assert {r["parent_asin"] for r in result.collect()} == {"i1", "i2"}


def test_index_ratings_produces_int_ids(spark):
    df = spark.createDataFrame([("a", "x", 5.0, 1), ("b", "y", 3.0, 1), ("a", "y", 4.0, 1)], COLS)
    indexed = index_ratings(df, fit_indexer(df))
    types = dict(indexed.dtypes)
    assert types["user_idx"] == "int" and types["item_idx"] == "int"
    assert indexed.select("user_idx").distinct().count() == 2
