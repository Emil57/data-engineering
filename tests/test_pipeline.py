from recsys.ingestion.synthetic import generate
from recsys.ingestion.to_parquet import read_raw_reviews, to_bronze
from recsys.ml.als import ALSParams, ranking_metrics, rmse, top_k_recommendations, train
from recsys.transform.prepare import clean_ratings, filter_k_core, fit_indexer, index_ratings


def test_end_to_end_on_synthetic_data(spark, tmp_path):
    raw = generate(tmp_path / "raw.jsonl.gz", n_users=200, n_items=60, reviews_per_user=15)
    bronze = to_bronze(read_raw_reviews(spark, str(raw)))
    assert "review_year" in bronze.columns

    ratings = filter_k_core(clean_ratings(bronze), 3, 3)
    indexed = index_ratings(ratings, fit_indexer(ratings)).cache()
    train_df, test_df = indexed.randomSplit([0.8, 0.2], seed=1)

    model = train(train_df, ALSParams(rank=8, max_iter=5))
    error = rmse(model, test_df)
    assert 0 < error < 1.5  # synthetic data has a strong signal

    metrics = ranking_metrics(model, train_df, test_df, k=5)
    assert set(metrics) == {"precisionAtK@5", "recallAtK@5", "ndcgAtK@5"}

    recs = top_k_recommendations(model, indexed, k=5)
    assert recs.columns == ["user_id", "rank", "parent_asin", "predicted_rating"]
    assert recs.groupBy("user_id").count().filter("count != 5").count() == 0


def test_recommendations_exclude_already_rated_items(spark, tmp_path):
    raw = generate(tmp_path / "raw.jsonl.gz", n_users=100, n_items=40, reviews_per_user=10)
    ratings = clean_ratings(read_raw_reviews(spark, str(raw)))
    indexed = index_ratings(ratings, fit_indexer(ratings)).cache()
    model = train(indexed, ALSParams(rank=4, max_iter=3))

    recs = top_k_recommendations(model, indexed, k=5)
    overlap = recs.join(indexed, ["user_id", "parent_asin"])
    assert overlap.count() == 0
