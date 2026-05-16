import polars as pl
import pytest

from analysis import aggregate_analysis, clean_and_validate


@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "window_start": ["2024-01-01T10:00:00", "2024-01-01T10:10:00",
                         "2024-01-01T10:20:00", "2024-01-01T10:30:00"],
        "window_end":   ["2024-01-01T10:10:00", "2024-01-01T10:20:00",
                         "2024-01-01T10:30:00", "2024-01-01T10:40:00"],
        "shard_id":     [1, 1, 2, 2],
        "total_count":  [100, 150, 200, 50],
        "total_amount": [500000.0, 750000.0, 1000000.0, 250000.0],
        "avg_amount":   [5000.0, 5000.0, 5000.0, 2000.0],
        "min_amount":   [100.0, 200.0, 50.0, 10.0],
        "max_amount":   [50000.0, 60000.0, 70000.0, 5000.0],
        "success_count": [90, 140, 185, 45],
        "failed_count":  [10, 10, 15, 5],
        "currency":     ["RUB", "RUB", "USD", None],
    })


def test_clean_removes_negative_amounts(sample_df):
    cleaned = clean_and_validate(sample_df)
    assert all(cleaned["total_amount"] > 0)


def test_clean_fills_null_currency(sample_df):
    cleaned = clean_and_validate(sample_df)
    assert cleaned["currency"].null_count() == 0
    assert "UNKNOWN" in cleaned["currency"].to_list()


def test_clean_adds_success_rate(sample_df):
    cleaned = clean_and_validate(sample_df)
    assert "success_rate" in cleaned.columns
    assert "failure_rate" in cleaned.columns
    for rate in cleaned["success_rate"].to_list():
        assert 0 <= rate <= 100


def test_aggregate_by_currency(sample_df):
    cleaned = clean_and_validate(sample_df)
    agg = aggregate_analysis(cleaned)
    assert "currency" in agg.columns
    assert "total_volume" in agg.columns
    assert len(agg) >= 1


def test_aggregate_sorted_by_volume(sample_df):
    cleaned = clean_and_validate(sample_df)
    agg = aggregate_analysis(cleaned)
    volumes = agg["total_volume"].to_list()
    assert volumes == sorted(volumes, reverse=True)


def test_clean_removes_zero_count(sample_df):
    df_with_zero = sample_df.with_columns(
        pl.when(pl.col("shard_id") == 2)
        .then(pl.lit(0))
        .otherwise(pl.col("total_count"))
        .alias("total_count")
    )
    cleaned = clean_and_validate(df_with_zero)
    assert all(cleaned["total_count"] > 0)
