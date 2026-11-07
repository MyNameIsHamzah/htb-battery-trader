"""Tests for Task A: data pipelines, parquet integrity, and leak-free features."""
from datetime import date, timedelta
from pathlib import Path
import pandas as pd
import pytest
from src.features import day_ahead_features, decision_time

DATA = Path(__file__).resolve().parent.parent / "data"
TEST_DAYS = [date(2026, 10, 1) + timedelta(days=i) for i in range(0, 37, 5)]


def test_prices_file_integrity():
    p = DATA / "prices.parquet"
    assert p.exists(), "prices.parquet missing"
    df = pd.read_parquet(p)
    assert len(df) >= 8000, f"Expected >= 8000 rows, got {len(df)}"
    assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
    assert df.index.tz is not None and str(df.index.tz) == "UTC", "Index must be UTC"
    assert not df.index.duplicated().any(), "Duplicate ts found in prices"
    assert df.index.is_monotonic_increasing, "Index must be sorted"
    assert "price" in df.columns and "volume" in df.columns
    assert (df["volume"] == 0).sum() == 0, "Zero-volume rows must be dropped"
    assert (df["price"] < 0).sum() > 0, "Genuine negative prices must be preserved"

    # Completed test days must all have full 48 settlement periods
    counts = df.groupby(df.index.date).size()
    test_counts = counts.loc[date(2026, 10, 1):date(2026, 11, 6)]
    assert (test_counts >= 46).all(), f"Test period days with < 46 periods: {test_counts[test_counts < 46]}"
    # Overall full days (excluding the final in-progress fetch day)
    full_days = counts[counts.index < df.index.date.max()]
    assert (full_days >= 40).all() and (full_days >= 46).mean() > 0.98


def test_imbalance_file_integrity():
    p = DATA / "imbalance.parquet"
    assert p.exists(), "imbalance.parquet missing"
    df = pd.read_parquet(p)
    assert len(df) >= 8000, f"Expected >= 8000 rows, got {len(df)}"
    assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
    assert df.index.tz is not None and str(df.index.tz) == "UTC", "Index must be UTC"
    assert not df.index.duplicated().any(), "Duplicate ts found in imbalance"
    for col in ["ssp", "sbp", "niv"]:
        assert col in df.columns, f"Missing column {col} in imbalance.parquet"


def test_windfor_file_integrity():
    p = DATA / "windfor.parquet"
    assert p.exists(), "windfor.parquet missing"
    df = pd.read_parquet(p)
    assert len(df) > 0, "windfor.parquet is empty"
    assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
    assert df.index.tz is not None and str(df.index.tz) == "UTC", "Index must be UTC"
    assert "wind_forecast_mw" in df.columns, "Missing wind_forecast_mw in windfor.parquet"
    assert "published_at" in df.columns, "Missing published_at in windfor.parquet"
    pub = pd.to_datetime(df["published_at"])
    assert pub.dt.tz is not None and str(pub.dt.tz) == "UTC", "published_at must be UTC"


def test_gen_file_integrity():
    p = DATA / "gen.parquet"
    assert p.exists(), "gen.parquet missing"
    df = pd.read_parquet(p)
    assert len(df) > 0, "gen.parquet is empty"
    assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
    assert df.index.tz is not None and str(df.index.tz) == "UTC", "Index must be UTC"
    assert not df.index.duplicated().any(), "Duplicate ts found in gen.parquet"
    assert "published_at" in df.columns, "Missing published_at in gen.parquet"
    for fuel in ["wind", "ccgt", "nuclear", "biomass"]:
        assert fuel in df.columns, f"Missing fuel type {fuel} in gen.parquet"


def test_demand_file_integrity():
    p = DATA / "demand.parquet"
    assert p.exists(), "demand.parquet missing"
    df = pd.read_parquet(p)
    assert len(df) > 0, "demand.parquet is empty"
    assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
    assert df.index.tz is not None and str(df.index.tz) == "UTC", "Index must be UTC"
    assert "demand_forecast_mw" in df.columns, "Missing demand_forecast_mw in demand.parquet"
    assert "published_at" in df.columns, "Missing published_at in demand.parquet"


@pytest.mark.parametrize("day", TEST_DAYS)
def test_day_ahead_features_no_lookahead(day):
    f = day_ahead_features(day)
    cutoff = decision_time(day)
    assert len(f) >= 46, f"Expected >= 46 periods for {day}, got {len(f)}"
    assert not f.index.duplicated().any(), f"Duplicate ts found in features for {day}"
    for col in [
        "hour", "dow", "price_lag_2d", "price_lag_7d", "price_last_known",
        "mean_by_hour_14d", "wind_forecast_mw", "demand_forecast_mw", "published_at"
    ]:
        assert col in f.columns, f"Missing feature column {col}"

    pubs = pd.to_datetime(f["published_at"])
    assert (pubs < cutoff).all(), f"Lookahead leak: published_at >= cutoff for {day}: {pubs[pubs >= cutoff]}"
    assert not f["wind_forecast_mw"].isna().all(), f"wind_forecast_mw is all NaN for {day}"
    assert not f["demand_forecast_mw"].isna().all(), f"demand_forecast_mw is all NaN for {day}"
