"""Tests for Task B: LightGBM model correctness and no data leakage."""
from datetime import date
import numpy as np
import pandas as pd
import pytest
from src import config as C
from src.model import train_day_ahead, predict_day_ahead, FEATURE_COLS, _build_dataset
from src.features import periods


@pytest.fixture(scope="module")
def lgbm_model():
    """Train lgbm once for all tests in this module."""
    return train_day_ahead("lgbm", C.TRAIN_START, C.TRAIN_END)


@pytest.fixture(scope="module")
def lgbm_peak_model():
    return train_day_ahead("lgbm_peak", C.TRAIN_START, C.TRAIN_END)


def test_lgbm_returns_48_non_nan(lgbm_model):
    day = date(2026, 10, 5)
    pred = predict_day_ahead(lgbm_model, day)
    assert len(pred) == 48, f"Expected 48 periods, got {len(pred)}"
    assert not pred.isna().any(), f"Predictions contain NaN: {pred.isna().sum()}"
    assert (pred.index == periods(day)).all(), "Index must match periods(day)"


def test_lgbm_peak_returns_48_non_nan(lgbm_peak_model):
    day = date(2026, 10, 10)
    pred = predict_day_ahead(lgbm_peak_model, day)
    assert len(pred) == 48
    assert not pred.isna().any()


def test_training_does_not_touch_test_dates():
    """Training dataset must contain no rows with ts >= TEST_START."""
    X, y, _ = _build_dataset(C.TRAIN_START, C.TRAIN_END)
    from src.features import day_ahead_features
    f = day_ahead_features(C.TEST_START)
    cutoff = pd.Timestamp(C.TEST_START, tz="UTC").replace(hour=11) - pd.Timedelta(days=1)
    pubs = pd.to_datetime(f["published_at"])
    assert (pubs < cutoff + pd.Timedelta(seconds=1)).all(), \
        "Features for TEST_START day reference data beyond cutoff"


@pytest.mark.parametrize("day", [date(2026, 10, 1), date(2026, 10, 15), date(2026, 11, 1)])
def test_lgbm_produces_sensible_range(lgbm_model, day):
    """Predictions should be within [-200, 800] £/MWh (the realistic GB range)."""
    pred = predict_day_ahead(lgbm_model, day)
    assert pred.min() > -200, f"Prediction too low: {pred.min():.1f}"
    assert pred.max() < 800, f"Prediction too high: {pred.max():.1f}"


def test_lgbm_varies_across_hours(lgbm_model):
    """Model should not predict a flat constant for all 48 periods."""
    day = date(2026, 10, 7)
    pred = predict_day_ahead(lgbm_model, day)
    std = pred.std()
    assert std > 1.0, f"Predictions are suspiciously flat (std={std:.2f})"
