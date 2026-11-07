"""Price forecasts. OWNER: B.

Contract: predict_day_ahead(model, day) -> pd.Series of 48 £/MWh, index = periods(day).
Baselines are implemented. TODO(B): add LightGBM model "lgbm" trained on
TRAIN_START..TRAIN_END only, using day_ahead_features.
"""
from __future__ import annotations
from datetime import date
import pandas as pd
from src.features import day_ahead_features

BASELINES = {
    "same_time_2_days_ago": "price_lag_2d",
    "same_time_last_week": "price_lag_7d",
    "mean_by_hour_14d": "mean_by_hour_14d",
}


def train_day_ahead(name: str, start: date, end: date):
    """Returns a model object. Baselines need no training."""
    if name in BASELINES:
        return name
    raise NotImplementedError(f"TODO(B): model {name}")


def predict_day_ahead(model, day: date) -> pd.Series:
    f = day_ahead_features(day)
    if model in BASELINES:
        s = f[BASELINES[model]]
        s = s.fillna(f["mean_by_hour_14d"]).fillna(f["price_last_known"])
        return s.rename("forecast")
    raise NotImplementedError(f"TODO(B): predict for {model}")


def train_intraday(start: date, end: date):
    raise NotImplementedError("TODO(B) Phase 3")


def predict_intraday(model, t: pd.Timestamp) -> pd.Series:
    raise NotImplementedError("TODO(B) Phase 3")
