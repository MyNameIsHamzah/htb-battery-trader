"""Feature tables. OWNER: A.  No-lookahead is enforced by tests/test_features.py.

Every feature row carries `published_at` = latest publish time of any input it
used. It MUST be < decision_time(day).
"""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
import pandas as pd
from src import config
from src.data import load_prices, load_wind_forecast


def decision_time(day: date) -> pd.Timestamp:
    """11:00 UTC on the day before `day`."""
    d = datetime.combine(day - timedelta(days=1), config.DECISION_TIME, tzinfo=timezone.utc)
    return pd.Timestamp(d)


def periods(day: date) -> pd.DatetimeIndex:
    return pd.date_range(pd.Timestamp(day, tz="UTC"), periods=48, freq="30min", name="ts")


def day_ahead_features(day: date) -> pd.DataFrame:
    """48 rows for `day`. Only data published before decision_time(day)."""
    cutoff = decision_time(day)
    p = load_prices()["price"]
    known = p[p.index + pd.Timedelta("30min") <= cutoff]  # periods FINISHED before cutoff
    idx = periods(day)
    lag2 = p.reindex(idx - pd.Timedelta(days=2)).to_numpy()
    lag7 = p.reindex(idx - pd.Timedelta(days=7)).to_numpy()
    hist = known[known.index >= cutoff - pd.Timedelta(days=14)]
    mean_by_hour = hist.groupby([hist.index.hour, hist.index.minute]).mean()
    mbh = [mean_by_hour.get((t.hour, t.minute), float("nan")) for t in idx]

    # TODO(A): wind forecast must be filtered on its own published_at, not assumed.
    wind = load_wind_forecast()["wind_forecast_mw"].reindex(idx).to_numpy()

    df = pd.DataFrame({
        "hour": idx.hour + idx.minute / 60,
        "dow": idx.dayofweek,
        "price_lag_2d": lag2,
        "price_lag_7d": lag7,
        "price_last_known": known.iloc[-1] if len(known) else float("nan"),
        "mean_by_hour_14d": mbh,
        "wind_forecast_mw": wind,
    }, index=idx)
    # latest period end actually used by price features
    df["published_at"] = known.index.max() + pd.Timedelta("30min") if len(known) else cutoff
    return df


def intraday_features(t: pd.Timestamp) -> pd.DataFrame:
    """Remaining periods of t's day, data published before t. TODO(A) — Phase 3."""
    raise NotImplementedError
