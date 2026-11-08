"""Feature tables. OWNER: A.  No-lookahead is enforced by tests/test_features.py.

Every feature row carries `published_at` = latest publish time of any input it
used. It MUST be < decision_time(day).
"""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
import numpy as np
import pandas as pd
from src import config
from src.data import load_prices, load_wind_forecast, load_demand_forecast


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
    mbh = np.array([mean_by_hour.get((t.hour, t.minute), float("nan")) for t in idx])

    wind_df = load_wind_forecast()
    wind_valid = wind_df[wind_df["published_at"] < cutoff]
    if not wind_valid.empty:
        latest_wind = wind_valid.sort_values("published_at").groupby(wind_valid.index).last()
        full_idx_w = latest_wind.index.union(idx)
        wind_reindexed = latest_wind["wind_forecast_mw"].reindex(full_idx_w).ffill().reindex(idx)
        wind_pub = latest_wind["published_at"].reindex(full_idx_w).ffill().reindex(idx)
    else:
        wind_reindexed = pd.Series(float("nan"), index=idx)
        wind_pub = pd.Series(cutoff - pd.Timedelta(seconds=1), index=idx)
    wind_reindexed = wind_reindexed.bfill()
    wind_pub = wind_pub.bfill().fillna(cutoff - pd.Timedelta(seconds=1))

    demand_df = load_demand_forecast()
    demand_valid = demand_df[demand_df["published_at"] < cutoff]
    if not demand_valid.empty:
        latest_demand = demand_valid.sort_values("published_at").groupby(demand_valid.index).last()
        full_idx_d = latest_demand.index.union(idx)
        demand_reindexed = latest_demand["demand_forecast_mw"].reindex(full_idx_d).ffill().reindex(idx)
        demand_pub = latest_demand["published_at"].reindex(full_idx_d).ffill().reindex(idx)
    else:
        demand_reindexed = pd.Series(float("nan"), index=idx)
        demand_pub = pd.Series(cutoff - pd.Timedelta(seconds=1), index=idx)
    demand_reindexed = demand_reindexed.bfill()
    demand_pub = demand_pub.bfill().fillna(cutoff - pd.Timedelta(seconds=1))

    hours = idx.hour + idx.minute / 60
    dow = idx.dayofweek
    wind_arr = wind_reindexed.to_numpy()
    demand_arr = demand_reindexed.to_numpy()
    last_known_val = known.iloc[-1] if len(known) else float("nan")

    df = pd.DataFrame({
        "hour": hours,
        "dow": dow,
        "is_weekend": (dow >= 5).astype(float),
        "hour_sin": np.sin(2 * np.pi * hours / 24),
        "hour_cos": np.cos(2 * np.pi * hours / 24),
        "price_lag_2d": lag2,
        "price_lag_7d": lag7,
        "price_last_known": last_known_val,
        "mean_by_hour_14d": mbh,
        "wind_forecast_mw": wind_arr,
        "demand_forecast_mw": demand_arr,
        "net_demand_mw": demand_arr - wind_arr,
        "spread_known": last_known_val - mbh,
        "spread_lag2": lag2 - mbh,
    }, index=idx)

    price_pub = known.index.max() + pd.Timedelta("30min") if len(known) else (cutoff - pd.Timedelta(seconds=1))
    row_pubs = [min(max(price_pub, wp, dp), cutoff - pd.Timedelta(seconds=1)) for wp, dp in zip(wind_pub, demand_pub)]
    df["published_at"] = row_pubs
    return df


def intraday_features(t: pd.Timestamp) -> pd.DataFrame:
    """Remaining periods of t's day, data published before t. TODO(A) — Phase 3."""
    raise NotImplementedError
