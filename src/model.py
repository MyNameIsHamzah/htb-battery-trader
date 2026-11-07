"""Price forecasts. OWNER: B.

Contract: predict_day_ahead(model, day) -> pd.Series of 48 GBP/MWh, index = periods(day).
The LightGBM model "lgbm" (and peak-weighted "lgbm_peak") predict the RESIDUAL actual
price minus a leak-safe time-of-day baseline, then add the baseline back (full amplitude).
Deviation features (lag minus baseline) let the booster time the daily spread.
"""
from __future__ import annotations
from datetime import date, timedelta
import pandas as pd
import lightgbm as lgb

from src import config as C
from src.data import load_prices
from src.features import day_ahead_features

BASELINES = {
    "same_time_2_days_ago": "price_lag_2d",
    "same_time_last_week": "price_lag_7d",
    "mean_by_hour_14d": "mean_by_hour_14d",
}

# Raw numeric features from day_ahead_features (published_at excluded, not predictive).
FEATURE_COLUMNS = [
    "hour",
    "dow",
    "price_lag_2d",
    "price_lag_7d",
    "price_last_known",
    "mean_by_hour_14d",
    "wind_forecast_mw",
    "demand_forecast_mw",
]

LGBM_MODELS = ("lgbm", "lgbm_peak")
ALL_MODELS = list(BASELINES) + list(LGBM_MODELS)

# Battery-relevant windows (SPEC: naive charges 00:00-04:00, discharges 17:00-21:00).
# The peak-weighted variant upweights these hours so the fit favours the trough/peak
# spread the battery actually monetises (lower MAE need not mean more GBP).
PEAK_HOURS = set(list(C.NAIVE_CHARGE_HOURS) + list(C.NAIVE_DISCHARGE_HOURS))

_LGBM_PARAMS = {
    "objective": "regression",
    "metric": "mae",
    "learning_rate": 0.03,
    "num_leaves": 31,
    "min_data_in_leaf": 40,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "verbose": -1,
    "seed": 42,
    "deterministic": True,
}


def _design(f: pd.DataFrame) -> pd.DataFrame:
    """Raw features + same-slot deviations from the time-of-day baseline. The
    deviations predict the residual (price - baseline) via autocorrelation."""
    d = f[FEATURE_COLUMNS].copy()
    base = f["mean_by_hour_14d"]
    d["dev_lag2"] = f["price_lag_2d"] - base
    d["dev_lag7"] = f["price_lag_7d"] - base
    d["dev_last"] = f["price_last_known"] - base
    return d


def _training_frame(start: date, end: date):
    """X, y, w over start..end. y = price MINUS the per-(hour,minute) 14-day mean,
    a leak-safe time-of-day baseline, so the booster only learns the residual."""
    prices = load_prices()["price"]
    xs, ys, ws = [], [], []
    day = start
    while day <= end:
        f = day_ahead_features(day)
        base = f["mean_by_hour_14d"]
        y = prices.reindex(f.index) - base
        keep = y.notna().to_numpy()
        if not keep.any():
            day += timedelta(days=1)
            continue
        xs.append(_design(f.loc[keep]))
        ys.append(y[keep])
        w = pd.Series(1.0, index=f.index)
        w[f.index.hour.isin(PEAK_HOURS)] = 8.0
        ws.append(w[keep])
        day += timedelta(days=1)
    return pd.concat(xs), pd.concat(ys), pd.concat(ws)


def _train_lgbm(start: date, end: date, peak_weight: bool = False) -> dict:
    X, y, w = _training_frame(start, end)
    val_start = pd.Timestamp(end - timedelta(days=21), tz="UTC")
    tr = X.index < val_start
    va = ~tr
    weights = w if peak_weight else None
    train_set = lgb.Dataset(X[tr], label=y[tr],
                            weight=weights[tr] if weights is not None else None)
    valid_set = lgb.Dataset(X[va], label=y[va],
                            weight=weights[va] if weights is not None else None,
                            reference=train_set)
    callbacks = [lgb.early_stopping(50, first_metric_only=True, verbose=False)] if va.any() else []
    valid_sets = [valid_set] if va.any() else []
    booster = lgb.train(
        _LGBM_PARAMS,
        train_set,
        num_boost_round=2000,
        valid_sets=valid_sets,
        callbacks=callbacks,
    )
    return {"kind": "lgbm", "booster": booster, "features": list(X.columns)}


def train_day_ahead(name: str, start: date, end: date):
    """Returns a model object. Baselines need no training."""
    if name in BASELINES:
        return name
    if name in LGBM_MODELS:
        return _train_lgbm(start, end, peak_weight=(name == "lgbm_peak"))
    raise NotImplementedError(f"TODO(B): model {name}")


def predict_day_ahead(model, day: date) -> pd.Series:
    f = day_ahead_features(day)
    if isinstance(model, str) and model in BASELINES:
        s = f[BASELINES[model]]
        s = s.fillna(f["mean_by_hour_14d"]).fillna(f["price_last_known"])
        return s.rename("forecast")
    if isinstance(model, dict) and model.get("kind") == "lgbm":
        base = f["mean_by_hour_14d"].fillna(f["price_last_known"])
        resid = model["booster"].predict(_design(f)[model["features"]])
        return pd.Series(base + resid, index=f.index, name="forecast")
    raise NotImplementedError(f"TODO(B): predict for {model}")


def train_intraday(start: date, end: date):
    raise NotImplementedError("TODO(B) Phase 3")


def predict_intraday(model, t: pd.Timestamp) -> pd.Series:
    raise NotImplementedError("TODO(B) Phase 3")
