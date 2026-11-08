"""Price forecasts. OWNER: B.

Contract: predict_day_ahead(model, day) -> pd.Series of 48 £/MWh, index = periods(day).
Baselines are implemented. LightGBM models trained on TRAIN_START..TRAIN_END.

Model names accepted by train_day_ahead:
  baselines         same_time_2_days_ago, same_time_last_week, mean_by_hour_14d
  lgbm              residual vs mean_by_hour_14d (learns level + shape corrections)
  lgbm_peak         as lgbm, peak periods up-weighted in training
  lgbm_direct       raw price level (shows the extrapolation failure)
  lgbm_shape        within-day SHAPE only: target = price - that day's mean price.
                    Forecast = predicted shape (re-centred to mean 0) + the daily
                    level of the 14d baseline. The battery mostly cares about WHEN
                    prices are high/low, so model capacity goes on ordering, not level.
  blend / blend_XX  mean_by_hour_14d + w * (lgbm's residual correction), w = XX/100
                    (default 0.5). Same as w*lgbm + (1-w)*baseline, i.e. shrinking the
                    model's corrections towards the baseline.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, timedelta
import numpy as np
import pandas as pd
import lightgbm as lgb

from src.features import day_ahead_features, periods
from src.data import load_prices

BASELINES = {
    "same_time_2_days_ago": "price_lag_2d",
    "same_time_last_week": "price_lag_7d",
    "mean_by_hour_14d": "mean_by_hour_14d",
}

FEATURE_COLS = [
    "hour", "dow", "is_weekend", "hour_sin", "hour_cos",
    "price_lag_2d", "price_lag_7d", "price_last_known",
    "mean_by_hour_14d", "wind_forecast_mw", "demand_forecast_mw",
    "net_demand_mw", "spread_known", "spread_lag2"
]

# Extra within-day features for lgbm_shape. Derived only from columns already in
# day_ahead_features (same row, same published_at), so no new lookahead risk.
SHAPE_EXTRA_COLS = ["mbh_shape", "net_demand_shape", "wind_shape"]
SHAPE_FEATURE_COLS = FEATURE_COLS + SHAPE_EXTRA_COLS

LGBM_NAMES = ("lgbm", "lgbm_peak", "lgbm_direct", "lgbm_shape")

# Peak hours in UTC (morning + evening ramp) used for peak-weighted variant
_PEAK_HOURS = set(range(7, 10)) | set(range(16, 21))

# Huber loss: LightGBM's `alpha` is the error size (£/MWh) where loss switches from
# squared to linear. Its default is 0.9, far below our residual spread (~£38 std), so
# every tree's step was clipped and corrections capped at about ±£10. We set it from
# the TRAINING targets: 1.345 x robust std (MAD x 1.4826), the standard Huber choice.
# Set HUBER_DELTA to a number to override, e.g. 0.9 + N_ESTIMATORS=400 = old behaviour.
HUBER_DELTA: float | None = None
N_ESTIMATORS = 2000  # early stopping on the validation window picks the real number


def _huber_delta(y: np.ndarray) -> float:
    if HUBER_DELTA is not None:
        return HUBER_DELTA
    mad = np.median(np.abs(y - np.median(y)))
    return float(1.345 * 1.4826 * mad)


@dataclass
class _LGBModel:
    name: str
    lgbm: lgb.LGBMRegressor
    feature_cols: list
    is_residual: bool = True
    target: str = "residual"  # "residual" | "level" | "shape"


@dataclass
class _BlendModel:
    name: str
    base: _LGBModel  # the residual lgbm whose corrections get shrunk
    weight: float    # 0 = pure baseline, 1 = pure lgbm


def _add_shape_features(f: pd.DataFrame) -> pd.DataFrame:
    """Within-day versions of key features: value minus that day's mean."""
    f = f.copy()
    f["mbh_shape"] = f["mean_by_hour_14d"] - f["mean_by_hour_14d"].mean()
    f["net_demand_shape"] = f["net_demand_mw"] - f["net_demand_mw"].mean()
    f["wind_shape"] = f["wind_forecast_mw"] - f["wind_forecast_mw"].mean()
    return f


def _parse_blend_weight(name: str) -> float | None:
    """'blend' -> 0.5, 'blend_30' -> 0.3, anything else -> None."""
    if name == "blend":
        return 0.5
    if name.startswith("blend_"):
        try:
            w = int(name.split("_", 1)[1]) / 100
        except ValueError:
            return None
        if 0.0 <= w <= 1.0:
            return w
    return None


def _build_dataset(start: date, end: date, weight_peaks: bool = False, residual: bool = True,
                   target: str | None = None, feature_cols: list | None = None):
    """Build (X, y, w) arrays from day_ahead_features over [start, end].

    target: "residual" (price - mean_by_hour_14d), "level" (raw price) or
    "shape" (price - that day's mean price). Defaults from `residual` for
    backwards compatibility.
    Returns (X: ndarray, y: ndarray, weights: ndarray | None).
    Rows with NaN target are dropped. LightGBM handles NaN features natively.
    """
    target = target or ("residual" if residual else "level")
    feature_cols = feature_cols or FEATURE_COLS
    prices = load_prices()["price"]
    Xs, ys, ws = [], [], []
    d = start
    while d <= end:
        f = day_ahead_features(d)
        if target == "shape":
            f = _add_shape_features(f)
        idx = f.index
        y = prices.reindex(idx)
        mask = ~y.isna()
        if target == "residual":
            mask = mask & ~f["mean_by_hour_14d"].isna()
        # shape needs the full day, otherwise the day's mean (and so the target) is biased
        min_rows = len(idx) if target == "shape" else 40
        if mask.sum() >= min_rows:
            Xs.append(f.loc[mask, feature_cols])
            if target == "residual":
                t = y[mask] - f.loc[mask, "mean_by_hour_14d"]
            elif target == "shape":
                t = y[mask] - y[mask].mean()
            else:
                t = y[mask]
            ys.append(t)
            if weight_peaks:
                w = np.where(idx[mask].hour.isin(_PEAK_HOURS), 2.5, 1.0)
                ws.append(w)
        d += timedelta(days=1)
    if not Xs:
        raise ValueError(f"No training data between {start} and {end}")
    X = pd.concat(Xs).to_numpy(dtype=float)
    y = np.concatenate(ys)
    weights = np.concatenate(ws) if weight_peaks else None
    return X, y, weights


def _train_lgbm(name: str, start: date, end: date) -> _LGBModel:
    # Time-based validation split: last 21 days of train window
    val_start = end - timedelta(days=20)
    target = {"lgbm_direct": "level", "lgbm_shape": "shape"}.get(name, "residual")
    is_residual = target != "level"
    feature_cols = SHAPE_FEATURE_COLS if target == "shape" else FEATURE_COLS
    weight_peaks = (name == "lgbm_peak")

    X_tr, y_tr, w_tr = _build_dataset(
        start, val_start - timedelta(days=1),
        weight_peaks=weight_peaks, target=target, feature_cols=feature_cols,
    )
    X_val, y_val, _ = _build_dataset(val_start, end, target=target, feature_cols=feature_cols)

    # Same hyperparameters for residual/shape, so comparisons isolate the target change
    params = dict(
        objective="huber" if is_residual else "regression",
        n_estimators=N_ESTIMATORS,
        learning_rate=0.03,
        num_leaves=24 if is_residual else 31,
        min_child_samples=25,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.5,
        reg_lambda=1.0,
        random_state=42,
        verbose=-1,
    )
    if is_residual:
        params["alpha"] = _huber_delta(y_tr)
    model = lgb.LGBMRegressor(**params)
    model.fit(
        X_tr, y_tr,
        sample_weight=w_tr,
        eval_X=X_val, eval_y=y_val,
        callbacks=[lgb.early_stopping(40, verbose=False), lgb.log_evaluation(-1)],
    )
    return _LGBModel(name=name, lgbm=model, feature_cols=feature_cols,
                     is_residual=is_residual, target=target)


def train_day_ahead(name: str, start: date, end: date):
    """Returns a model object. Baselines need no training."""
    if name in BASELINES:
        return name
    if name in LGBM_NAMES:
        return _train_lgbm(name, start, end)
    w = _parse_blend_weight(name)
    if w is not None:
        return _BlendModel(name=name, base=_train_lgbm("lgbm", start, end), weight=w)
    raise NotImplementedError(f"Unknown model: {name!r}")


def _baseline_forecast(f: pd.DataFrame, name: str) -> pd.Series:
    s = f[BASELINES[name]]
    return s.fillna(f["mean_by_hour_14d"]).fillna(f["price_last_known"])


def _lgbm_forecast(model: _LGBModel, f: pd.DataFrame, day: date) -> pd.Series:
    if model.target == "shape":
        f = _add_shape_features(f)
    X = f[model.feature_cols].to_numpy(dtype=float)
    pred = model.lgbm.predict(X)
    if model.target == "residual":
        pred = pred + f["mean_by_hour_14d"].to_numpy()
    elif model.target == "shape":
        level = np.nanmean(f["mean_by_hour_14d"].to_numpy())
        if np.isnan(level):
            level = float(f["price_last_known"].iloc[0])
        pred = pred - pred.mean() + level
    return pd.Series(pred, index=periods(day), name="forecast")


def predict_day_ahead(model, day: date) -> pd.Series:
    f = day_ahead_features(day)
    if isinstance(model, str) and model in BASELINES:
        return _baseline_forecast(f, model).rename("forecast")
    if isinstance(model, _LGBModel):
        return _lgbm_forecast(model, f, day)
    if isinstance(model, _BlendModel):
        base = _baseline_forecast(f, "mean_by_hour_14d").to_numpy()
        lgbm_pred = _lgbm_forecast(model.base, f, day).to_numpy()
        pred = model.weight * lgbm_pred + (1 - model.weight) * base
        return pd.Series(pred, index=periods(day), name="forecast")
    raise NotImplementedError(f"Unknown model type: {type(model)!r}")


def train_intraday(start: date, end: date):
    raise NotImplementedError("TODO(B) Phase 3")


def predict_intraday(model, t: pd.Timestamp) -> pd.Series:
    raise NotImplementedError("TODO(B) Phase 3")