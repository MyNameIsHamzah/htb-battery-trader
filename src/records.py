"""Records <-> the model's per-period input.

This module is the single place where a wire record becomes a feature row and back.
Training replays parquet through `make_feature_record`, serving decodes JSON through
`FeatureRecord.from_dict`; both then use the SAME `predict_from_record`, so
train/serve skew cannot appear from two divergent copies of this logic.

`src/model.py` is deliberately untouched: it still owns `predict_day_ahead` for the
offline lab, and this module reuses its internals for the per-record path.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from src.contracts import (
    DEFAULT_ZONE,
    FEATURE_SET_VERSION,
    FeatureRecord,
    PredictionRecord,
)
from src.model import (
    BASELINES,
    FEATURE_COLS,
    _BlendModel,
    _LGBModel,
    _baseline_forecast,
)


def record_to_row(record: FeatureRecord, feature_cols: list[str] | None = None) -> pd.DataFrame:
    """One-row DataFrame with the model's columns — the streaming twin of
    `day_ahead_features(day).loc[period]`."""
    cols = feature_cols or FEATURE_COLS
    values = {c: float(record.features[c]) for c in cols}
    return pd.DataFrame([values], index=pd.Index([record.delivery_ts], name="ts"))


def _lgb_row(model: _LGBModel, row: pd.DataFrame) -> float:
    """Per-period twin of `model._lgbm_forecast` (same arithmetic, one row)."""
    X = row[model.feature_cols].to_numpy(dtype=float)
    pred = float(model.lgbm.predict(X)[0])
    if model.target == "residual":
        pred += float(row["mean_by_hour_14d"].iloc[0])
    return pred


def predict_row(model, row: pd.DataFrame) -> float:
    """Predict one period from one feature row. Mirrors `predict_day_ahead` exactly."""
    if isinstance(model, str) and model in BASELINES:
        return float(_baseline_forecast(row, model).iloc[0])
    if isinstance(model, _LGBModel):
        if model.target == "shape":
            raise ValueError(
                "lgbm_shape is not servable per record: it re-centres the whole day "
                "(pred - mean(pred) + level), which needs the other 47 periods. "
                "It is also the only significantly worse model (PLAN.md M2)."
            )
        return _lgb_row(model, row)
    if isinstance(model, _BlendModel):
        base = float(_baseline_forecast(row, "mean_by_hour_14d").iloc[0])
        return model.weight * _lgb_row(model.base, row) + (1.0 - model.weight) * base
    raise NotImplementedError(f"Unknown model type: {type(model)!r}")


def predict_from_record(model, record: FeatureRecord) -> float:
    """The served entry point: one feature record in, one £/MWh out."""
    return predict_row(model, record_to_row(record))


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def record_prediction(
    model,
    record: FeatureRecord,
    *,
    model_id: str,
    model_version: int,
    horizon: str = "day_ahead",
) -> PredictionRecord:
    """Wrap a prediction in the output envelope (INTERFACE.md §2.2)."""
    value = predict_from_record(model, record)
    return PredictionRecord(
        model_id=model_id,
        model_version=model_version,
        feature_set_version=record.feature_set_version,
        prediction_key=record.prediction_key,
        delivery_start=record.delivery_start,
        as_of=record.as_of,
        prediction=value,
        horizon=horizon,
        emitted_at=_now_iso(),
    )


def _iso(ts: pd.Timestamp) -> str:
    return pd.Timestamp(ts).tz_convert("UTC").isoformat().replace("+00:00", "Z")


def make_feature_record(
    row: pd.Series,
    *,
    model_id: str = "gb-price",
    feature_set_version: int = FEATURE_SET_VERSION,
    zone: str = DEFAULT_ZONE,
) -> FeatureRecord:
    """Build a record from one row of `day_ahead_features(day)`.

    `row.name` is the period start; `row["published_at"]` becomes `as_of`
    (day_ahead_features already clamps it to before the decision cutoff).
    Used by the fixture generator and by the skew test — not by the serve loop.
    """
    period = pd.Timestamp(row.name)
    if period.tzinfo is None:
        period = period.tz_localize("UTC")
    published = row.get("published_at")
    if published is None or pd.isna(published):
        raise ValueError("row has no published_at; cannot derive as_of")
    as_of = pd.Timestamp(published)
    if as_of.tzinfo is None:
        as_of = as_of.tz_localize("UTC")
    features: dict[str, Any] = {}
    for col in FEATURE_COLS:
        value = row[col]
        features[col] = float(value) if isinstance(value, (int, float, np.floating, np.integer)) else value
    delivery = _iso(period)
    return FeatureRecord(
        model_id=model_id,
        feature_set_version=feature_set_version,
        prediction_key=f"{zone}|{delivery}",
        delivery_start=delivery,
        as_of=_iso(as_of),
        features=features,
    )
