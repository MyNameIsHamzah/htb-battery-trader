"""The record contracts at the edge of the model service.

Two dataclasses and one validator. No pydantic: the repo convention is dataclasses
(see `src/model.py`) and stdlib is enough for four invariants.

The invariants are exactly the ones written in `INTERFACE.md` §2.1. A record that
fails any of them is DROPPED and counted — the service never guesses.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import math
from typing import Any

import pandas as pd

from src import config

# Version of the *record shape* (envelope + feature map), not of the model.
# Bump when a field is added/removed/retyped in INTERFACE.md.
FEATURE_SET_VERSION = 1

DEFAULT_ZONE = "GB"
UNIT = "GBP/MWh"


def _parse_ts(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


@dataclass(frozen=True)
class FeatureRecord:
    """One feature event: one settlement period, self-sufficient (INTERFACE.md §2.1)."""

    model_id: str
    feature_set_version: int
    prediction_key: str
    delivery_start: str
    as_of: str
    features: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "FeatureRecord":
        missing = [k for k in _REQUIRED_KEYS if k not in raw]
        if missing:
            raise ValueError(f"feature record missing keys: {missing}")
        features = raw["features"]
        if not isinstance(features, dict):
            raise ValueError("feature record 'features' must be an object")
        return cls(
            model_id=str(raw["model_id"]),
            feature_set_version=int(raw["feature_set_version"]),
            prediction_key=str(raw["prediction_key"]),
            delivery_start=str(raw["delivery_start"]),
            as_of=str(raw["as_of"]),
            features=dict(features),
        )

    @property
    def delivery_ts(self) -> pd.Timestamp:
        return _parse_ts(self.delivery_start)

    @property
    def as_of_ts(self) -> pd.Timestamp:
        return _parse_ts(self.as_of)


_REQUIRED_KEYS = (
    "model_id",
    "feature_set_version",
    "prediction_key",
    "delivery_start",
    "as_of",
    "features",
)


@dataclass(frozen=True)
class PredictionRecord:
    """One prediction event. Idempotent: replaying an input yields the same output."""

    model_id: str
    model_version: int
    feature_set_version: int
    prediction_key: str
    delivery_start: str
    as_of: str
    prediction: float
    unit: str = UNIT
    horizon: str = "day_ahead"
    quantiles: dict[str, float] = field(default_factory=dict)
    confidence: float = 0.0
    emitted_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        """The on-the-wire form (INTERFACE.md §2.2). Keys are stable; add to the end."""
        out: dict[str, Any] = {
            "model_id": self.model_id,
            "model_version": self.model_version,
            "feature_set_version": self.feature_set_version,
            "prediction_key": self.prediction_key,
            "delivery_start": self.delivery_start,
            "as_of": self.as_of,
            "prediction": float(self.prediction),
            "unit": self.unit,
            "horizon": self.horizon,
            "quantiles": {k: float(v) for k, v in self.quantiles.items()},
            "confidence": float(self.confidence),
            "emitted_at": self.emitted_at,
        }
        return out


def decision_cutoff(delivery_start: str | pd.Timestamp) -> pd.Timestamp:
    """Day-ahead decision time: DECISION_TIME UTC on D-1, for the delivery day D."""
    ts = _parse_ts(delivery_start) if isinstance(delivery_start, str) else delivery_start
    day = ts.tz_convert("UTC").normalize().date()
    return pd.Timestamp(datetime.combine(day - timedelta(days=1), config.DECISION_TIME, tzinfo=timezone.utc))


def validate_feature_record(
    record: FeatureRecord,
    *,
    required_features: list[str],
    feature_set_version: int = FEATURE_SET_VERSION,
) -> list[str]:
    """Return a list of human-readable contract violations (empty == record is good).

    Deliberately non-raising: the serve loop drops-and-counts, it does not crash the pod.
    """
    errors: list[str] = []

    if record.feature_set_version != feature_set_version:
        errors.append(
            f"feature_set_version {record.feature_set_version} != artifact's {feature_set_version}"
        )

    # 4. join-key integrity: "<zone>|<delivery_start>"
    key_ok = record.prediction_key.endswith("|" + record.delivery_start)
    if not key_ok or not record.prediction_key.split("|", 1)[0]:
        errors.append(
            f"prediction_key {record.prediction_key!r} does not match delivery_start {record.delivery_start!r}"
        )

    try:
        as_of = record.as_of_ts
        cutoff = decision_cutoff(record.delivery_start)
    except (ValueError, TypeError) as exc:
        errors.append(f"unparseable timestamp: {exc}")
        return errors

    # 2. no lookahead: strictly before 11:00 UTC on D-1
    if as_of >= cutoff:
        errors.append(f"lookahead: as_of {as_of.isoformat()} >= decision cutoff {cutoff.isoformat()}")

    # 3. schema: every required feature present, numeric and finite
    for col in required_features:
        if col not in record.features:
            errors.append(f"missing feature {col!r}")
            continue
        value = record.features[col]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            errors.append(f"feature {col!r} is not numeric: {value!r}")
        elif math.isnan(value) or math.isinf(value):
            errors.append(f"feature {col!r} is not finite: {value!r}")

    return errors
