"""Data access layer. OWNER: A.

Contract: every loader returns a DataFrame indexed by `ts` (UTC, period start,
30-min) with the columns listed below. Until real parquet files exist in data/,
SYNTHETIC data is returned so the whole pipeline runs from minute one.
"""
from __future__ import annotations
import warnings
from pathlib import Path
import numpy as np
import pandas as pd

DATA = Path(__file__).resolve().parent.parent / "data"
_START, _END = "2026-04-20", "2026-11-08"


def _synthetic() -> pd.DataFrame:
    rng = np.random.default_rng(0)
    ts = pd.date_range(_START, _END, freq="30min", tz="UTC", inclusive="left")
    h = ts.hour + ts.minute / 60
    wind = np.clip(8 + 6 * np.sin(np.arange(len(ts)) / 300) + rng.normal(0, 2, len(ts)), 0, 20)
    price = 80 + 35 * np.sin((h - 12) / 24 * 2 * np.pi) + 25 * np.exp(-((h - 18) ** 2) / 2) \
        - 4 * wind + rng.normal(0, 12, len(ts))
    ssp = price + rng.normal(0, 25, len(ts))
    demand = 25000 + 5000 * np.sin((h - 8) / 24 * 2 * np.pi)
    pub = ts - pd.Timedelta(days=1)
    return pd.DataFrame({
        "price": price,
        "ssp": ssp,
        "wind_forecast_mw": wind * 1000,
        "demand_forecast_mw": demand,
        "published_at": pub
    }, index=pd.Index(ts, name="ts"))


def _load(name: str, cols: list[str]) -> pd.DataFrame:
    p = DATA / f"{name}.parquet"
    if p.exists():
        df = pd.read_parquet(p)
        avail = [c for c in cols if c in df.columns]
        return df[avail] if avail else df
    warnings.warn(f"data/{name}.parquet missing -> using SYNTHETIC data", stacklevel=3)
    synth = _synthetic()
    avail = [c for c in cols if c in synth.columns]
    return synth[avail] if avail else synth


def load_prices() -> pd.DataFrame:
    """Columns: price (MID, APXMIDP, £/MWh). Zero-volume periods already DROPPED."""
    return _load("prices", ["price"])


def load_imbalance() -> pd.DataFrame:
    """Columns: ssp (system sell price, £/MWh)."""
    return _load("imbalance", ["ssp"])


def load_wind_forecast() -> pd.DataFrame:
    """Columns: wind_forecast_mw, published_at."""
    return _load("windfor", ["wind_forecast_mw", "published_at"])


def load_generation() -> pd.DataFrame:
    """Columns: fuel generation mix and published_at (5-min)."""
    p = DATA / "gen.parquet"
    if p.exists():
        return pd.read_parquet(p)
    warnings.warn("data/gen.parquet missing -> using SYNTHETIC data", stacklevel=3)
    synth = _synthetic()
    return pd.DataFrame({"wind": synth["wind_forecast_mw"], "published_at": synth["published_at"]}, index=synth.index)


def load_demand_forecast() -> pd.DataFrame:
    """Columns: demand_forecast_mw, published_at."""
    return _load("demand", ["demand_forecast_mw", "published_at"])


def is_synthetic() -> bool:
    return not (DATA / "prices.parquet").exists()
