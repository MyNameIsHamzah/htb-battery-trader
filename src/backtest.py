"""Backtest + settlement. OWNER: C.

Settlement (SPEC.md): DA position settles at MID; deviation (RT - DA) at SSP.
pos = discharge_mw - charge_mw (MW). P&L = h*(DA_pos*MID + (RT_pos-DA_pos)*SSP)
"""
from __future__ import annotations
from datetime import date, timedelta
import pandas as pd
from src import config as C
from src.data import load_prices, load_imbalance
from src.features import periods
from src.model import train_day_ahead, predict_day_ahead
from src.optimise import schedule, naive_schedule


def settle(da: pd.DataFrame, rt: pd.DataFrame, mid: pd.Series, ssp: pd.Series) -> pd.Series:
    da_pos = da["discharge_mw"] - da["charge_mw"]
    rt_pos = rt["discharge_mw"] - rt["charge_mw"]
    return (C.PERIOD_H * (da_pos * mid + (rt_pos - da_pos) * ssp)).rename("pnl_gbp")


def run(strategy: str, start: date = C.TEST_START, end: date = C.TEST_END,
        model_name: str = "same_time_2_days_ago") -> pd.DataFrame:
    """strategy in {'naive','day_ahead','day_ahead_rt','perfect'}. Days with
    missing/!=48 periods are skipped (clock-change days, zero-volume gaps)."""
    mid_all = load_prices()["price"]
    ssp_all = load_imbalance()["ssp"]
    model = train_day_ahead(model_name, C.TRAIN_START, C.TRAIN_END) if strategy.startswith("day_ahead") else None
    out, day = [], start
    while day <= end:
        idx = periods(day)
        mid, ssp = mid_all.reindex(idx), ssp_all.reindex(idx)
        if mid.isna().any() or ssp.isna().any():
            day += timedelta(days=1); continue
        if strategy == "naive":
            da = naive_schedule(idx)
        elif strategy == "perfect":
            da = schedule(mid)
        elif strategy == "day_ahead":
            da = schedule(predict_day_ahead(model, day))
        elif strategy == "day_ahead_rt":
            raise NotImplementedError("TODO(C) Phase 3: re-plan each period, see SPEC")
        else:
            raise ValueError(strategy)
        rt = da  # no deviation for these strategies
        df = da.copy()
        df["mid"], df["ssp"] = mid, ssp
        df["pnl_gbp"] = settle(da, rt, mid, ssp)
        df["strategy"] = strategy if strategy != "day_ahead" else f"day_ahead:{model_name}"
        out.append(df)
        day += timedelta(days=1)
    return pd.concat(out)
