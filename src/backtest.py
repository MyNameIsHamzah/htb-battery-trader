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
from src.model import train_day_ahead, predict_day_ahead, predict_intraday
from src.optimise import schedule, naive_schedule


def settle(da: pd.DataFrame, rt: pd.DataFrame, mid: pd.Series, ssp: pd.Series) -> pd.Series:
    da_pos = da["discharge_mw"] - da["charge_mw"]
    rt_pos = rt["discharge_mw"] - rt["charge_mw"]
    return (C.PERIOD_H * (da_pos * mid + (rt_pos - da_pos) * ssp)).rename("pnl_gbp")


def _intraday_forecast(model, t: int, da_forecast: pd.Series, ssp: pd.Series) -> pd.Series:
    """Price forecast for the remaining periods (t..end) made at the start of
    period `t`, using only data published before `t`.

    Placeholder until B ships `predict_intraday`: the day-ahead MID forecast for
    the remaining periods (used as an SSP proxy), with the *current* period set
    to the latest known imbalance price (`ssp[t-1]`, published just before `t`).
    """
    idx = da_forecast.index
    fc = da_forecast.iloc[t:].astype(float).copy()
    if model is not None:
        try:
            fc2 = predict_intraday(model, idx[t])
        except NotImplementedError:
            fc2 = None
        if fc2 is not None and len(fc2) == len(fc):
            fc = fc2.reindex(idx[t:]).astype(float)
    if t > 0:
        fc.iloc[0] = float(ssp.iloc[t - 1])  # deviation settles at SSP
    return fc


def replan_day(da: pd.DataFrame, da_forecast: pd.Series, mid: pd.Series, ssp: pd.Series,
               soc0_mwh: float = C.SOC_START_MWH, cycles_left: float = C.MAX_CYCLES_PER_DAY,
               model=None, perfect_foresight: bool = False):
    """Day-ahead + real-time re-planning for one day (SPEC 'day_ahead_rt').

    At each period t: re-forecast the remaining horizon, re-solve `schedule()`
    from the current SoC with the remaining cycle budget, execute only period t.
    SoC and cycles_left are carried across re-plans; end-of-day SoC >= 50.

    `perfect_foresight=True` feeds actual remaining SSP (upper bound / test).
    Returns (rt, events): rt is the executed RT schedule; events are per-period
    dicts shaped for `src.replay` (ts, new_data, forecast_remaining, plan_before,
    plan_after, action, soc, pnl_so_far).
    """
    idx = da.index
    n = len(idx)
    soc = float(soc0_mwh)
    cyc = float(cycles_left)
    rows, events = [], []
    pnl_so_far = 0.0
    prev_plan = da  # DA commitment covers periods t..end (t=0: full day)
    for t in range(n):
        fc = ssp.iloc[t:].astype(float) if perfect_foresight else _intraday_forecast(model, t, da_forecast, ssp)
        plan = schedule(fc, soc0_mwh=soc, cycles_left=cyc, soc_end_min=C.SOC_START_MWH)
        c_t = float(plan.charge_mw.iloc[0])
        d_t = float(plan.discharge_mw.iloc[0])
        soc = float(plan.soc_mwh.iloc[0])
        cyc -= d_t * C.PERIOD_H / C.ENERGY_MWH
        da_pos = float(da.discharge_mw.iloc[t] - da.charge_mw.iloc[t])
        rt_pos = d_t - c_t
        pnl_so_far += C.PERIOD_H * (da_pos * float(mid.iloc[t]) + (rt_pos - da_pos) * float(ssp.iloc[t]))
        rows.append((c_t, d_t, soc))
        events.append({
            "ts": idx[t],
            "new_data": {"ssp_last": float(ssp.iloc[t - 1]) if t > 0 else None,
                         "mid_last": float(mid.iloc[t - 1]) if t > 0 else None},
            "forecast_remaining": fc,
            "plan_before": prev_plan.reindex(idx[t:]),
            "plan_after": plan,
            "action": {"charge_mw": c_t, "discharge_mw": d_t},
            "soc": soc,
            "pnl_so_far": pnl_so_far,
        })
        prev_plan = plan.iloc[1:]
    rt = pd.DataFrame(rows, columns=["charge_mw", "discharge_mw", "soc_mwh"], index=idx)
    return rt, events


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
            da_fc = predict_day_ahead(model, day)
            da = schedule(da_fc)
            rt, _ = replan_day(da, da_fc, mid, ssp, model=model)
        else:
            raise ValueError(strategy)
        if strategy != "day_ahead_rt":
            rt = da  # no deviation for these strategies
        df = rt.copy()
        df["mid"], df["ssp"] = mid, ssp
        df["pnl_gbp"] = settle(da, rt, mid, ssp)
        df["strategy"] = (f"{strategy}:{model_name}"
                          if strategy in ("day_ahead", "day_ahead_rt") else strategy)
        out.append(df)
        day += timedelta(days=1)
    return pd.concat(out)
