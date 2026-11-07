"""Replay engine for the dashboard demo. OWNER: C/D.

Streams a past day period-by-period, as if live, so task D can animate the
re-plan (SPEC.md 'Replay'). Each yielded event is a dict:
    {ts, new_data, forecast_remaining, plan_before, plan_after,
     action, soc, pnl_so_far}
"""
from __future__ import annotations
from datetime import date
import time
from typing import Iterator
from src import config as C
from src.data import load_prices, load_imbalance
from src.features import periods
from src.model import train_day_ahead, predict_day_ahead
from src.optimise import schedule
from src.backtest import replan_day

DEFAULT_MODEL = "same_time_2_days_ago"


def replay(day: date, speed: float = 60) -> Iterator[dict]:
    """Yield one event per period, as if live.

    speed is the playback multiplier: speed=60 -> a full day in ~24 min
    (each 30-min period plays in 30 s); speed=500 -> ~3 min for the pitch.
    """
    idx = periods(day)
    mid = load_prices()["price"].reindex(idx)
    ssp = load_imbalance()["ssp"].reindex(idx)
    if mid.isna().any() or ssp.isna().any():
        raise ValueError(f"replay({day}): missing prices/imbalance for one or more periods")

    model = train_day_ahead(DEFAULT_MODEL, C.TRAIN_START, C.TRAIN_END)
    da_fc = predict_day_ahead(model, day)
    da = schedule(da_fc)
    _, events = replan_day(da, da_fc, mid, ssp, model=model)

    pause = C.PERIOD_H * 3600.0 / speed if speed and speed > 0 else 0.0
    for event in events:
        yield event
        if pause:
            time.sleep(pause)
