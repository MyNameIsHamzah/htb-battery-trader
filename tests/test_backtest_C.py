"""Phase 3 re-planning guard rails (Task C). DO NOT loosen.

Covers: perfect-foresight RT >= day-ahead, deviations settle at SSP, cycle/SoC
limits are respected across re-plans, and the replay events are well-formed.
Data-independent: builds synthetic price/ssp series so it runs without A's parquet.
"""
import numpy as np
import pandas as pd
import pytest
from src import config as C
from src.backtest import settle, replan_day
from src.optimise import schedule

IDX = pd.date_range("2026-10-01", periods=48, freq="30min", tz="UTC")


def _series(seed):
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(80, 40, 48), index=IDX)


def _day(seed=0, perfect=False):
    mid = _series(seed)
    ssp = _series(seed + 100)
    da_fc = _series(seed + 200)
    da = schedule(da_fc)
    rt, events = replan_day(da, da_fc, mid, ssp, perfect_foresight=perfect)
    return da, rt, mid, ssp, events


def test_rt_perfect_info_beats_da():
    """With perfect SSP foresight the re-plan can always fall back to the DA plan,
    so it must earn at least the DA plan (within solver tolerance)."""
    for seed in range(3):
        da, rt, mid, ssp, _ = _day(seed=seed, perfect=True)
        pnl_rt = settle(da, rt, mid, ssp).sum()
        pnl_da = settle(da, da, mid, ssp).sum()
        assert pnl_rt >= pnl_da - 1e-3 * max(abs(pnl_da), 1.0), (seed, pnl_rt, pnl_da)


def test_deviation_pnl_settles_at_ssp():
    idx = pd.date_range("2026-10-01", periods=1, freq="30min", tz="UTC")
    da = pd.DataFrame({"charge_mw": [0.0], "discharge_mw": [50.0]}, index=idx)
    rt = pd.DataFrame({"charge_mw": [50.0], "discharge_mw": [0.0]}, index=idx)
    mid = pd.Series([100.0], index=idx)
    ssp = pd.Series([-50.0], index=idx)
    # DA pos = +50, RT pos = -50 -> deviation -100 MW*h. At SSP this is +5000;
    # at MID it would be -2500. Assert the deviation is priced at SSP.
    assert settle(da, rt, mid, ssp).iloc[0] == 5000.0


def test_cycles_and_soc_limits_never_exceed_across_replans():
    for seed in range(3):
        da, rt, mid, ssp, _ = _day(seed=seed)
        assert (rt.discharge_mw * C.PERIOD_H).sum() <= C.ENERGY_MWH * C.MAX_CYCLES_PER_DAY + 1e-6
        assert rt.soc_mwh.iloc[-1] >= C.SOC_START_MWH - 1e-6          # end-of-day SoC >= 50
        assert (rt.soc_mwh >= C.SOC_MIN_MWH - 1e-6).all()
        assert (rt.soc_mwh <= C.SOC_MAX_MWH + 1e-6).all()
        assert ((rt.charge_mw * rt.discharge_mw).abs() < 1e-6).all()  # never both at once
        # SoC dynamics stay consistent with executed actions across every re-plan
        prev = np.r_[C.SOC_START_MWH, rt.soc_mwh.to_numpy()[:-1]]
        expected = prev + C.ETA * C.PERIOD_H * rt.charge_mw - C.PERIOD_H / C.ETA * rt.discharge_mw
        assert np.allclose(rt.soc_mwh, expected, atol=1e-4)


def test_replay_events_are_well_formed_and_pnl_matches_settle():
    da, rt, mid, ssp, events = _day(seed=0)
    assert len(events) == 48
    keys = {"ts", "new_data", "forecast_remaining", "plan_before", "plan_after",
            "action", "soc", "pnl_so_far"}
    for e in events:
        assert keys <= set(e)
    # cumulative two-price P&L in the events equals settle() over the whole day
    assert events[-1]["pnl_so_far"] == pytest.approx(settle(da, rt, mid, ssp).sum(), rel=1e-9)


def test_intraday_forecast_has_no_lookahead():
    _, _, _, ssp, events = _day(seed=0)
    for t in range(1, 48):
        fc = events[t]["forecast_remaining"]
        assert len(fc) == 48 - t
        # current period uses only the SSP published just before t (not ssp[t])
        assert fc.iloc[0] == ssp.iloc[t - 1]
