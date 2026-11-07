"""Guard rails for the optimiser. DO NOT loosen to make code pass."""
import numpy as np
import pandas as pd
import pytest
from src import config as C
from src.optimise import schedule, naive_schedule

IDX = pd.date_range("2026-10-01", periods=48, freq="30min", tz="UTC")


def _prices(seed=0):
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(60, 40, 48), index=IDX)


@pytest.mark.parametrize("seed", range(5))
def test_physical_limits(seed):
    s = schedule(_prices(seed))
    assert (s.charge_mw >= -1e-6).all() and (s.charge_mw <= C.POWER_MW + 1e-6).all()
    assert (s.discharge_mw >= -1e-6).all() and (s.discharge_mw <= C.POWER_MW + 1e-6).all()
    assert (s.soc_mwh >= C.SOC_MIN_MWH - 1e-6).all() and (s.soc_mwh <= C.SOC_MAX_MWH + 1e-6).all()
    assert (s.discharge_mw * C.PERIOD_H).sum() <= C.ENERGY_MWH * C.MAX_CYCLES_PER_DAY + 1e-6
    assert s.soc_mwh.iloc[-1] >= C.SOC_START_MWH - 1e-6


def test_no_simultaneous_charge_discharge_even_with_negative_prices():
    p = pd.Series(-200.0, index=IDX)  # LP would burn energy here
    s = schedule(p)
    assert ((s.charge_mw * s.discharge_mw).abs() < 1e-6).all()


def test_soc_dynamics_consistent():
    s = schedule(_prices(1))
    prev = np.r_[C.SOC_START_MWH, s.soc_mwh.to_numpy()[:-1]]
    expected = prev + C.ETA * C.PERIOD_H * s.charge_mw - C.PERIOD_H / C.ETA * s.discharge_mw
    assert np.allclose(s.soc_mwh, expected, atol=1e-4)


def test_buys_low_sells_high():
    p = pd.Series(np.where(IDX.hour < 6, 10.0, np.where(IDX.hour >= 17, 200.0, 80.0)), index=IDX)
    s = schedule(p)
    assert s.charge_mw[IDX.hour < 6].sum() > 0
    assert s.discharge_mw[IDX.hour >= 17].sum() > 0
    assert s.discharge_mw[IDX.hour < 6].sum() == 0


def test_flat_prices_do_nothing():
    s = schedule(pd.Series(50.0, index=IDX))
    assert s.charge_mw.sum() < 1e-6 and s.discharge_mw.sum() < 1e-6


def test_naive_rule_windows():
    s = naive_schedule(IDX)
    assert s.charge_mw[~IDX.hour.isin(list(C.NAIVE_CHARGE_HOURS))].sum() < 1e-6
    assert s.discharge_mw[~IDX.hour.isin(list(C.NAIVE_DISCHARGE_HOURS))].sum() < 1e-6
    assert s.discharge_mw.sum() > 0
