"""Two-price settlement (SPEC.md). DO NOT loosen."""
import pandas as pd
from src.backtest import settle

IDX = pd.date_range("2026-10-01", periods=2, freq="30min", tz="UTC")


def _plan(c, d):
    return pd.DataFrame({"charge_mw": c, "discharge_mw": d}, index=IDX)


def test_no_deviation_settles_at_mid_only():
    da = _plan([50, 0], [0, 50])
    pnl = settle(da, da, pd.Series([10.0, 100.0], index=IDX), pd.Series([999.0, -999.0], index=IDX))
    assert pnl.tolist() == [-250.0, 2500.0]  # 0.5h * MW * £


def test_deviation_settles_at_ssp():
    da = _plan([0, 0], [50, 0])
    rt = _plan([0, 0], [0, 0])  # cancelled the DA discharge
    pnl = settle(da, rt, pd.Series([100.0, 0.0], index=IDX), pd.Series([300.0, 0.0], index=IDX))
    # sold 25MWh at MID 100 (+2500), bought it back at SSP 300 (-7500)
    assert pnl.iloc[0] == -5000.0
