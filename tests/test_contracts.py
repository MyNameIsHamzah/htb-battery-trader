"""Interface shapes every module must keep."""
from datetime import date
from src.features import periods
from src.model import predict_day_ahead, BASELINES
from src.backtest import run


def test_predict_shape():
    d = date(2026, 10, 5)
    for m in BASELINES:
        s = predict_day_ahead(m, d)
        assert len(s) == 48 and (s.index == periods(d)).all() and not s.isna().any()


def test_backtest_runs_and_has_columns():
    bt = run("naive", date(2026, 10, 1), date(2026, 10, 3))
    for col in ["charge_mw", "discharge_mw", "soc_mwh", "mid", "ssp", "pnl_gbp", "strategy"]:
        assert col in bt.columns


def test_perfect_beats_naive():
    a = run("naive", date(2026, 10, 1), date(2026, 10, 10))["pnl_gbp"].sum()
    b = run("perfect", date(2026, 10, 1), date(2026, 10, 10))["pnl_gbp"].sum()
    assert b >= a
