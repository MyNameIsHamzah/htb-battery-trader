"""No-lookahead guard. DO NOT loosen."""
from datetime import date, timedelta
import pandas as pd
import pytest
from src.features import day_ahead_features, decision_time

DAYS = [date(2026, 10, 1) + timedelta(days=i) for i in range(0, 30, 7)]


@pytest.mark.parametrize("day", DAYS)
def test_features_published_before_decision(day):
    f = day_ahead_features(day)
    assert len(f) == 48
    assert (pd.to_datetime(f["published_at"]) <= decision_time(day)).all()


@pytest.mark.parametrize("day", DAYS)
def test_price_last_known_is_before_cutoff(day):
    f = day_ahead_features(day)
    assert f["price_lag_2d"].index.min() - pd.Timedelta(days=2) < decision_time(day)


def test_no_d_minus_1_afternoon_lag_feature():
    f = day_ahead_features(DAYS[0])
    assert "price_lag_1d" not in f.columns, "D-1 afternoon prices are unknown at 11:00 D-1"
