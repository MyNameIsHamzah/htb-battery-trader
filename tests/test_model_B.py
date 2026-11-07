"""Task B guard rails: lgbm returns 48 non-NaN; training never touches test."""
from datetime import date
import pytest

from src import config as C
from src.features import periods
import src.model as M
from src.model import ALL_MODELS, train_day_ahead, predict_day_ahead

D = date(2026, 10, 5)


@pytest.fixture(scope="module")
def lgbm_model():
    return train_day_ahead("lgbm", C.TRAIN_START, C.TRAIN_END)


@pytest.fixture(scope="module")
def lgbm_peak_model():
    return train_day_ahead("lgbm_peak", C.TRAIN_START, C.TRAIN_END)


def test_lgbm_predicts_48_non_nan(lgbm_model):
    s = predict_day_ahead(lgbm_model, D)
    assert len(s) == 48
    assert not s.isna().any()
    assert (s.index == periods(D)).all()


def test_lgbm_peak_predicts_48_non_nan(lgbm_peak_model):
    s = predict_day_ahead(lgbm_peak_model, D)
    assert len(s) == 48
    assert not s.isna().any()


def test_training_never_touches_test(monkeypatch):
    seen = []
    real = M.day_ahead_features

    def spy(day):
        seen.append(day)
        return real(day)

    monkeypatch.setattr(M, "day_ahead_features", spy)
    train_day_ahead("lgbm", C.TRAIN_START, C.TRAIN_END)
    assert seen, "training must build features"
    assert max(seen) < C.TEST_START


def test_all_models_registered(lgbm_model, lgbm_peak_model):
    assert set(ALL_MODELS) >= set(list(M.BASELINES) + list(M.LGBM_MODELS))
    for name in M.BASELINES:
        s = predict_day_ahead(name, D)
        assert len(s) == 48 and not s.isna().any(), name
    assert predict_day_ahead(lgbm_model, D).notna().all()
    assert predict_day_ahead(lgbm_peak_model, D).notna().all()
