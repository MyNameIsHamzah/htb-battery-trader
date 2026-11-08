"""Train/serve skew: the served path and the lab path must agree EXACTLY.

`predict_day_ahead(model, day)[period]` is what every number in the pitch is built
from. `predict_from_record(model, record)` is what the pod runs. If these two ever
disagree, the container silently serves a different model than the one we validated.

So: same model, same period, both paths, same float. Plus a proof that the serve
path never touches the lab's parquet loaders.
"""
from datetime import date

import lightgbm as lgb
import numpy as np
import pytest

from src.contracts import validate_feature_record
from src.features import day_ahead_features
from src.model import (
    FEATURE_COLS,
    SHAPE_FEATURE_COLS,
    _BlendModel,
    _LGBModel,
    _lgbm_forecast,
    predict_day_ahead,
)
from src.records import (
    make_feature_record,
    predict_from_record,
    record_to_row,
)

DAY = date(2026, 10, 5)  # inside the test window, no NaNs in any feature


@pytest.fixture(scope="module")
def features():
    return day_ahead_features(DAY)


@pytest.fixture(scope="module")
def residual_model():
    """A tiny LightGBM: this test is about plumbing, not accuracy — keep it fast."""
    rng = np.random.default_rng(0)
    X = rng.normal(size=(600, len(FEATURE_COLS)))
    y = (
        0.5 * X[:, FEATURE_COLS.index("mean_by_hour_14d")]
        + 3.0 * X[:, FEATURE_COLS.index("price_lag_7d")]
        + rng.normal(scale=0.1, size=600)
    )
    regressor = lgb.LGBMRegressor(
        n_estimators=25, num_leaves=7, min_child_samples=5, verbose=-1, random_state=0
    ).fit(X, y)
    return _LGBModel(
        name="lgbm", lgbm=regressor, feature_cols=FEATURE_COLS,
        is_residual=True, target="residual",
    )


# --------------------------------------------------------------------------- skew
@pytest.mark.parametrize("name", ["same_time_2_days_ago", "same_time_last_week", "mean_by_hour_14d"])
def test_baseline_record_equals_lab(name, features):
    offline = predict_day_ahead(name, DAY)
    assert len(offline) == 48
    for ts, row in features.iterrows():
        served = predict_from_record(name, make_feature_record(row))
        assert served == pytest.approx(offline.loc[ts], abs=1e-9), f"{name} @ {ts}"


def test_residual_lgbm_record_equals_lab(residual_model, features):
    offline = _lgbm_forecast(residual_model, features, DAY)
    for ts, row in features.iterrows():
        served = predict_from_record(residual_model, make_feature_record(row))
        assert served == pytest.approx(offline.loc[ts], abs=1e-9), f"residual @ {ts}"


def test_level_lgbm_record_equals_lab(residual_model, features):
    level_model = _LGBModel(
        name="lgbm_direct", lgbm=residual_model.lgbm, feature_cols=FEATURE_COLS,
        is_residual=False, target="level",
    )
    offline = _lgbm_forecast(level_model, features, DAY)
    for ts, row in features.iterrows():
        served = predict_from_record(level_model, make_feature_record(row))
        assert served == pytest.approx(offline.loc[ts], abs=1e-9), f"level @ {ts}"


def test_blend_record_equals_lab(residual_model, features):
    blend = _BlendModel(name="blend_30", base=residual_model, weight=0.3)
    offline = predict_day_ahead(blend, DAY)
    for ts, row in features.iterrows():
        served = predict_from_record(blend, make_feature_record(row))
        assert served == pytest.approx(offline.loc[ts], abs=1e-9), f"blend @ {ts}"


# --------------------------------------------------------------------------- row shape
def test_record_to_row_is_the_lab_row(features):
    row = features.iloc[7]
    record = make_feature_record(row)
    back = record_to_row(record)

    assert list(back.columns) == FEATURE_COLS
    assert back.index[0] == row.name
    np.testing.assert_allclose(
        back.iloc[0].to_numpy(dtype=float), row[FEATURE_COLS].to_numpy(dtype=float), rtol=0, atol=0
    )
    assert validate_feature_record(record, required_features=FEATURE_COLS) == []


# --------------------------------------------------------------------------- no lab I/O
def test_serve_path_never_touches_the_data_layer(monkeypatch, features):
    """The pod has no parquet. If predicting a record reached for one, this fails."""
    import src.data
    import src.features
    import src.model

    record = make_feature_record(features.iloc[20])  # built while I/O was still allowed
    expected = predict_from_record("mean_by_hour_14d", record)

    def forbidden(*args, **kwargs):
        raise AssertionError("the serve path touched the lab data layer")

    monkeypatch.setattr(src.data, "_load", forbidden)          # the parquet choke point
    monkeypatch.setattr(src.model, "load_prices", forbidden)
    monkeypatch.setattr(src.features, "day_ahead_features", forbidden)
    monkeypatch.setattr(src.model, "day_ahead_features", forbidden)

    assert predict_from_record("mean_by_hour_14d", record) == expected


def test_shape_model_cannot_be_served_per_record(residual_model, features):
    """lgbm_shape re-centres across the day, so one record is not enough context."""
    shape_model = _LGBModel(
        name="lgbm_shape", lgbm=residual_model.lgbm, feature_cols=SHAPE_FEATURE_COLS,
        is_residual=True, target="shape",
    )
    record = make_feature_record(features.iloc[0])
    with pytest.raises(ValueError, match="not servable"):
        predict_from_record(shape_model, record)
