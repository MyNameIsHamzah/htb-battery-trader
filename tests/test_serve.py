"""The wire contract and the serve loop: 1 feature event in -> 1 prediction event out.

Everything here is broker-free: `ReplayTransport` stands in for Kafka, so the loop
that ships is the loop under test.
"""
from datetime import date
import json

import pytest

from src.artifacts import load_artifact, save_artifact
from src.contracts import FEATURE_SET_VERSION, PredictionRecord, validate_feature_record
from src.features import day_ahead_features
from src.model import FEATURE_COLS, _LGBModel
from src.records import make_feature_record
from src.serve import (
    ReplayTransport,
    capture_stream,
    default_record,
    main,
    process_stream,
    self_check,
)

DAY = date(2026, 10, 5)
MODEL = "mean_by_hour_14d"


@pytest.fixture(scope="module")
def records():
    day = day_ahead_features(DAY)
    return [make_feature_record(row) for _, row in day.iterrows()][:6]


def _payload(record, **overrides) -> str:
    """Wire form of a record, with optional vandalism for the negative tests."""
    body = {
        "model_id": record.model_id,
        "feature_set_version": record.feature_set_version,
        "prediction_key": record.prediction_key,
        "delivery_start": record.delivery_start,
        "as_of": record.as_of,
        "features": dict(record.features),
    }
    body.update(overrides)
    return json.dumps(body)


# --------------------------------------------------------------------------- happy path
def test_one_prediction_per_feature_event(records):
    transport = ReplayTransport([_payload(r) for r in records])
    stats = process_stream(transport, MODEL, model_id="gb-price", model_version=1)

    assert stats.as_dict() == {"consumed": 6, "produced": 6, "dropped": 0, "reasons": {}}
    assert [key for key, _ in transport.produced] == [r.prediction_key for r in records]
    assert transport.acked == 6


def test_prediction_envelope_matches_interface(records):
    transport = ReplayTransport([_payload(records[0])])
    process_stream(transport, MODEL, model_id="gb-price", model_version=3)

    key, body = transport.produced[0]
    out = json.loads(body)

    assert set(PredictionRecord.__dataclass_fields__) <= set(out)
    assert out["unit"] == "GBP/MWh"
    assert out["horizon"] == "day_ahead"
    assert out["model_id"] == "gb-price"
    assert out["model_version"] == 3
    assert out["as_of"] == records[0].as_of
    assert isinstance(out["prediction"], float)
    assert out["emitted_at"].endswith("Z")
    # the join key is unchanged end to end: downstream joins on it
    assert out["prediction_key"] == key == records[0].prediction_key
    assert out["prediction_key"].endswith("|" + out["delivery_start"])
    assert out["prediction_key"].split("|")[0] == "GB"


# --------------------------------------------------------------------------- bad input
def test_bad_records_are_dropped_not_fatal(records):
    good = records[0]
    missing = {k: v for k, v in good.features.items() if k != "wind_forecast_mw"}
    messages = [
        _payload(good),
        "{not json at all",                                        # malformed
        json.dumps({"model_id": "gb-price"}),                      # missing keys
        _payload(good, feature_set_version=99),                    # version gate
        _payload(good, as_of=good.delivery_start),                 # lookahead
        _payload(good, prediction_key="GB|2026-11-07T00:00:00Z"),  # join key
        _payload(good, features=missing),                          # missing feature
        _payload(good, features={**good.features, "hour": "eighteen"}),  # not numeric
    ]
    transport = ReplayTransport(messages)
    stats = process_stream(transport, MODEL, model_id="gb-price", model_version=1)

    assert stats.consumed == 8
    assert stats.produced == 1
    assert stats.dropped == 7
    assert stats.reasons["malformed"] == 2
    assert stats.reasons["invalid"] == 5
    assert len(transport.produced) == 1


def test_version_gate_drops_a_stale_stream(records):
    transport = ReplayTransport([_payload(r) for r in records])
    stats = process_stream(
        transport, MODEL, model_id="gb-price", model_version=1,
        feature_set_version=FEATURE_SET_VERSION + 1,
    )
    assert stats.produced == 0
    assert stats.dropped == len(records)
    assert "feature_set_version" in stats.last_error


def test_replay_is_idempotent(records):
    def run():
        transport = ReplayTransport([_payload(r) for r in records])
        process_stream(transport, MODEL, model_id="gb-price", model_version=1)
        out = [json.loads(body) for _, body in transport.produced]
        for row in out:
            row.pop("emitted_at")
        return out

    assert run() == run()


# --------------------------------------------------------------------------- fixture capture
def test_capture_writes_raw_messages_verbatim(tmp_path, records):
    lines = [_payload(r) for r in records]
    out = tmp_path / "captured.jsonl"

    written = capture_stream(ReplayTransport(lines), out, 2)

    assert written == 2
    assert out.read_text(encoding="utf-8").strip().splitlines() == lines[:2]


# --------------------------------------------------------------------------- CLI
def test_cli_replay_writes_predictions(tmp_path, records):
    src = tmp_path / "stream.jsonl"
    src.write_text("\n".join(_payload(r) for r in records) + "\n", encoding="utf-8")
    out = tmp_path / "predictions.jsonl"

    assert main(["--replay", str(src), "--out", str(out)]) == 0

    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == len(records)
    assert [r["prediction_key"] for r in rows] == [r.prediction_key for r in records]


def test_cli_needs_a_source():
    with pytest.raises(SystemExit):
        main([])


def test_self_check_passes_and_the_default_record_is_valid():
    assert validate_feature_record(default_record(), required_features=FEATURE_COLS) == []
    assert self_check() == 0


# --------------------------------------------------------------------------- artifacts
def test_artifact_round_trip(tmp_path):
    out = save_artifact(
        MODEL, tmp_path / "gb-price" / "v1",
        model_name=MODEL, model_version=1,
        train_start=date(2026, 5, 1), train_end=date(2026, 9, 30),
    )
    assert (out / "model.pkl").exists() and (out / "manifest.json").exists()

    model, manifest = load_artifact(out)

    assert model == MODEL
    assert manifest["model_version"] == 1
    assert manifest["feature_set_version"] == FEATURE_SET_VERSION
    assert manifest["horizon"] == "day_ahead"
    assert manifest["requires_day_context"] is False
    assert len(manifest["sha256"]) == 64


def test_corrupt_artifact_is_fatal(tmp_path):
    out = save_artifact(MODEL, tmp_path / "v1", model_name=MODEL, model_version=1)
    (out / "model.pkl").write_bytes(b"tampered")

    with pytest.raises(ValueError, match="corrupt"):
        load_artifact(out)


def test_incomplete_artifact_is_fatal(tmp_path):
    out = save_artifact(MODEL, tmp_path / "v1", model_name=MODEL, model_version=1)
    (out / "manifest.json").unlink()

    with pytest.raises(FileNotFoundError):
        load_artifact(out)


def test_shape_artifact_cannot_be_loaded_for_serving(tmp_path):
    shape_model = _LGBModel(name="lgbm_shape", lgbm=None, feature_cols=[], target="shape")
    out = save_artifact(shape_model, tmp_path / "v1", model_name="lgbm_shape", model_version=1)

    with pytest.raises(ValueError, match="whole delivery day"):
        load_artifact(out)


def test_artifact_version_gate_stops_a_mismatched_stream(tmp_path, records):
    out = save_artifact(
        MODEL, tmp_path / "v2", model_name=MODEL, model_version=2, feature_set_version=2
    )
    model, manifest = load_artifact(out)
    transport = ReplayTransport([_payload(r) for r in records])

    stats = process_stream(
        transport, model,
        model_id=manifest["model_id"], model_version=manifest["model_version"],
        feature_set_version=manifest["feature_set_version"],
    )

    assert stats.produced == 0
    assert stats.dropped == len(records)
    assert "feature_set_version 1 != artifact's 2" in stats.last_error
