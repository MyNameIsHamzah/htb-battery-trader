"""The model service: consume feature records -> predict -> produce predictions.

Runs against any `Transport`:

    ReplayTransport  a JSONL file (or an in-memory list). No broker, no extra deps.
    KafkaTransport   confluent-kafka, imported lazily inside __init__, so the replay
                     path works on a machine where it is not installed.

This is the only runtime job of this repo. Feeds, Flink, topics and dashboards live
in the platform repo; the contract is INTERFACE.md.

    python -m src.serve --replay tests/fixtures/feature_stream.synthetic.jsonl
    python -m src.serve --self-check
    python -m src.serve --kafka --artifact artifacts/gb-price/v1
    python -m src.serve --replay in.jsonl --capture 10 --capture-out tests/fixtures/feature_stream.captured.jsonl
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass, field
import json
import logging
import os
from pathlib import Path
import sys
from typing import Iterable, Iterator, Protocol

from src.contracts import (
    FEATURE_SET_VERSION,
    FeatureRecord,
    PredictionRecord,
    validate_feature_record,
)
from src.model import FEATURE_COLS
from src.records import record_prediction

log = logging.getLogger("serve")


# --------------------------------------------------------------------------- transports
class Transport(Protocol):
    def consume(self) -> Iterator[bytes]: ...
    def produce(self, key: str, value: bytes) -> None: ...
    def ack(self, raw: bytes) -> None: ...
    def close(self) -> None: ...


class ReplayTransport:
    """Deterministic, broker-free stand-in: reads a JSONL stream, collects outputs."""

    def __init__(self, messages: Iterable[str | bytes],
                 produced: list[tuple[str, bytes]] | None = None):
        self._messages = messages
        self.produced: list[tuple[str, bytes]] = produced if produced is not None else []
        self.acked = 0

    @classmethod
    def from_path(cls, path: str | Path) -> "ReplayTransport":
        text = Path(path).read_text(encoding="utf-8")
        return cls([line for line in text.splitlines() if line.strip()])

    def consume(self) -> Iterator[bytes]:
        for raw in self._messages:
            yield raw.encode("utf-8") if isinstance(raw, str) else raw

    def produce(self, key: str, value: bytes) -> None:
        self.produced.append((key, value))

    def ack(self, raw: bytes) -> None:
        self.acked += 1

    def close(self) -> None:
        return None


class KafkaTransport:
    """Real transport. `confluent-kafka` is imported here and nowhere else."""

    def __init__(self, bootstrap: str, topic_in: str, topic_out: str, group: str,
                 poll_timeout: float = 1.0):
        from confluent_kafka import Consumer, Producer  # lazy: optional dependency

        self.topic_out = topic_out
        self.poll_timeout = poll_timeout
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
        })
        self.consumer.subscribe([topic_in])
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
        })

    def consume(self) -> Iterator[bytes]:
        while True:
            msg = self.consumer.poll(self.poll_timeout)
            if msg is None:
                continue
            if msg.error():
                err = msg.error()
                if getattr(err, "fatal", lambda: False)():
                    raise RuntimeError(f"kafka consumer fatal error: {err}")
                log.warning("kafka transient error: %s", err)
                continue
            yield msg.value()

    def produce(self, key: str, value: bytes) -> None:
        self.producer.produce(topic=self.topic_out, key=key.encode("utf-8"), value=value)
        self.producer.poll(0)

    def ack(self, raw: bytes) -> None:
        self.consumer.commit(asynchronous=True)

    def close(self) -> None:
        try:
            self.consumer.close()
        finally:
            self.producer.flush(10)


# --------------------------------------------------------------------------- loop
@dataclass
class Stats:
    consumed: int = 0
    produced: int = 0
    dropped: int = 0
    reasons: Counter = field(default_factory=Counter)
    last_error: str = ""

    def drop(self, reason: str, detail: str) -> None:
        self.dropped += 1
        self.reasons[reason] += 1
        self.last_error = detail
        log.warning("dropped record (%s): %s", reason, detail)

    def as_dict(self) -> dict:
        return {
            "consumed": self.consumed,
            "produced": self.produced,
            "dropped": self.dropped,
            "reasons": dict(self.reasons),
        }


def process_stream(
    transport: Transport,
    model,
    *,
    model_id: str = "gb-price",
    model_version: int = 1,
    feature_set_version: int = FEATURE_SET_VERSION,
    required_features: list[str] | None = None,
    limit: int | None = None,
    on_prediction=None,
) -> Stats:
    """The whole service: decode -> validate -> predict -> produce. 1 output per input."""
    required = required_features or FEATURE_COLS
    stats = Stats()

    for raw in transport.consume():
        stats.consumed += 1
        try:
            record = FeatureRecord.from_dict(json.loads(raw))
        except (ValueError, TypeError) as exc:
            stats.drop("malformed", str(exc))
        else:
            errors = validate_feature_record(
                record,
                required_features=required,
                feature_set_version=feature_set_version,
            )
            if errors:
                stats.drop("invalid", "; ".join(errors))
            else:
                prediction = record_prediction(
                    model, record,
                    model_id=model_id, model_version=model_version,
                )
                body = json.dumps(prediction.to_dict(), separators=(",", ":")).encode("utf-8")
                transport.produce(record.prediction_key, body)
                transport.ack(raw)
                stats.produced += 1
                if on_prediction is not None:
                    on_prediction(prediction)
        if limit is not None and stats.consumed >= limit:
            break

    return stats


def capture_stream(transport: Transport, out_path: str | Path, n: int) -> int:
    """Write the first `n` RAW messages verbatim — this is how a real topic becomes a fixture."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with out.open("w", encoding="utf-8") as fh:
        for raw in transport.consume():
            fh.write(raw.decode("utf-8").strip() + "\n")
            written += 1
            if written >= n:
                break
    return written


# --------------------------------------------------------------------------- self-check
def default_record() -> FeatureRecord:
    """A plausible record that needs no data files — used by --self-check/--probe.

    Hand-built on purpose: the image must be able to answer "am I healthy?" with no
    parquet, no fixture and no broker in the container.
    """
    return FeatureRecord(
        model_id="gb-price",
        feature_set_version=FEATURE_SET_VERSION,
        prediction_key="GB|2026-11-07T18:00:00Z",
        delivery_start="2026-11-07T18:00:00Z",
        as_of="2026-11-06T10:59:00Z",
        features={
            "hour": 18.0, "dow": 5.0, "is_weekend": 1.0,
            "hour_sin": -0.7071, "hour_cos": 0.7071,
            "price_lag_2d": 118.4, "price_lag_7d": 96.1, "price_last_known": 104.2,
            "mean_by_hour_14d": 121.7,
            "wind_forecast_mw": 8420.0, "demand_forecast_mw": 31100.0,
            "net_demand_mw": 22680.0,
            "spread_known": -17.5, "spread_lag2": -3.3,
        },
    )


def self_check(model=None, manifest: dict | None = None) -> int:
    """Validate + predict + serialise one record. Exit 0 == the pod can serve."""
    manifest = manifest or {}
    model = model if model is not None else "mean_by_hour_14d"
    record = default_record()

    errors = validate_feature_record(record, required_features=FEATURE_COLS)
    if errors:
        print(f"self-check FAILED: default record invalid: {errors}", file=sys.stderr)
        return 1

    prediction = record_prediction(
        model, record,
        model_id=manifest.get("model_id", "gb-price"),
        model_version=int(manifest.get("model_version", 0)),
    )
    body = prediction.to_dict()
    missing = set(PredictionRecord.__dataclass_fields__) - set(body)
    if missing:
        print(f"self-check FAILED: envelope missing {sorted(missing)}", file=sys.stderr)
        return 1
    if not isinstance(body["prediction"], float):
        print(f"self-check FAILED: prediction is not a float: {body['prediction']!r}", file=sys.stderr)
        return 1
    print(json.dumps(body, indent=2))
    print("self-check OK")
    return 0


# --------------------------------------------------------------------------- CLI
def _load_model(artifact_dir: str | None):
    """Return (model, manifest). Falls back to a baseline when no artifact is given."""
    if not artifact_dir:
        log.warning("no --artifact/$ARTIFACT_DIR -> using the mean_by_hour_14d baseline")
        return "mean_by_hour_14d", {
            "model_id": "gb-price",
            "model_name": "mean_by_hour_14d",
            "model_version": 0,
            "feature_set_version": FEATURE_SET_VERSION,
        }
    from src.artifacts import load_artifact

    return load_artifact(artifact_dir)


def _build_kafka_transport() -> KafkaTransport:
    env = os.environ
    return KafkaTransport(
        bootstrap=env["KAFKA_BOOTSTRAP"],
        topic_in=env.get("FEATURES_TOPIC", "features.gb-price.v1"),
        topic_out=env.get("PREDICTIONS_TOPIC", "predictions.gb-price.v1"),
        group=env.get("CONSUMER_GROUP", "model-gb-price-v1"),
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.serve",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--artifact", default=os.environ.get("ARTIFACT_DIR"),
                   help="artifact dir (default: $ARTIFACT_DIR; falls back to a baseline)")
    p.add_argument("--replay", help="JSONL feature stream to replay (no broker needed)")
    p.add_argument("--kafka", action="store_true",
                   help="consume from Kafka (needs confluent-kafka, see requirements-serve.txt)")
    p.add_argument("--out", help="also write predictions as JSONL here")
    p.add_argument("--limit", type=int, default=None, help="stop after N input records")
    p.add_argument("--capture", type=int, default=0,
                   help="write the first N raw input messages verbatim, then exit")
    p.add_argument("--capture-out", default="tests/fixtures/feature_stream.captured.jsonl")
    p.add_argument("--self-check", action="store_true",
                   help="validate + predict one synthetic record, exit 0/1 (image gate)")
    p.add_argument("--probe", action="store_true",
                   help="same as --self-check; for k8s readiness/liveness probes")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.self_check or args.probe:
        model, manifest = _load_model(args.artifact)
        return self_check(model, manifest)

    if not args.replay and not args.kafka:
        parser.error("choose --replay PATH or --kafka")

    model, manifest = _load_model(args.artifact)
    transport: Transport = (
        ReplayTransport.from_path(args.replay) if args.replay else _build_kafka_transport()
    )
    sink = None
    try:
        if args.capture:
            written = capture_stream(transport, args.capture_out, args.capture)
            print(json.dumps({"captured": written, "out": args.capture_out}, indent=2))
            return 0 if written else 1

        if args.out:
            sink_path = Path(args.out)
            sink_path.parent.mkdir(parents=True, exist_ok=True)
            sink = sink_path.open("w", encoding="utf-8")

        def on_prediction(prediction: PredictionRecord) -> None:
            if sink is not None:
                sink.write(json.dumps(prediction.to_dict(), separators=(",", ":")) + "\n")

        stats = process_stream(
            transport, model,
            model_id=manifest.get("model_id", "gb-price"),
            model_version=int(manifest.get("model_version", 0)),
            feature_set_version=int(manifest.get("feature_set_version", FEATURE_SET_VERSION)),
            limit=args.limit,
            on_prediction=on_prediction,
        )
        print(json.dumps(stats.as_dict(), indent=2))
        return 0 if stats.produced > 0 or stats.consumed == 0 else 1
    finally:
        if sink is not None:
            sink.close()
        transport.close()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
