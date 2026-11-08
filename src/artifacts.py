"""Model artifacts: what actually ships inside the container.

    artifacts/gb-price/v1/
        model.pkl        # pickled model object (a str for baselines, _LGBModel, ...)
        manifest.json    # provenance + version gate + metrics

The manifest is the version gate: `src.serve` refuses to start if the incoming
records' `feature_set_version` differs from the manifest's.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import pickle
import sys
from typing import Any

from src import config
from src.contracts import FEATURE_SET_VERSION
from src.model import BASELINES, train_day_ahead

MANIFEST = "manifest.json"
MODEL_FILE = "model.pkl"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save_artifact(
    model,
    out_dir: str | Path,
    *,
    model_name: str,
    model_id: str = "gb-price",
    model_version: int = 1,
    feature_set_version: int = FEATURE_SET_VERSION,
    train_start: date | None = None,
    train_end: date | None = None,
    horizon: str = "day_ahead",
    metrics: dict[str, Any] | None = None,
) -> Path:
    """Write model.pkl + manifest.json and return the directory."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    model_path = out / MODEL_FILE
    with model_path.open("wb") as fh:
        pickle.dump(model, fh, protocol=pickle.HIGHEST_PROTOCOL)

    feature_cols = list(getattr(model, "feature_cols", []) or [])
    manifest: dict[str, Any] = {
        "model_id": model_id,
        "model_name": model_name,
        "model_version": int(model_version),
        "feature_set_version": int(feature_set_version),
        "feature_cols": feature_cols,
        "horizon": horizon,
        "target": getattr(model, "target", "baseline" if isinstance(model, str) else None),
        "requires_day_context": getattr(model, "target", None) == "shape",
        "train_start": train_start.isoformat() if train_start else None,
        "train_end": train_end.isoformat() if train_end else None,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "sha256": _sha256(model_path),
        "metrics": metrics or {},
    }
    (out / MANIFEST).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return out


def load_artifact(artifact_dir: str | Path) -> tuple[Any, dict[str, Any]]:
    """Load (model, manifest). Raises on a missing/corrupt/half-written artifact."""
    d = Path(artifact_dir)
    manifest_path = d / MANIFEST
    model_path = d / MODEL_FILE
    for p in (manifest_path, model_path):
        if not p.exists():
            raise FileNotFoundError(f"artifact incomplete: {p} missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    actual = _sha256(model_path)
    if manifest.get("sha256") and manifest["sha256"] != actual:
        raise ValueError(f"artifact corrupt: {MODEL_FILE} sha256 {actual} != manifest {manifest['sha256']}")
    with model_path.open("rb") as fh:
        model = pickle.load(fh)
    if manifest.get("requires_day_context"):
        raise ValueError(
            f"{manifest.get('model_name')} needs the whole delivery day (cross-sectional "
            "re-centring) and cannot be served from one record — see PLAN.md M2/M4"
        )
    return model, manifest


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m src.artifacts", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    t = sub.add_parser("train", help="fit a model and write an artifact")
    t.add_argument("--model", default="lgbm", help=f"one of {sorted(BASELINES)} or lgbm*")
    t.add_argument("--out", required=True, help="e.g. artifacts/gb-price/v1")
    t.add_argument("--start", default=config.TRAIN_START.isoformat())
    t.add_argument("--end", default=config.TRAIN_END.isoformat())
    t.add_argument("--version", type=int, default=1)

    s = sub.add_parser("show", help="print an artifact manifest")
    s.add_argument("--dir", required=True)

    args = parser.parse_args(argv)

    if args.cmd == "train":
        start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
        model = train_day_ahead(args.model, start, end)
        out = save_artifact(
            model, args.out,
            model_name=args.model, model_version=args.version,
            train_start=start, train_end=end,
        )
        print(f"wrote {out}/{MODEL_FILE} + {MANIFEST}")
        return 0

    manifest = json.loads((Path(args.dir) / MANIFEST).read_text(encoding="utf-8"))
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(_cli())
