"""Stream-to-Model control plane — FastAPI orchestrator (SPEC-streaming.md §6).

    pip install -r requirements-stream.txt
    uvicorn stream.control_api:app --host 0.0.0.0 --port 8000

A *thin* orchestrator (not a full CRD reconciler). It validates a Feed or
ModelQuery request, performs the derived actions in order, and records the
result in a small JSON catalogue. It shells out to the binaries the bootstrap
already installs (`rpk`, `kubectl`) and talks to the Schema Registry + Flink SQL
Gateway over HTTP (`httpx`).

Env:
  NAMESPACE            k8s namespace for workloads           (default "battery")
  FLINK_NAMESPACE      namespace of the session cluster       (default "flink")
  BROKER               Kafka bootstrap endpoint               (default redpanda-0...:9093)
  SCHEMA_REGISTRY      Schema Registry base URL               (default ...:8081)
  FLINK_SQL_GATEWAY    SQL Gateway base URL                   (default http://sql-gateway:8083)
  CATALOG_PATH         where the catalogue JSON is persisted  (default outputs/catalog.json)
  CONTROL_API_DRY_RUN  "1" = log commands, don't execute      (default "0")

Scope notes (honest limits of this milestone):
  * Jobs run on the SQL Gateway's session cluster and are submitted via
    `scripts/submit_sql.py`. `POST /scale` patches a per-model FlinkDeployment
    (`model-<id>`) if one exists, otherwise it scales the shared session cluster
    and says so. Application-mode-per-model is a documented follow-up.
  * Feeds store their column schema so `POST /model-queries` can generate DDL.
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

NAMESPACE = os.environ.get("NAMESPACE", "battery")
FLINK_NAMESPACE = os.environ.get("FLINK_NAMESPACE", "flink")
BROKER = os.environ.get("BROKER", "redpanda-0.redpanda.redpanda.svc.cluster.local:9093")
SCHEMA_REGISTRY = os.environ.get(
    "SCHEMA_REGISTRY", "http://redpanda-0.redpanda.redpanda.svc.cluster.local:8081"
)
FLINK_SQL_GATEWAY = os.environ.get("FLINK_SQL_GATEWAY", "http://sql-gateway:8083")
DRY_RUN = os.environ.get("CONTROL_API_DRY_RUN", "0") == "1"
CATALOG_PATH = Path(
    os.environ.get("CATALOG_PATH", Path(__file__).resolve().parent.parent / "outputs" / "catalog.json")
)
REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------- resources ---
class FeedSource(BaseModel):
    type: str = "rest"
    url: str
    poll_seconds: int = 60


class FeedSchema(BaseModel):
    subject: str
    format: str = "json"
    version: int = 1


class Column(BaseModel):
    name: str
    sql_type: str = "STRING"


class Feed(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str
    source: FeedSource
    topic: str
    key_fields: list[str] = Field(default_factory=list)
    feed_schema: FeedSchema = Field(alias="schema")
    columns: list[Column] = Field(default_factory=list)
    manifest: str | None = None  # optional Connect manifest to kubectl apply


class ModelQuery(BaseModel):
    model_id: str
    version: int = 1
    inputs: list[str]
    output_topic: str | None = None
    sql: str | None = None       # inline author SQL (control plane generates DDL)
    sql_file: str | None = None  # OR a fully-resolved .sql artifact
    replicas: int = 1
    owner: str = "battery-trader"


# Fallback column schemas for the demo feeds (used when a Feed has no `columns`).
DEFAULT_COLUMNS = {
    "elexon-wind-forecast": [
        {"name": "event_time", "sql_type": "TIMESTAMP(3)"},
        {"name": "published_at", "sql_type": "TIMESTAMP(3)"},
        {"name": "region", "sql_type": "STRING"},
        {"name": "delivery_start", "sql_type": "TIMESTAMP(3)"},
        {"name": "forecast_mw", "sql_type": "DOUBLE"},
    ],
    "elexon-demand-forecast": [
        {"name": "event_time", "sql_type": "TIMESTAMP(3)"},
        {"name": "published_at", "sql_type": "TIMESTAMP(3)"},
        {"name": "region", "sql_type": "STRING"},
        {"name": "delivery_start", "sql_type": "TIMESTAMP(3)"},
        {"name": "demand_mw", "sql_type": "DOUBLE"},
    ],
    "elexon-system-price": [
        {"name": "event_time", "sql_type": "TIMESTAMP(3)"},
        {"name": "published_at", "sql_type": "TIMESTAMP(3)"},
        {"name": "price_gbp_mwh", "sql_type": "DOUBLE"},
    ],
    "elexon-fuelinst": [
        {"name": "event_time", "sql_type": "TIMESTAMP(3)"},
        {"name": "published_at", "sql_type": "TIMESTAMP(3)"},
        {"name": "fuel_type", "sql_type": "STRING"},
        {"name": "generation_mw", "sql_type": "DOUBLE"},
    ],
    "elexon-imbalance": [
        {"name": "event_time", "sql_type": "TIMESTAMP(3)"},
        {"name": "published_at", "sql_type": "TIMESTAMP(3)"},
        {"name": "settlement_period", "sql_type": "INT"},
        {"name": "net_imbalance_mw", "sql_type": "DOUBLE"},
    ],
}

# ---------------------------------------------------------------- helpers ---
def sh(cmd: list[str], check: bool = True) -> str:
    if DRY_RUN:
        print("[dry-run]", " ".join(cmd))
        return ""
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} -> {proc.returncode}: {proc.stderr.strip()}")
    return proc.stdout


def load_catalog() -> dict:
    if CATALOG_PATH.exists():
        return json.loads(CATALOG_PATH.read_text())
    return {"feeds": {}, "model_queries": {}}


def save_catalog(cat: dict) -> None:
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CATALOG_PATH.write_text(json.dumps(cat, indent=2))


def ensure_topic(topic: str, partitions: int) -> None:
    sh(["rpk", "topic", "create", topic, "-p", str(max(1, partitions)), "-r", "1",
        "--brokers", BROKER], check=False)


def register_schema(subject: str, schema: dict) -> None:
    if DRY_RUN:
        print(f"[dry-run] POST {SCHEMA_REGISTRY}/subjects/{subject}/versions")
        return
    with httpx.Client(timeout=15) as client:
        client.post(f"{SCHEMA_REGISTRY}/subjects/{subject}/versions",
                    json={"schema": json.dumps(schema)})


def feed_columns(feed: Feed) -> list[Column]:
    if feed.columns:
        return feed.columns
    return [Column(**c) for c in DEFAULT_COLUMNS.get(feed.id, [])]


def feed_record_schema(feed: Feed) -> dict:
    props = {}
    for c in feed_columns(feed):
        props[c.name] = {"type": "string" if c.sql_type == "STRING" else "number"}
    return {"type": "object", "properties": props}


def table_name(feed_id: str) -> str:
    return feed_id.replace("-", "_")


def create_table_ddl(feed: Feed, group: str) -> str:
    cols = feed_columns(feed)
    lines = [f"  {c.name} {c.sql_type}" for c in cols]
    if any(c.name == "published_at" for c in cols):
        lines.append("  WATERMARK FOR published_at AS published_at - INTERVAL '2' MINUTE")
    body = ",\n".join(lines)
    return (
        f"CREATE TABLE {table_name(feed.id)} (\n{body}\n) WITH (\n"
        f"  'connector' = 'kafka',\n"
        f"  'topic' = '{feed.topic}',\n"
        f"  'properties.bootstrap.servers' = '{BROKER}',\n"
        f"  'properties.group.id' = '{group}',\n"
        f"  'scan.startup.mode' = 'earliest-offset',\n"
        f"  'format' = 'json'\n);"
    )


def submit_sql(full_sql: str) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as fh:
        fh.write(full_sql)
        tmp = fh.name
    try:
        sh([sys.executable, str(REPO_ROOT / "scripts" / "submit_sql.py"), tmp,
            "--gateway", FLINK_SQL_GATEWAY])
    finally:
        Path(tmp).unlink(missing_ok=True)


def flinkdeployment_name(model_id: str) -> str:
    return f"model-{model_id}"


def _flinkdeployment_exists(name: str) -> bool:
    if DRY_RUN:
        return False
    out = sh(["kubectl", "-n", NAMESPACE, "get", "flinkdeployment", name,
              "-o", "name", "--ignore-not-found"], check=False)
    return bool(out.strip())


def _audit(action: str, resource_id: str) -> None:
    if DRY_RUN:
        return
    payload = json.dumps({"action": action, "resource": resource_id})
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        fh.write(payload)
        tmp = fh.name
    try:
        sh(["rpk", "topic", "produce", "control.audit", "-k", resource_id,
            "-z", "none", "--brokers", BROKER, tmp], check=False)
    finally:
        Path(tmp).unlink(missing_ok=True)

# --------------------------------------------------------------- endpoints ---
app = FastAPI(title="Battery Trader — Stream-to-Model control plane", version="1.0")


def resolve_sql(mq: ModelQuery, feeds: list[Feed]) -> str:
    """sql_file = a fully-resolved artifact; inline sql = author SQL (we add source DDL)."""
    if mq.sql_file:
        return Path(mq.sql_file).read_text()
    group = f"mq-{mq.model_id}-v{mq.version}"
    ddl = "\n\n".join(create_table_ddl(f, group) for f in feeds)
    return ddl + "\n\n" + (mq.sql or "")


@app.post("/feeds", status_code=201)
def create_feed(feed: Feed):
    cat = load_catalog()
    if feed.id in cat["feeds"]:
        raise HTTPException(409, f"feed {feed.id} already exists")
    ensure_topic(feed.topic, partitions=3)
    register_schema(feed.feed_schema.subject, feed_record_schema(feed))
    if feed.manifest:
        sh(["kubectl", "-n", NAMESPACE, "apply", "-f", feed.manifest])
    cat["feeds"][feed.id] = feed.model_dump(by_alias=True)
    save_catalog(cat)
    _audit("create_feed", feed.id)
    return {"id": feed.id, "topic": feed.topic, "schema": feed.feed_schema.subject}


@app.get("/feeds")
def list_feeds():
    return load_catalog()["feeds"]


@app.post("/model-queries", status_code=201)
def create_model_query(mq: ModelQuery):
    cat = load_catalog()
    if mq.model_id in cat["model_queries"]:
        raise HTTPException(409, f"model query {mq.model_id} already exists")
    feeds = []
    for fid in mq.inputs:
        if fid not in cat["feeds"]:
            raise HTTPException(400, f"unknown input feed {fid!r} (POST /feeds first)")
        feeds.append(Feed(**cat["feeds"][fid]))
    out_topic = mq.output_topic or f"features.{mq.model_id}.v{mq.version}"
    pred_topic = f"predictions.{mq.model_id}.v{mq.version}"
    ensure_topic(out_topic, partitions=3)
    ensure_topic(pred_topic, partitions=3)
    submit_sql(resolve_sql(mq, feeds))
    cat["model_queries"][mq.model_id] = {
        **mq.model_dump(),
        "output_topic": out_topic,
        "prediction_topic": pred_topic,
        "consumer_group": f"mq-{mq.model_id}-v{mq.version}",
    }
    save_catalog(cat)
    _audit("create_model_query", mq.model_id)
    return {"model_id": mq.model_id, "output_topic": out_topic,
            "prediction_topic": pred_topic, "submitted": True}


@app.get("/model-queries")
def list_model_queries():
    return load_catalog()["model_queries"]


@app.post("/model-queries/{model_id}/scale")
def scale_model_query(model_id: str, replicas: int = Query(..., ge=1)):
    """Engine-aware scaling: patch FlinkDeployment taskManager.replicas + job.parallelism."""
    cat = load_catalog()
    if model_id not in cat["model_queries"]:
        raise HTTPException(404, f"unknown model query {model_id}")
    name = flinkdeployment_name(model_id)
    patch = json.dumps(
        {"spec": {"taskManager": {"replicas": replicas}, "job": {"parallelism": replicas}}}
    )
    if _flinkdeployment_exists(name):
        sh(["kubectl", "-n", NAMESPACE, "patch", "flinkdeployment", name,
            "--type", "merge", "-p", patch])
        target = name
    else:
        # No per-model FlinkDeployment yet -> scale the shared session cluster.
        session_patch = json.dumps({"spec": {"taskManager": {"replicas": replicas}}})
        sh(["kubectl", "-n", FLINK_NAMESPACE, "patch", "flinkdeployment",
            "flink-sql-gateway", "--type", "merge", "-p", session_patch])
        target = "flink-sql-gateway (shared session)"
    cat["model_queries"][model_id]["replicas"] = replicas
    save_catalog(cat)
    _audit("scale_model_query", model_id)
    return {"model_id": model_id, "replicas": replicas, "engine_target": target}


@app.get("/metrics")
def metrics():
    out = {}
    for mid in load_catalog().get("model_queries", {}):
        name = flinkdeployment_name(mid)
        if DRY_RUN:
            out[mid] = {"state": "unknown", "target": name}
            continue
        raw = sh(["kubectl", "-n", NAMESPACE, "get", "flinkdeployment", name,
                  "-o", "json"], check=False)
        if not raw.strip():
            out[mid] = {"state": "absent", "target": name}
            continue
        doc = json.loads(raw)
        out[mid] = {
            "state": doc.get("status", {}).get("jobStatus", {}).get("state", "UNKNOWN"),
            "parallelism": doc.get("spec", {}).get("job", {}).get("parallelism"),
            "target": name,
        }
    return out


class SchemaRegistration(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    subject: str
    definition: dict = Field(alias="schema")


@app.post("/schemas", status_code=201)
def register_schema_endpoint(reg: SchemaRegistration):
    register_schema(reg.subject, reg.definition)
    return {"subject": reg.subject, "registered": True}


@app.get("/catalog")
def catalog():
    return load_catalog()


@app.get("/health")
def health():
    return {"status": "ok", "dry_run": DRY_RUN, "broker": BROKER,
            "schema_registry": SCHEMA_REGISTRY, "sql_gateway": FLINK_SQL_GATEWAY}




