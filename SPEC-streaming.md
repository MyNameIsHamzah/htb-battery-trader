# Battery Trader — Stream-to-Model Control Plane

**Status:** executable target spec (replaces the earlier "4 jobs J1–J4" demo framing).
**Scope of this doc:** the streaming platform. The batch pitch (`SPEC.md`, `src/`, `main.py`) is unchanged and frozen — `pytest -q` and `python main.py` must stay green.

The control plane turns the streaming stack from *"four hand-wired Flink jobs"* into **two first-class resources you declare**: a **Feed** and a **Model Query**. Everything else — topics, schemas, connector pipelines, Flink jobs, consumer groups, scaling — is derived from those two objects. The deadline demo is a **wind-first vertical slice** (`wind-demo`) that exercises the full path, with a second model (`gb-imbalance-price`) proving isolation.

---

## 0. What this is (and what changed)

The old framing described four fixed jobs (J1–J4) glued together by hand. That is not a platform: adding a model meant copying YAML, guessing topic names, and manually bumping Flink parallelism. This spec inverts the direction:

- **You declare intent.** A user POSTs a `Feed` ("poll Elexon wind forecasts every 60 s into a shared topic") or a `ModelQuery` ("build features for model `wind-demo` from these three feeds and write `features.wind-demo.v1`").
- **The control plane reconciles.** It creates the topic, registers the schema, deploys the Redpanda Connect pipeline, generates the Flink `CREATE TABLE` DDL from the input feed schemas, submits the job through the SQL Gateway, and records the resource in a catalogue.
- **Everything is versioned and addressable.** Feature and prediction topics are per-model and carry a `feature_set_version` / `model_version` in the envelope, so a model can evolve without stepping on its neighbour.

### 0.1 Non-negotiables

1. **Batch stays green.** `pytest -q -p no:warnings` and `python main.py` pass before and after any change. Do not touch `src/` contracts or `tests/`.
2. **No lookahead.** Every feature record carries an `as_of` timestamp no later than the data that produced it. Day-ahead-style features use only data published before 11:00 UTC on D-1.
3. **Feeds are shared, models are isolated.** Many models read the same `feeds.*` topics. Each model writes only to *its own* `features.<model>.v<n>` / `predictions.<model>.v<n>` topics, with its own consumer group and Flink job.
4. **Engine-aware scaling.** Scaling a model query means scaling its Flink task-managers (`FlinkDeployment.spec.taskManager.replicas`) — **not** a Kubernetes Deployment replica count. The control API hides that difference.
5. **Small diffs, run what you ship.** Don't claim a target works without running the command it wraps.

### 0.2 Stack decisions (pinned)

| Concern | Choice | Notes |
|---|---|---|
| Broker + schema registry | **Redpanda** (single broker, dev) | Kafka API + built-in Confluent-compatible Schema Registry on `:8081` |
| Ingestion | **Redpanda Connect** | One declarative pipeline per feed; JSON in, keyed JSON out |
| Stream processing | **Apache Flink** on Kubernetes (Operator + SQL Gateway) | Primary engine; `flink:1.19.1-scala_2.12-java11` |
| Control plane | **Thin FastAPI service** (`stream/control_api.py`) | Orchestrates the above; not a full CRD reconciler |
| Local cluster | **k3d** | `make up` is atomic and idempotent |
| Fallback | **docker compose** (Redpanda + a single Flink session cluster) | Only if the gate at 22:00 says the Operator path is at risk |

Pinned image tags (verify on first `make up`): `redpandadata/redpanda` (chart-managed), `flink-kubernetes-operator:1.9.0`, `flink:1.19.1-scala_2.12-java11`, `python:3.12-slim`.

---

## 1. Architecture

```
            external sources                       Redpanda                         Flink (Operator)                 outputs
 ┌───────────────────────────────┐      ┌──────────────────────────┐      ┌──────────────────────────┐      ┌────────────────────────┐
 │ Elexon BMRS REST (per feed)   │      │  feeds.<source>.<dataset>│      │  one FlinkDeployment     │      │ features.<model>.v1    │
 │   WINDFOR / demand / prices   │─────▶│  (shared, keyed JSON)    │─────▶│  per ModelQuery          │─────▶│ predictions.<model>.v1 │
 └──────────────┬────────────────┘      ├──────────────────────────┤      │  (generated DDL + SQL)   │      └───────────┬────────────┘
                │                        │  Schema Registry :8081   │      └───────────┬──────────────┘                  │
                │ Redpanda Connect       │  feed-* / feature-* /    │                  │ SQL Gateway :8083                    │
                ▼                        │  prediction-* subjects   │                  ▼                                      ▼
 ┌───────────────────────────────┐      └──────────────────────────┘      ┌──────────────────────────┐      ┌────────────────────────┐
 │ Control plane (FastAPI :8000) │────────────────────────────────────────▶│  Control plane submits   │      │ model consumer /        │
 │  POST /feeds                  │  create topic+schema+connect+deploy      │  statements over REST    │      │ dashboard tail          │
 │  POST /model-queries          │                                          └──────────────────────────┘      └────────────────────────┘
 │  GET  /catalog /metrics       │
 │  POST /model-queries/{id}/scale│
 └───────────────────────────────┘
```

Data path: **source → Connect → `feeds.*` → Flink model job → `features.<model>.v1` → model consumer → `predictions.<model>.v1`**.
Control path: **REST → control plane → (Redpanda Admin / Schema Registry / K8s API / Flink SQL Gateway)**.

## 2. Resources

Two JSON resources are the whole user-facing API. Keep these shapes stable — they are the contract between the user, the control plane, and the dashboard.

### 2.1 `Feed`

A **Feed** is a source dataset polled into a shared topic. One writer, many readers.

```json
{
  "id": "elexon-wind-forecast",
  "source": {
    "type": "rest",
    "url": "https://data.elexon.co.uk/bmrs/api/v1/datasets/WINDFOR",
    "poll_seconds": 60
  },
  "topic": "feeds.elexon.wind_forecast",
  "key_fields": ["region", "delivery_start"],
  "schema": {
    "subject": "feeds.elexon.wind_forecast-value",
    "format": "json",
    "version": 1
  }
}
```

Creating a feed triggers, in order:
1. **Create topic** `feeds.<source>.<dataset>` with `partitions = max(1, len(key_fields) + 1)` (default sizing; overridable).
2. **Register schema** — POST the discovered/declared JSON schema to the Schema Registry subject `feeds.elexon.wind_forecast-value`.
3. **Deploy the Connect pipeline** — render `deploy/connect/<id>.yaml` from a template and `kubectl apply` it (a ConfigMap + Deployment; see Section 5).
4. **Catalogue** — record the feed so `GET /catalog` and the dashboard can list it, and so `POST /model-queries` can resolve `inputs` → topic + schema.

### 2.2 `ModelQuery`

A **ModelQuery** is one model's feature (and optionally prediction) computation. It reads feeds, writes its own topics.

```json
{
  "model_id": "wind-demo",
  "version": 1,
  "inputs": ["elexon-wind-forecast", "elexon-demand-forecast", "elexon-system-price"],
  "output_topic": "features.wind-demo.v1",
  "sql": "INSERT INTO features_wind_demo SELECT 'wind-demo' AS model_id, 1 AS feature_set_version, ... FROM elexon_wind_forecast ...",
  "owner": "battery-trader"
}
```

`sql` may be given inline **or** as `sql_file` pointing at a checked-in `.sql` (the resolved artifact, e.g. `deploy/flink/wind_demo_query.sql`). `inputs` reference **feed IDs**, not raw topic names — the control plane resolves IDs to `topic` + `schema`.

Creating a model query triggers, in order:
1. **Resolve inputs** — feed ID → `{topic, schema_subject}` from the catalogue; fail fast if any input is unknown.
2. **Ensure output topics** — create `features.<model_id>.v<version>` (and `predictions.<model_id>.v<version>` if the query also emits predictions).
3. **Generate DDL** — for each input, emit a Flink `CREATE TABLE <feed_id>` with columns + watermark from the registered feed schema; append the author's `INSERT INTO`.
4. **Deploy the job** — create a `FlinkDeployment` with `job.parallelism` and `taskManager.replicas` from the request (default 1/1), then submit the generated SQL via the SQL Gateway (Section 7).
5. **Catalogue** — record the query with its `job_id`, `rest_job_id`, `consumer_group`, and current parallelism.

### 2.3 Versioned envelopes

Every message on a `features.*` / `predictions.*` topic is a **versioned envelope** so consumers can detect schema and lineage:

```json
{
  "model_id": "wind-demo",
  "feature_set_version": 1,
  "prediction_key": "GB|2026-11-07T18:00:00Z",
  "delivery_start": "2026-11-07T18:00:00Z",
  "as_of": "2026-11-07T15:42:00Z",
  "features": { "wind_forecast_mw": 8420.0, "demand_forecast_mw": 31100.0, "price_gbp_mwh": 71.4 }
}
```

- `feature_set_version` / `model_version` are mandatory — a consumer rejects a version it does not understand.
- `as_of` is the newest source timestamp folded into the record (enforces the no-lookahead invariant).
- `prediction_key` is the stable join key (`<zone>|<delivery_start>`) a downstream consumer dedupes on.

## 3. Topic & schema contracts

### 3.1 Topic naming

| Pattern | Purpose | Key | Partitions (default) |
|---|---|---|---|
| `feeds.<source>.<dataset>` | shared source topic, one writer | `key_fields` | `len(key_fields)+1`, min 1 |
| `features.<model>.v<n>` | per-model features | `prediction_key` | 3 |
| `predictions.<model>.v<n>` | per-model predictions | `prediction_key` | 1 |
| `control.audit` | control-plane action log | `resource_id` | 1 |

Concrete set for the demo (see `deploy/topics-job.yaml`): `feeds.elexon.wind_forecast`, `feeds.elexon.demand_forecast`, `feeds.elexon.system_price`, `feeds.elexon.fuelinst`, `feeds.elexon.imbalance`, `features.wind-demo.v1`, `predictions.wind-demo.v1`, `features.gb-imbalance-price.v1`, `predictions.gb-imbalance-price.v1`, `control.audit`.

### 3.2 The three versioned contracts

Register each as a JSON schema in the registry. Consumers code against `subject` + `version`.

**Feed record** — `feeds.*-value` (v1)

```json
{
  "type": "object",
  "required": ["event_time", "published_at", "region"],
  "properties": {
    "event_time":     { "type": "string", "format": "date-time" },
    "published_at":   { "type": "string", "format": "date-time" },
    "region":         { "type": "string" },
    "delivery_start": { "type": "string", "format": "date-time" },
    "forecast_mw":    { "type": "number" },
    "demand_mw":      { "type": "number" },
    "price_gbp_mwh":  { "type": "number" },
    "trade_type":     { "type": "string" }
  }
}
```

**Feature record** — `features.<model>.v<n>-value` (v1) — the envelope from §2.3, with a required `features` object whose properties are the model's feature set. Adding a feature is a **minor** version bump; removing/renaming one is **major**.

**Prediction record** — `predictions.<model>.v<n>-value` (v1)

```json
{
  "type": "object",
  "required": ["model_id", "model_version", "prediction_key", "as_of", "prediction"],
  "properties": {
    "model_id":     { "type": "string" },
    "model_version": { "type": "integer" },
    "prediction_key": { "type": "string" },
    "as_of":        { "type": "string", "format": "date-time" },
    "prediction":   { "type": "number" },
    "unit":         { "type": "string" }
  }
}
```

### 3.3 Compatibility policy

- **Backward compatible by default** — new optional fields and new features are minor bumps; consumers on older versions keep working.
- **Breaking changes** (renames, removals, type changes) require a **new major topic** `features.<model>.v<n+1>`; the old topic keeps serving until consumers migrate. Never mutate a `v<n>` contract in place.

## 4. Feed catalogue & sharing/isolation model

### 4.1 Catalogue (demo set)

| Feed ID | Source dataset | Topic | Key fields |
|---|---|---|---|
| `elexon-wind-forecast` | WINDFOR | `feeds.elexon.wind_forecast` | `region, delivery_start` |
| `elexon-demand-forecast` | INDO / demand forecast | `feeds.elexon.demand_forecast` | `region, delivery_start` |
| `elexon-system-price` | System prices | `feeds.elexon.system_price` | `event_time` |
| `elexon-fuelinst` | FUELINST (fuel mix, 5-min) | `feeds.elexon.fuelinst` | `fuel_type, event_time` |
| `elexon-imbalance` | Net imbalance / settlement | `feeds.elexon.imbalance` | `settlement_period` |

Wind + demand + price are the slice inputs. `fuelinst` and `imbalance` exist so the second model (`gb-imbalance-price`) can be declared with the *same* feeds and prove sharing + isolation without new ingestion work.

### 4.2 Sharing vs isolation

- **Shared (many readers, one writer):** all `feeds.*` topics. A feed is created **once**; any number of model queries read it. Adding a model never re-ingests data.
- **Isolated (one writer, its own everything):** each model query owns
  - its own **output topics** `features.<model>.v<n>` / `predictions.<model>.v<n>`,
  - its own **Flink job + FlinkDeployment** (`model-<model_id>`),
  - its own **Kafka consumer group** `mq-<model_id>-v<n>` on its inputs,
  - its own **checkpoint / savepoint state** under `s3://…/<model_id>` (dev: taskmanager-local).

Consequence: two models reading `feeds.elexon.wind_forecast` never interfere — different consumer groups, different jobs, different output topics. Scaling one model does not touch the other.

## 5. Ingestion — Redpanda Connect pipelines

One pipeline per feed, rendered as a ConfigMap + Deployment (`deploy/connect/<id>.yaml`). The control plane applies these when a feed is created; `make feeds` applies them all; `make feed-wind` applies just the slice.

### 5.1 The pattern

Elexon returns a single JSON object `{ "data": [ {...}, {...} ] }`. The canonical Connect recipe:

```yaml
pipeline:
  processors:
    - mapping: 'root = this.data'      # message becomes the array
    - unarchive:
        format: json_array             # one message per array element
    - mapping: |                       # shape each element into the feed contract
        root.event_time   = this.startTime
        root.published_at = this.publishTime
        root.region       = "GB"
        root.forecast_mw  = this.generation
output:
  kafka:
    addresses: ["redpanda-0.redpanda.redpanda.svc.cluster.local:9093"]
    topic: feeds.elexon.wind_forecast
    key: ${! json("region") }
    max_in_flight: 1
```

Key points verified against Redpanda Connect docs:
- `split` does **not** fan out a single message; use **`unarchive: {format: json_array}`** (or `json_documents`) — the extracted messages stay in one batch, which the `kafka` output writes as individual records.
- Poll throttle via a `rate_limit_resources` entry (`count: 1, interval: 60s`) referenced by the input's `rate_limit`, matching the feed's `poll_seconds`.
- The `kafka` output is deprecated in favour of the `redpanda` output in newer Connect builds; keep `kafka` now and migrate when the pinned image moves past 4.113.

### 5.2 Adding a feed

1. Add the feed to the catalogue (`POST /feeds`, or a checked-in manifest).
2. Copy `deploy/connect/wind_forecast.yaml` → `deploy/connect/<id>.yaml`; change the URL, the two `mapping` blocks, `topic`, and `key`.
3. `kubectl apply -f deploy/connect/<id>.yaml` (or let the control plane do it).
4. Confirm messages: `rpk topic consume <topic> -n 3`.

### 5.3 Fallback if a dataset is unavailable

If a REST dataset (e.g. BOD/BOALF for the imbalance model) has no clean endpoint, reuse **FUELINST** (already ingested) and derive the feature from fuel mix. The second model is deliberately chosen so its inputs are already covered by the slice's ingestion — no new connector is required to prove isolation.

## 6. Control plane service

Thin FastAPI app in `stream/control_api.py` (runtime: `deploy/apps/control-api.yaml`). It is an **orchestrator**, not a full CRD reconciler: it validates a request, performs the derived actions in order, and records the result. Stateless except for the catalogue (a small JSON file or a Redpanda `control.audit` topic).

### 6.1 Endpoints

| Method & path | Body / params | Effect |
|---|---|---|
| `GET /health` | — | liveness |
| `POST /feeds` | `Feed` | create topic + schema + Connect pipeline + catalogue |
| `GET /feeds` | — | list feeds |
| `POST /model-queries` | `ModelQuery` (or `sql_file`) | resolve inputs → gen DDL → deploy Flink job → catalogue |
| `GET /model-queries` | — | list model queries + status |
| `POST /model-queries/{id}/scale` | `?replicas=n` | engine-aware scale (below) |
| `GET /catalog` | — | feeds + model queries + topics + subjects |
| `GET /metrics` | — | per-job throughput / lag / parallelism |
| `POST /schemas` | — | register all three contracts (bootstrap) |

### 6.2 Engine-aware scaling (the load-bearing detail)

`POST /model-queries/wind-demo/scale?replicas=6` must **not** touch a Deployment's `replicas`. It patches the model's Flink workload:

```python
# pseudo — stream/control_api.py
def scale(model_id, replicas):
    fd = k8s.get_namespaced_custom_object(
        "flink.apache.org", "v1beta1", NAMESPACE, "flinkdeployments", f"model-{model_id}")
    fd["spec"]["taskManager"]["replicas"] = replicas
    fd["spec"]["job"]["parallelism"] = replicas
    k8s.patch_namespaced_custom_object(
        "flink.apache.org", "v1beta1", NAMESPACE, "flinkdeployments", f"model-{model_id}", fd)
    # Flink Operator reconciles task-manager pods; the running job picks up on resubmit.
```

The same call works regardless of engine because the control plane owns the translation: Flink → `FlinkDeployment.taskManager.replicas`; a compose/session fallback → `flink run -p <replicas>` against the session cluster. The user only ever names a model and a replica count.

### 6.3 Idempotency

- Creating an existing feed/model query is a no-op **plus** reconciliation (re-apply topic/pipeline/job), so `make up && make wind-demo` is safe to re-run.
- Deleting is out of scope for the demo (`DELETE` returns 501 with a note); tear down with `make down`.

## 7. Flink query generation & submission

### 7.1 Install the operator + SQL Gateway

```bash
helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.9.0/
helm install flink-operator flink-operator/flink-kubernetes-operator \
  -n flink --create-namespace \
  --set image.tag=1.9.0
kubectl apply -f deploy/flink/sql-gateway.yaml   # flink:1.19.1 session cluster + SQL Gateway on :8083
kubectl -n flink rollout status deploy/flink-sql-gateway --timeout=180s
```

### 7.2 Generate the query

For a `ModelQuery`, the control plane emits one `CREATE TABLE` per input feed (columns + watermark from the registered feed schema), then appends the author's `INSERT INTO`. For the slice this is checked in as the fully-resolved artifact **`deploy/flink/wind_demo_query.sql`** (3 source tables + 1 sink table + the `INSERT … SELECT`). That file is what a human reviews; the control plane regenerates the DDL portion so column drift can't sneak in.

### 7.3 Submit through the SQL Gateway

The "create a query" mechanism is the **Flink SQL Gateway REST API** (`flink:1.19.1`, `bin/sql-gateway.sh`, port 8083):

```bash
HOST=sql-gateway.battery.svc.cluster.local:8083
SID=$(curl -s -X POST http://$HOST/v1/sessions \
        -H 'content-type: application/json' \
        -d '{"properties":{"execution.runtime-mode":"streaming"}}' | jq -r .sessionHandle)

# submit every statement in the resolved .sql file, in order
python scripts/submit_sql.py deploy/flink/wind_demo_query.sql   # walks the file, POSTs each statement

# each statement: POST /v1/sessions/$SID/statements -> operationHandle, poll status
curl -s "http://$HOST/v1/sessions/$SID/statements/$OP" | jq -r .status
```

`scripts/submit_sql.py` is the thin client: it opens a session, splits the `.sql` on `;`, POSTs each statement, and waits for `FINISHED`/`RUNNING`/`ERROR`. The **`INSERT`** statement starts the streaming job and returns its Flink job id.

### 7.4 Per-model job identity

Each model query runs as FNV1a-stable named identifiers so the operator and the control plane agree on what to scale:
- `FlinkDeployment` name: `model-<model_id>` (e.g. `model-wind-demo`)
- Flink job name: `wind-demo-v<n>`
- Kafka consumer group: `mq-<model_id>-v<n>`

Restart/upgrade policy: let the operator's `restartNonce`/upgrade handle redeploys; never delete-and-recreate a running model job during the demo.

## 8. First vertical slice — `wind-demo`

Goal: **one feed → one model query → feature records on a topic**, end to end, reproducibly.

### 8.1 Run it

```bash
make bootstrap          # once: docker, kubectl, helm, k3d, rpk, images
make up                 # atomic: k3d cluster + Redpanda + schema registry + all topics
make deploy-apps        # control-api (needs stream/control_api.py built; see §6)

make feed-wind          # step 1: elexon-wind-forecast -> feeds.elexon.wind_forecast
make query-wind-demo    # step 2: register wind-demo -> features.wind-demo.v1
#   equivalently:
#   curl -X POST localhost:8000/model-queries --data @deploy/flink/wind_demo_model_query.json
#   or: python scripts/submit_sql.py deploy/flink/wind_demo_query.sql

make wind-demo          # convenience: feed-wind + query-wind-demo + consume output
```

### 8.2 What "done" looks like

- `rpk topic consume features.wind-demo.v1 -n 5` prints envelope records with `model_id=wind-demo`, `feature_set_version=1`, a `prediction_key`, an `as_of`, and the `features` object.
- `curl -s localhost:8000/catalog | jq` lists the feed, the model query, its topics, and its schema subjects.
- `curl -s localhost:8000/metrics | jq` shows the `wind-demo` job `RUNNING` with non-zero processed records.

### 8.3 Sequence (what runs where)

1. Connect `elexon-wind-forecast` polls WINDFOR → `feeds.elexon.wind_forecast`.
2. Control plane resolves `wind-demo`'s 3 inputs and submits `deploy/flink/wind_demo_query.sql` via the SQL Gateway.
3. The Flink job interval-joins wind ⋈ demand and temporal-joins the latest price → emits to `features.wind-demo.v1`.
4. A model consumer (or the dashboard) reads `features.wind-demo.v1` and, once a model is attached, writes `predictions.wind-demo.v1`.

### 8.4 Inspect

```bash
make status                       # pods + topics
make catalog                      # feed + model query catalogue
make metrics                      # per-job throughput / lag / parallelism
rpk topic consume features.wind-demo.v1 -n 5 --brokers $BROKER
```

## 9. Scaling & isolation demo

Two claims to prove live:

**A. A model query scales by replica count, engine-hidden.**
```bash
make scale JOB=wind-demo REPLICAS=6
# under the hood: patch FlinkDeployment model-wind-demo taskManager.replicas=6, job.parallelism=6
make metrics            # wind-demo processed-rate rises; task-manager count goes 1 -> 6
make scale JOB=wind-demo REPLICAS=1
```

**B. A second model shares the same feeds but stays isolated.**
```bash
curl -X POST localhost:8000/model-queries --data @deploy/flink/gb_imbalance_model_query.json
# -> features.gb-imbalance-price.v1, own job model-gb-imbalance-price, own group mq-gb-imbalance-price-v1
make status             # two independent jobs consuming the SAME feeds.* topics
```
Killing or scaling `wind-demo` must not affect `gb-imbalance-price`: different jobs, groups, and output topics.

## 10. Replay / historical backfill

Live Elexon polls are fine for "is it alive", but the demo needs throughput *now*. Provide a replay producer that pushes historical records into the `feeds.*` topics at a configurable speed — the model queries can't tell replay from live.

```bash
make replay SPEED=1000    # 1000x: ~1 h of history per 3.6 s
# stream/producer_replay.py: read historical parquet (src/fetch output) ->
# emit feed-contract records into feeds.elexon.* at SPEEDx wall-clock
```

Replay reads the same historical data the batch pipeline already fetches (`src/fetch.py` output), maps it to the feed contracts, and produces with `region`/`delivery_start` keys so partitioning matches live. It must never emit records with `published_at` in the future (no-lookahead survives replay).

## 11. Decision gates & timeline

A hard, time-boxed path so we always have something to show:

| Time | Gate | If it passes | If it fails |
|---|---|---|---|
| 21:30 | **Infra works** — `make up` brings up cluster + broker + topics; `rpk topic list` shows them | proceed | fix broker/topics only; Flink not yet in play |
| 22:00 | **Engine flip check** — Flink Operator + SQL Gateway health, OR switch to Quix/net.stream fallback | stay on Flink | flip to the compose/session fallback (single `flink run`) |
| 22:30 | **End-to-end** — `features.wind-demo.v1` receives records from the live/replayed feed | demo on Flink | fall back to `docker compose up` single-broker + single-session path (Section 13) |

Rules: at each gate, state pass/fail out loud; do not silently continue past a failed gate. The `demo` branch must always be the last known-good end-to-end state.

## 12. Repo layout additions

```
stream/
  control_api.py        # FastAPI: /feeds /model-queries /scale /catalog /metrics (Section 6)
  producer_replay.py    # historical -> feeds.* at SPEEDx (Section 10)
  model_consumer.py     # features.<model>.v1 -> predictions.<model>.v1 (optional, if a model is attached)
  contracts.py          # pydantic models for Feed / ModelQuery / envelopes
deploy/
  bootstrap.sh          # one-time VM tooling install
  redpanda-values.yaml  # 1-broker dev broker + built-in schema registry
  topics-job.yaml       # idempotent topic creation
  connect/wind_forecast.yaml   # first feed pipeline (template for others)
  flink/sql-gateway.yaml  wind_demo_query.sql  wind_demo_model_query.json
  apps/control-api.yaml        # ServiceAccount + RBAC + Deployment + Service
  gb_imbalance_model_query.json # second model (sharing/isolation proof)
  rbac.yaml            # cluster glue (operator, connect) if needed
docker-compose.yml     # fallback path (Section 13)
scripts/
  submit_sql.py        # SQL Gateway thin client (Section 7.3)
app/dashboard.py       # add a "Streaming" tab: feeds, model queries, live feature tail
Makefile               # make up / feed-wind / query-wind-demo / wind-demo / scale / replay ...
```

`app/dashboard.py` gets one new **Streaming** tab (read-only): catalogue tables, per-job status/throughput, and a live tail of the selected `features.<model>.v1` topic. No writes from the dashboard in the demo.

## 13. Dependencies & fallback

- **New Python deps** (declared in `requirements-stream.txt`): `fastapi`, `uvicorn`, `httpx`, `pydantic`. No `confluent-kafka`/`jsonschema`: the control plane shells out to `rpk` for topic/record I/O and registers raw JSON schemas over the Schema Registry REST API. Reuse existing `pandas`/`pyarrow` for replay. Everything in `src/` and `tests/` stays on the frozen dependency set — the streaming code lives in `stream/`.
- **Binary deps** (installed by `deploy/bootstrap.sh`): `docker`, `kubectl`, `helm`, `k3d`, `rpk`, plus `jq`.
- **Fallback (only if the 22:00 gate flips):** `docker compose up` runs one Redpanda + one Flink session cluster + the control API; `scripts/submit_sql.py` targets the session's SQL Gateway (`flink run -p`). Same `feeds.*` / `features.*` contracts, same ModelQuery SQL — only the *engine deployment* differs.

## 14. VERIFY checklist (run before declaring done)

```bash
# batch stays green (never regress the pitch numbers)
pytest -q -p no:warnings
python main.py

# infra
make up
rpk topic list --brokers $BROKER          # feeds.* + features.wind-demo.v1 present
curl http://redpanda-0.redpanda.redpanda.svc.cluster.local:8081/subjects   # schemas registered

# slice
make feed-wind
make query-wind-demo
rpk topic consume features.wind-demo.v1 -n 5 --brokers $BROKER            # envelopes, as_of present
curl -s localhost:8000/catalog | jq
curl -s localhost:8000/metrics | jq        # wind-demo RUNNING

# scaling + isolation
make scale JOB=wind-demo REPLICAS=6
make status                                # task-managers 1 -> 6
make scale JOB=wind-demo REPLICAS=1
```

Known-unknowables to confirm on first run (fix in place, don't assume):
- Redpanda chart schema-registry listener/port (`:8081`).
- Redpanda Connect image tag + entrypoint/args (`run /connect/<id>.yaml`).
- Elexon WINDFOR / demand / price field names ↔ the `mapping` blocks and `wind_demo_query.sql`.
- Flink Operator chart repo/tag and the `flinkdeployments` CRD group/version.

## Appendix — quick commands

```bash
# feeds
make feeds                       # apply all Connect pipelines
make feed-wind                   # apply just the wind feed
rpk topic consume feeds.elexon.wind_forecast -n 3 --brokers $BROKER

# model queries
make query-wind-demo             # control-api POST or submit_sql.py
make catalog                     # GET /catalog
make metrics                     # GET /metrics

# scale / teardown
make scale JOB=wind-demo REPLICAS=6
make down                        # k3d cluster delete battery (tears all)
```

Delete this whole stack with `make down`. The batch artefacts (`data/`, `src/`, `main.py`) are untouched by any of the above.










