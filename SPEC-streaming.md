# SPEC — Streaming platform (Redpanda + Flink on Kubernetes)

## Goal
Show that Battery Trader's pipeline is event-driven and **scales horizontally**: the dashboard can scale stream workers through the Kubernetes API while a high-volume replay is running, and throughput visibly increases.

Demo line: *"Live, Elexon publishes every 5 minutes, so we replay years of bid/offer data at 1000× to prove the pipeline. Watch throughput as we scale workers from the dashboard."*

## Non-negotiables
1. **`main` keeps working without Kubernetes.** All streaming work lives on branch `streaming` until it runs end to end.
2. **Checkpoint 22:30:** a message goes API → Redpanda → stream job → model → `results.plan` on the cluster. If not → fall back to `docker compose` (same images, same topics) and stop spending time on K8s.
3. **Never expose the kube API to the browser.** Frontend → backend (FastAPI) → K8s API via a ServiceAccount with RBAC limited to `scale` on 2 deployments in namespace `battery`.
4. Pre-pull all images on venue wifi *now*. Laptop needs ≥ 16 GB RAM; set small resource requests.

## Stack
| Layer | Choice | Why |
|---|---|---|
| Cluster | **k3d** (k3s in Docker), 1 server + 2 agents | Fastest local K8s, multi-node look |
| Broker | Redpanda (Helm chart, 1 broker, `tls`/`sasl` off) | Kafka-compatible, single binary |
| Stream processing | Flink via **Flink Kubernetes Operator**, Flink SQL jobs | SQL avoids JAR/PyFlink pain |
| Producers / consumers | Python (`confluent-kafka`) Deployments | Reuse existing `src/` code |
| Control API | FastAPI + `kubernetes` Python client | Scale endpoints for the frontend |
| Metrics | Redpanda consumer lag + msgs/s from Flink REST API | Throughput numbers for the demo |

**Fallback if Flink operator fights back past 22:00:** replace Flink jobs with Python consumers (Quix Streams) as Deployments. Same topics, same scaling story.

## Topics (partitions = max parallelism, so use ≥ 6 for the high-volume one)
| Topic | Key | Partitions | Source |
|---|---|---|---|
| `raw.prices.mid` | ts | 1 | MID |
| `raw.prices.imbalance` | ts | 1 | system prices |
| `raw.gen.fuelinst` | ts | 3 | FUELINST 5-min |
| `raw.forecast.wind` | target_ts | 1 | WINDFOR (with `published_at`) |
| `raw.bm.bod` | bmUnit | **12** | BOD/BOALF — the volume source |
| `features.v1` | ts | 3 | Job 2 output |
| `events.replan` | date | 1 | Job 3 output |
| `results.plan` | date | 1 | Model consumer output |

Message format: JSON, every message has `event_time` (data's own timestamp, UTC) and `published_at`.

## Components
| # | Component | Type | Does |
|---|---|---|---|
| P1 | `producer-live` | Deployment | Polls Elexon every 60 s, publishes new rows to `raw.*` |
| P2 | `producer-replay` | Job | Reads historical parquet, publishes to `raw.*` at `SPEED`× (env var), ordered by `published_at` |
| J1 | `gen-windows` | Flink SQL | `raw.gen.fuelinst` → 30-min tumbling windows by event_time |
| J2 | `feature-join` | Flink SQL | Joins prices + wind forecast + gen windows per period; **watermark on published_at** so a day-D row is only emitted after all data before 11:00 D-1 → `features.v1` |
| J3 | `bm-aggregates` | Flink SQL | `raw.bm.bod` → per-period bid/offer stack stats (volume, price percentiles). **This is the scaling showcase** |
| J4 | `replan-trigger` | Flink SQL | Imbalance / gen deviation beyond threshold → `events.replan` |
| C1 | `model-consumer` | Deployment | `features.v1` + `events.replan` → calls `src.model.predict_*` + `src.optimise.schedule` → `results.plan` |
| API | `control-api` | Deployment | `GET /metrics` (msgs/s, lag per topic), `POST /scale/{deployment}?replicas=n`, `POST /replay?speed=1000` |

## Tasks (2 people)
**S1 — Platform:** k3d cluster, Redpanda Helm, Flink operator, namespaces, RBAC, `make up` / `make down`, image pre-pull. Checkpoint 21:30: `rpk topic produce/consume` works in-cluster.

**S2 — Data path:** P1, P2, J1–J4 (Flink SQL files), C1, control API. Checkpoint 22:30: replay of 1 day flows to `results.plan`.

Both: one `Makefile`, everything in `deploy/` (manifests, Helm values) and `stream/` (Python, SQL). Don't touch `src/` signatures — call them.

## Repo additions
```
deploy/k3d.yaml  deploy/redpanda-values.yaml  deploy/flink/*.yaml  deploy/apps/*.yaml  deploy/rbac.yaml
stream/producer_live.py  stream/producer_replay.py  stream/model_consumer.py  stream/control_api.py
stream/sql/j1_gen_windows.sql  j2_feature_join.sql  j3_bm_aggregates.sql  j4_replan_trigger.sql
Makefile   docker-compose.yml (fallback)
```

## Acceptance
- `make up && make replay SPEED=1000` → `results.plan` receives plans; `GET /metrics` shows msgs/s.
- `POST /scale/bm-aggregates?replicas=6` → throughput on `raw.bm.bod` measurably increases (record before/after numbers for the slide).
- Kill a pod during replay → pipeline recovers (good 20-second demo moment).
- Feature rows in `features.v1` never have `published_at` > decision time (reuse test logic from `tests/test_features.py` on a sample of topic output).

## Pitch numbers to collect
Events/s at 1 vs 6 replicas · end-to-end latency (event → plan) · total events replayed · recovery time after pod kill.
