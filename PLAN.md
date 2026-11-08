# PLAN — battery-price-model (model-only repo)

**Status:** Phase 0 + 1 **executed** (`git tag pre-refocus`; 17 tracked files deleted;
`INTERFACE.md` + new `Makefile` + `.dockerignore` written; `pytest` 55 passed,
`python main.py` green, gate output identical to baseline). Phase 2 next.
Deviations from the plan as written: the 4 phase-2 `make` targets ship **commented out**
(no dead targets), and `src/replay.py` is retained pending sign-off (§3.1).
**Scope ruling:** this repo is the **price model and its objective function**. Feeds,
Flink queries, Redpanda, topic provisioning, control plane, dashboards live in the
**platform repo**. This repo's only runtime job: subscribe to the features topic,
predict, publish to the predictions topic, inside k8s.

Baseline being refactored: commit `76b38b6` + the 3-file build fix
(`ALL_MODELS`/`LGBM_MODELS` restored, dead block in `main.py` removed,
`scikit-learn` added to `requirements.txt`). `pytest` = **55 passed**,
`python main.py` = runs.

---

## 1. Decisions (answers to the open questions)

| # | Decision | Consequence |
|---|---|---|
| D1 | Upstream emits **one record per event** (one settlement period per message) | service is a **stateless 1:1 transform**: 1 input event → 1 prediction event. No day buffer in the default path. |
| D2 | Feeding the model is **someone else's job** | no `kubectl`/`rpk`/HTTP/Flink code survives here. One seam: two record contracts. |
| D3 | Features are **self-sufficient per record** (D1 + stateless ⇒ this is mandatory, not optional) | all 14 `FEATURE_COLS` already are row-level. Only 3 are day-context: `mbh_shape`, `net_demand_shape`, `wind_shape` (= value − day mean). They must be computed upstream (`avg(...) OVER (PARTITION BY delivery_day)`) **or** by the fallback cache in §1.1. |
| D4 | The LP/backtest stays in-repo as the **objective function** | "accurate" is undefined without £ on the 37 test days. `optimise.py`/`backtest.py`/`evaluate.py` are lab-only, never imported by the server. |
| D5 | Prediction volume is 1:1 with input; dedupe downstream | `prediction_key` is the join key, latest-wins. We do not buffer, re-emit, or window. |

### 1.1 The one piece of state we may need (fallback, off by default)

If the platform will not compute the 3 day-context columns upstream, add a bounded
day cache: `dict[(delivery_day) -> {period -> feature_row}]`, max 3 days, evict LRU.
On each event: update the slot, recompute the 3 day means over currently-known slots,
predict, emit **only the updated slot** plus a `completeness` field
(`n_slots_known/48`). Enabled only if `SERVE_DAY_CONTEXT=local`.
Cost: ~40 lines, makes pods stateful (state lost on rebalance — acceptable because
predictions are recomputed on the next update of that day, and late-ness is visible
in `completeness`). **Preference: push it upstream and never turn this on.**

---

## 2. `INTERFACE.md` — the only contract with the outside world

### 2.1 In: `features.gb-price.v<n>` (key = `prediction_key`)

Envelope per `SPEC-streaming.md` §2.3, unchanged:

```json
{ "model_id": "gb-price", "feature_set_version": 2,
  "prediction_key": "GB|2026-11-07T18:00:00Z",
  "delivery_start": "2026-11-07T18:00:00Z",
  "as_of": "2026-11-07T15:42:00Z",
  "features": { "hour": 18.0, "dow": 5.0, "is_weekend": 1.0, "hour_sin": -0.71,
                "hour_cos": 0.71, "price_lag_2d": 118.4, "price_lag_7d": 96.1,
                "price_last_known": 104.2, "mean_by_hour_14d": 121.7,
                "wind_forecast_mw": 8420.0, "demand_forecast_mw": 31100.0,
                "net_demand_mw": 22680.0, "spread_known": -17.5, "spread_lag2": -3.3,
                "mbh_shape": 12.4, "net_demand_shape": 3.1, "wind_shape": -820.0 } }
```

Invariants the server **enforces** (violation = drop + metric, never a guess):
1. `feature_set_version` == the loaded artifact's version → else **fail fast at boot**.
2. `as_of` ≤ `delivery_start − lead_time` (DA head: `as_of` < 11:00 UTC on D-1).
3. every key in `FEATURE_SCHEMA` present, numeric, not NaN where the model requires it.
4. `prediction_key` == `GB|<delivery_start>` (join-key integrity).

### 2.2 Out: `predictions.gb-price.v<n>` (key = `prediction_key`)

```json
{ "model_id": "gb-price", "model_version": 3, "feature_set_version": 2,
  "prediction_key": "GB|2026-11-07T18:00:00Z",
  "delivery_start": "2026-11-07T18:00:00Z", "as_of": "2026-11-07T15:42:00Z",
  "prediction": 141.2, "unit": "GBP/MWh", "horizon": "day_ahead",
  "quantiles": { "p10": 96.4, "p50": 141.2, "p90": 223.8 },
  "confidence": 0.62, "completeness": 1.0, "emitted_at": "2026-11-07T15:42:03Z" }
```

`prediction` = p50 until M3 lands the quantile heads; `confidence` is `p90−p10`
mapped to [0,1] and is what the RT edge filter and the optimiser consume.

### 2.3 Non-goals of the contract

We never emit schedules, SoC, or trade instructions. The optimiser is downstream.


---

## 3. Repo cleanup

### 3.1 Delete — platform/demo surface (~1,100 lines)

| Path | Lines | Reason |
|---|---|---|
| `stream/control_api.py` + `stream/` | 370 | control plane belongs to the platform repo (its own §0.3 handoff lists the *model consumer* as the piece that belongs here) |
| `scripts/submit_sql.py` | 168 | Flink SQL Gateway submission |
| `deploy/bootstrap.sh`, `redpanda-values.yaml`, `topics-job.yaml`, `connect/`, `flink/`, `apps/` | ~250 | k3d, Redpanda, topics, Connect pipelines, SQL Gateway |
| `Makefile` | 105 | every target is `k3d`/`rpk`/`flink`; replaced by §3.4 |
| `requirements-stream.txt` | 4 | fastapi/uvicorn/httpx — no HTTP surface survives |
| `SPEC-streaming.md` | 35 KB | moves to platform repo; replaced by `INTERFACE.md` (§2) |
| `app/dashboard.py` | 30 | demo surface; replaced by `outputs/report.md` from `evaluate.py` |
| `src/dashboard.html` | — | stray committed artifact ("added dash html temp") |
| `tasks/{A-data,B-model,C-optimise,D-dashboard}.md` | — | obsolete role split; fold into `AGENTS.md`, keep `R-rigour.md` as the review checklist |

**Needs human sign-off (test-deletion rule):** `src/replay.py` (43 lines) is the demo
replay engine; deleting it orphans `tests/test_backtest_C.py::test_replay_events_are_well_formed_and_pnl_matches_settle`.
Recommendation: move `replay.py` + that one test **as a pair** to the demo repo.
Otherwise it stays as-is (harmless, 43 lines).

### 3.2 Keep, re-scoped

| Path | New role |
|---|---|
| `src/model.py` | the model (contracts frozen) |
| `src/features.py` | **feature contract** + point-in-time builder; gains `FEATURE_SCHEMA` (name/dtype/unit/row-or-day) enforced by §2.1 |
| `src/optimise.py`, `src/backtest.py`, `src/evaluate.py` | the £ objective + the ruler (lab only, off the serve import path) |
| `src/data.py`, `src/fetch.py` | lab data (excluded from the image by `.dockerignore`) |
| `src/config.py`, `tests/`, `scripts/peek.py`, `SPEC.md` | keep; trim `SPEC.md` to model + eval protocol |

### 3.3 Add (~400 lines)

| File | Responsibility |
|---|---|
| `src/contracts.py` | `FeatureRecord`, `PredictionRecord` (pydantic), version gate, `as_of`/join-key validation |
| `src/records.py` | `record -> one-row DataFrame` and `prediction -> record`. **Shared by `train` and `serve`** — the train/serve-skew guard |
| `src/serve.py` | consume → validate → `predict_day_ahead` → produce; metrics; SIGTERM drain |
| `src/artifacts.py` | `save/load models/<model_id>/v<N>/` + `manifest.json` (sha256, train window, metrics, `feature_set_version`) |
| `deploy/model/Dockerfile`, `deployment.yaml`, `configmap.yaml` | 2 replicas, probes, env-only config, read-only artifacts volume |
| `tests/test_contracts_io.py`, `test_serve.py`, `test_records_skew.py`, `fixtures/feature_record.json` | the fixture is copied from the platform repo — non-negotiable, it is how skew is caught |

### 3.4 New `Makefile`

```
make test         # pytest -q -p no:warnings
make fixture      # python scripts/make_fixture.py  (synthetic feature stream + manifest)
make train        # python -m src.artifacts train --model lgbm --out artifacts/gb-price/v1
make serve-local  # python -m src.serve --replay tests/fixtures/feature_stream.synthetic.jsonl
make self-check   # python -m src.serve --self-check   (the image build gate)
make image        # docker build -f deploy/model/Dockerfile -t battery/price-model:$TAG .
make eval         # python -m src.evaluate -> outputs/report.md + leaderboard.csv  (phase 3)
```

### 3.5 Target tree

```
AGENTS.md          # rewritten: model-only rules
INTERFACE.md       # in/out records (§2)
PLAN.md            # this file
SPEC.md            # trimmed: model + evaluation protocol
src/{config,data,fetch,features,model,optimise,backtest,evaluate}.py   # lab
src/{contracts,records,serve,artifacts}.py                             # service
tests/             # existing + tests/test_{records_skew,serve}.py + fixtures/
deploy/model/      # Dockerfile + deployment.yaml + configmap.yaml
scripts/peek.py
scripts/make_fixture.py   # synthetic feature stream + manifest.json (sha256/provenance)
requirements.txt          # lab + service (no broker client)
requirements-serve.txt    # confluent-kafka only, for src/serve.py --kafka
```

---

## 4. Modelling backlog (each item = 1 commit + 1 measured delta)

**Promotion rule for every item:** ships only if the paired-bootstrap **95% CI lower
bound on daily £ > 0** vs `mean_by_hour_14d`. Never a point estimate — the noise floor
is **±£23k at 1σ** on 37 days (daily £ std £3,761, SE £618/day × √37).

### M1 — Fix the ruler first (S, blocks everything else)
`evaluate.py` headline becomes `{total_£, %_of_perfect, rank_IC, spread_capture,
top-k hit rate, bootstrap CI}`; MAE demoted to a sanity column. Emits
`outputs/leaderboard.csv` + `outputs/report.md`.
*Why this is the right ruler:* MAE↔£ r = **+0.04**, rank IC↔£ r = **+0.44**;
`perfect SHAPE + baseline LEVEL` = **100.0%** of perfect at MAE 36.5, whereas
`perfect LEVEL + baseline SHAPE` = **73.5%** at MAE 21.0.

### M2 — Ordering-aware objective (M)
- **(a) within-day z-score target + L2** — repairs exactly what broke `lgbm_shape`
  (it demeaned but never rescaled; forecast spread collapsed to 53% of actual;
  −£1,770/day, the only statistically significant loser).
- **(b) sample weight ∝ |price − day median|** — only large deviations pay; MAE
  spends capacity on the ~38 flat periods per day.
- **(c) pairwise / LambdaRank within day** (top-4 vs bottom-5 = the LP's decision set).
- **(d) decision-focused weights**: weight each period by how often it sat at the LP
  margin historically — that is literally `∂£/∂forecast`, and it is free from
  `backtest.parquet`.

### M3 — Distribution instead of a point (M)
Quantile LGBM p10/p50/p90. Wins: peaks are the revenue, so the upper quantile is the
decision-relevant output; the optimiser can size against risk; `confidence` for the RT gate.

### M4 — Split DA and RT heads, mirroring the topic split (M)
- **DA head** — robust/median, peak-weighted, `as_of` < 11:00 UTC on D-1.
- **RT head** — predicts the **edge vs the committed plan**, emits only when
  `E[edge] > imbalance risk`. Fixes the −£45,226 hole: today `_intraday_forecast`
  injects `ssp[t-1]` into the current period, trades 273 MWh off-plan and turns
  +£9,602 into −£5,503. The same loop with perfect SSP foresight makes +£199k/20 days,
  so the channel is a leverage amplifier that needs an edge test, not a smoother signal.

### M5 — Features only a stream can give (M)
`wind forecast error now`, `net demand percentile vs 14d`, `system margin`,
`gas share now`, `niv_last`, `ssp_last` — plus **staleness as a feature**
(`now − as_of`), invisible in the batch lab.

### M6 — Continual retrain + shadow eval (L)
Every record becomes a labelled example once `delivery_start` passes. Nightly
walk-forward retrain, shadow model scored on the same stream, drift alarm on rolling
rank IC. Replaces the frozen May–Sep window — the real reason no two models are
statistically distinguishable today.

### M7 — Evaluation protocol (S)
Walk-forward (retrain monthly, test on the next month) alongside the fixed split;
always print the CI and the noise floor, so "lower MAE ≠ more £" stays a claim with an
interval attached.

---

## 5. Phases and gates

| Phase | Work | Gate (must be *run*, not asserted) |
|---|---|---|
| **0** | `git tag green-<date>` on the current green commit | tag exists; `pytest` = 55 passed |
| **1** Clean | §3.1 deletions, `INTERFACE.md`, new `Makefile`, `requirements.txt`, rewritten `AGENTS.md`, trimmed `SPEC.md` | `pytest -q` green · `python main.py` green · `git ls-files \| wc -l` down ~40% |
| **2** Serve | `contracts.py`, `records.py`, `artifacts.py`, `serve.py` (with `--replay` transport), Dockerfile + 2 manifests, fixture | `make serve-local` emits valid envelopes for every fixture record · `make image && docker run --rm …:x --self-check` exits 0 · image < 400 MB |
| **3** Model | M1, then M2a→d, M3, M4 (each own commit) | ≥1 variant with bootstrap CI lower bound > 0 vs baseline |
| **4** Operate | M5, M6, M7: stream features, nightly retrain, drift monitor, `report.md` | shadow eval runs 3 nights unattended; `report.md` regenerates from one command |

## 6. Risks / open items

1. **New dependency:** `confluent-kafka` (librdkafka wheels exist for 3.12) — needs
   approval per `AGENTS.md`. Alternative: `aiokafka` (pure Python, slower). No Kafka
   client exists in the repo today.
2. **Contract fixture is external.** Without a real `features.*` message checked into
   `tests/fixtures/`, train/serve skew is guaranteed. Needs someone with platform-repo
   access to drop one in — the single biggest delivery risk in phase 2.
3. **Day-context columns (D3).** If the platform won't compute `mbh_shape`,
   `net_demand_shape`, `wind_shape` upstream, we fall back to the stateful day cache
   (§1.1) and pods stop being disposable.
4. **Test-deletion rule** blocks removing `src/replay.py` unilaterally (§3.1).
5. **`outputs/model_comparison.csv` is stale** (lgbm £215,033 / MAE 39.8 vs real
   £213,258 / MAE 38.5) — do not publish it; M1 regenerates it.
6. **37 days is not enough to rank good models.** Until M6/M7 land, treat any UK
   ordering inside the noise band as unresolved, not as a finding.

