# INTERFACE — the only contract with the outside world

This repo is the **price model** and nothing else. It does not fetch data, run Flink,
provision topics, or serve a dashboard. It does exactly one thing:

> consume one feature event → predict → produce one prediction event.

Feeds, Connect, Flink ModelQuery, Redpanda and the control plane live in the
**platform repo**. The full streaming design that used to sit in `SPEC-streaming.md`
lives there too (recoverable here via `git show pre-refocus:SPEC-streaming.md`).
This file is the entire seam between the two repos.

---

## Input — `features.gb-price.v<n>`, key = `prediction_key`

**One record per event** (one settlement period per message). Records may arrive more
than once for the same `prediction_key`; the newest `as_of` wins.

```json
{
  "model_id": "gb-price",
  "feature_set_version": 2,
  "prediction_key": "GB|2026-11-07T18:00:00Z",
  "delivery_start": "2026-11-07T18:00:00Z",
  "as_of": "2026-11-07T15:42:00Z",
  "features": {
    "hour": 18.0, "dow": 5.0, "is_weekend": 1.0,
    "hour_sin": -0.71, "hour_cos": 0.71,
    "price_lag_2d": 118.4, "price_lag_7d": 96.1, "price_last_known": 104.2,
    "mean_by_hour_14d": 121.7,
    "wind_forecast_mw": 8420.0, "demand_forecast_mw": 31100.0, "net_demand_mw": 22680.0,
    "spread_known": -17.5, "spread_lag2": -3.3,
    "mbh_shape": 12.4, "net_demand_shape": 3.1, "wind_shape": -820.0
  }
}
```

### Required invariants (violation ⇒ drop the record and increment a metric; never guess)

1. `feature_set_version` equals the loaded artifact's `feature_set_version`.
   Mismatch at **boot** is a crash-loop, not a warning.
2. `as_of` < 11:00 UTC on D-1 for the day-ahead head (no lookahead, ever).
3. Every key in the model's `FEATURE_SCHEMA` is present, numeric, and finite.
4. `prediction_key` == `"GB|" + delivery_start`.

### Self-sufficiency (the rule that keeps the service stateless)

Every record must be **predictable on its own**. All 14 `FEATURE_COLS` are row-level
already. The three day-context columns — `mbh_shape`, `net_demand_shape`,
`wind_shape`, each `value − delivery_day_mean` — are the only cross-record features
and **must be computed upstream** (`avg(...) OVER (PARTITION BY delivery_day)`).
The service performs no cross-record aggregation; there is no window, no state store,
and no rebalance penalty. If upstream cannot supply them, the fallback is the bounded
day cache in `PLAN.md` §1.1 — an exception, not the design.

---

## Output — `predictions.gb-price.v<n>`, key = `prediction_key`

One prediction per accepted input event. Idempotent: replaying an input yields the
same output bytes. 1:1 with input; consumers dedupe by `prediction_key`, latest-wins.

```json
{
  "model_id": "gb-price",
  "model_version": 3,
  "feature_set_version": 2,
  "prediction_key": "GB|2026-11-07T18:00:00Z",
  "delivery_start": "2026-11-07T18:00:00Z",
  "as_of": "2026-11-07T15:42:00Z",
  "prediction": 141.2,
  "unit": "GBP/MWh",
  "horizon": "day_ahead",
  "quantiles": { "p10": 96.4, "p50": 141.2, "p90": 223.8 },
  "confidence": 0.62,
  "emitted_at": "2026-11-07T15:42:03Z"
}
```

`prediction` is the p50 until the quantile heads land (`PLAN.md` M3).
`confidence` is derived from `p90 − p10` and is what the RT edge gate and the
optimiser consume.

**Non-goals.** No schedules, no SoC, no trade instructions — the optimiser is
downstream of this repo. No alerts, no HTTP query API, no dashboards.

---

## Environment

| Var | Meaning |
|---|---|
| `KAFKA_BOOTSTRAP` | broker list (platform repo owns the cluster) |
| `FEATURES_TOPIC` | default `features.gb-price.v1` |
| `PREDICTIONS_TOPIC` | default `predictions.gb-price.v1` |
| `CONSUMER_GROUP` | `model-gb-price-v<N>` — one group per model version |
| `ARTIFACT_DIR` | `/artifacts` (read-only volume) |
| `SERVE_DAY_CONTEXT` | `upstream` (default) or `local` (fallback cache, `PLAN.md` §1.1) |
