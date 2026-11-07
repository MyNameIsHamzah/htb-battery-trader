# Task A — Data & features
**Goal:** real Elexon data in `data/`, and leak-free features.
**Your files:** `src/fetch.py`, `src/data.py`, `src/features.py`, `tests/test_data_A.py` (new)
**Don't touch:** everything else.

## Steps
1. Run `python -m src.fetch 2026-04-20 2026-11-07`. Check with `python scripts/peek.py data/prices.parquet`.
   - Accept: ≥ 8,000 rows, no zero-volume rows, negative prices kept, index UTC 30-min.
2. Run `python main.py` — the "SYNTHETIC" warning must be gone. Tell the team the real numbers.
3. Add FUELINST (5-min gen by fuel) → `data/gen.parquet` and WINDFOR → `data/windfor.parquet`, each with a `published_at` column (UTC).
4. In `features.py`, filter wind forecast on `published_at < decision_time(day)`; take the latest forecast per target period.
5. Add features: `wind_forecast_mw`, `demand_forecast_mw` (find Elexon endpoint), later system margin forecast.
6. Write `tests/test_data_A.py`: no duplicate ts, ≥ 46 periods/day, every feature's `published_at` < cutoff.

## Pitfalls
- Settlement periods: use `startTime` as ts, not period number. Clock-change days have 46/50 periods.
- MID: keep only `APXMIDP`; drop `volume == 0`.
- FUELINST is large: fetch in ≤ 7-day chunks, resample to 30-min (mean) for features.

## Done when
`pytest -q -p no:warnings` passes, `main.py` runs on real data, features include wind forecast with no lookahead.
