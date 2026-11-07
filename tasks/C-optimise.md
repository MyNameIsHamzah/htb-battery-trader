# Task C — Optimiser, backtest, real-time re-planning
**Goal:** correct settlement + Phase 3 intraday re-planning + replay engine.
**Your files:** `src/optimise.py`, `src/backtest.py`, `src/replay.py` (new), `main.py`, `tests/test_backtest_C.py` (new)
**Already done & tested:** MILP `schedule()`, `naive_schedule()`, `settle()`, `run()` for naive/day_ahead/perfect.

## Steps
1. Read `src/optimise.py` and `src/backtest.py`. Run tests. Understand the MILP (variables c, d, b, s).
2. Check on real data (after A): any day where solver fails or is slow (> 2 s)? Log and fix.
3. Implement `day_ahead_rt` in `run()`:
   - DA plan = schedule(day-ahead forecast) (committed).
   - For each period t: re-forecast remaining periods (use `predict_intraday` once B/A have it; until then use DA forecast + latest known SSP as a placeholder), re-solve `schedule()` for remaining horizon from current SoC with remaining cycles, execute only period t.
   - RT plan settles via `settle(da, rt, mid, ssp)` — deviations at SSP.
4. `src/replay.py`: `replay(day, speed)` yielding per-period dicts (see SPEC) for D's demo.
5. Tests: RT with perfect info ≥ DA; deviation P&L computed at SSP; cycles never exceed limit across re-plans.

## Pitfalls
- Track `cycles_left` and SoC across re-plans. End-of-day SoC ≥ 50.
- Re-planning must only use data published before t.

## Done when
`main.py` prints all 4 strategies; £C − £B reported honestly.
