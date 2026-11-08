"""Walk-forward evaluation: does the edge survive out of sample? OWNER: B/C.

    python scripts/walkforward.py                                  # lgbm, defaults
    python scripts/walkforward.py lgbm lgbm_peak --test-days 30 --step 30
    python scripts/walkforward.py --train-days 60 --test-days 7 --step 60 lgbm   # quick

The frozen holdout is 37 days; the daily-£ noise floor there is ~£618/day
(1 sigma), so no model is statistically distinguishable (PLAN §6.6). Walk-
forward buys back sample size: every day becomes an out-of-sample day exactly
once, each scored against `mean_by_hour_14d` *on the same days* (paired), with a
circular block bootstrap CI (consecutive days cluster around wind spells).

Why not `backtest.run`? `run()` hardcodes
`train_day_ahead(name, C.TRAIN_START, C.TRAIN_END)` (backtest.py:98), so reusing
it would train on data *inside/after* the test window -> lookahead. This script
re-implements the (small) day loop against the public model/optimise/backtest
API instead. Keep the two in sync by hand.

Deliberately day-ahead only: no deviation, so `settle(da, da, ...)`. The RT head
is evaluated by `python -m src.evaluate` / `main.py` instead.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src import config as C  # noqa: E402
from src.backtest import settle  # noqa: E402
from src.data import load_imbalance, load_prices  # noqa: E402
from src.evaluate import BASELINE, paired_bootstrap  # noqa: E402
from src.features import periods  # noqa: E402
from src.model import predict_day_ahead, train_day_ahead  # noqa: E402
from src.optimise import schedule  # noqa: E402

OUT = ROOT / "outputs"


def daily_pnl(model_name: str, train_start: date, train_end: date,
              test_start: date, test_end: date) -> pd.Series:
    """£/day on [test_start, test_end] for a model trained on [train_start, train_end].

    Mirrors `backtest.run(..., strategy="day_ahead")`: same skips (days with any
    missing MID/SSP are dropped), same `schedule()` defaults, no deviation.
    """
    mid_all = load_prices()["price"]
    ssp_all = load_imbalance()["ssp"]
    model = train_day_ahead(model_name, train_start, train_end)
    rows: dict[date, float] = {}
    day = test_start
    while day <= test_end:
        idx = periods(day)
        mid, ssp = mid_all.reindex(idx), ssp_all.reindex(idx)
        if mid.isna().any() or ssp.isna().any():
            day += timedelta(days=1)
            continue
        da = schedule(predict_day_ahead(model, day))
        rows[day] = float(settle(da, da, mid, ssp).sum())
        day += timedelta(days=1)
    return pd.Series(rows, name=model_name).sort_index()


def windows(start: date, end: date, train_days: int, test_days: int, step: int):
    """Rolling-origin windows; test blocks never overlap when step == test_days."""
    test_start = start + timedelta(days=train_days)
    while test_start + timedelta(days=test_days - 1) <= end:
        test_end = test_start + timedelta(days=test_days - 1)
        yield (test_start - timedelta(days=train_days), test_start - timedelta(days=1),
               test_start, test_end)
        test_start += timedelta(days=step)


def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("models", nargs="*", default=["lgbm"])
    ap.add_argument("--start", default=str(C.TRAIN_START), help="earliest training day")
    ap.add_argument("--end", default=str(C.TEST_END), help="latest test day")
    ap.add_argument("--train-days", type=int, default=90)
    ap.add_argument("--test-days", type=int, default=21)
    ap.add_argument("--step", type=int, default=21)
    ap.add_argument("--block", type=int, default=3, help="bootstrap block length (days)")
    a = ap.parse_args(argv)

    start, end = date.fromisoformat(a.start), date.fromisoformat(a.end)
    models = [m for m in (a.models or ["lgbm"]) if m != BASELINE]
    wins = list(windows(start, end, a.train_days, a.test_days, a.step))
    if not wins:
        sys.exit(f"no windows fit in {start}..{end} with train_days={a.train_days} "
                 f"test_days={a.test_days}")
    if a.step < a.test_days:
        print(f"WARNING: step ({a.step}d) < test_days ({a.test_days}d) -> test blocks "
              f"overlap and pooled days are double-counted. Use step == test_days "
              f"for an honest walk-forward.")

    print(f"walk-forward: {len(wins)} windows, train={a.train_days}d, "
          f"test={a.test_days}d, step={a.step}d, models={models} vs {BASELINE}", flush=True)

    per_window, pooled = [], {m: [] for m in models}
    for i, (tr_s, tr_e, te_s, te_e) in enumerate(wins, 1):
        print(f"\n[{i}/{len(wins)}] train {tr_s}..{tr_e}  test {te_s}..{te_e}", flush=True)
        base = daily_pnl(BASELINE, tr_s, tr_e, te_s, te_e)
        for m in models:
            pnl = daily_pnl(m, tr_s, tr_e, te_s, te_e)
            diff = (pnl - base).dropna()
            st = paired_bootstrap(diff.to_numpy(), block=a.block)
            st |= {"window": i, "model": m, "train_start": tr_s, "train_end": tr_e,
                   "test_start": te_s, "test_end": te_e, "baseline_gbp": float(base.sum())}
            per_window.append(st)
            pooled[m].append(diff)
            print(f"    {m:12s} {st['mean_diff_per_day']:+8.0f} GBP/day  "
                  f"CI [{st['ci95_lo']:+8.0f}, {st['ci95_hi']:+8.0f}]  "
                  f"win {st['win_rate']:.0%}  sig={st['significant']}", flush=True)

    OUT.mkdir(exist_ok=True)
    pd.DataFrame(per_window).to_csv(OUT / "walkforward_windows.csv", index=False)

    print(f"\nPooled out-of-sample days (block={a.block}):")
    pool_rows = []
    for m in models:
        diff = pd.concat(pooled[m]) if pooled[m] else pd.Series(dtype=float)
        if diff.empty:
            continue
        st = paired_bootstrap(diff.to_numpy(), block=a.block)
        st["model"] = m
        pool_rows.append(st)
        print(f"  {m:12s} n={st['days']:4d} days  {st['mean_diff_per_day']:+8.0f} GBP/day  "
              f"total {st['total_diff']:+10.0f} GBP  "
              f"CI [{st['ci95_lo']:+8.0f}, {st['ci95_hi']:+8.0f}]  "
              f"win {st['win_rate']:.0%}  sig={st['significant']}")
    pd.DataFrame(pool_rows).to_csv(OUT / "walkforward_pooled.csv", index=False)

    print("\nCaveat: training windows overlap across folds, so pooled days are not "
          "fully independent — the block bootstrap widens the CI but does not "
          "remove the dependence. Treat `sig=True` as necessary, not sufficient.")


if __name__ == "__main__":
    main(sys.argv[1:])

