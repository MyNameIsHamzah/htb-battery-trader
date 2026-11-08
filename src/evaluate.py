"""Model evaluation beyond headline totals. OWNER: B/C.

    python -m src.evaluate                          # default model list
    python -m src.evaluate lgbm lgbm_shape blend_30 # any model names

1. Daily P&L for each model's day-ahead strategy over the test window.
2. Paired bootstrap of daily P&L vs the baseline: is the gap real or noise?
3. Win/loss vs forecast wind: on which kind of day does each model help?

Writes outputs/daily_pnl.csv, outputs/bootstrap_vs_baseline.csv,
outputs/winloss_by_wind.csv.
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

from src import config as C
from src.backtest import run
from src.features import day_ahead_features

BASELINE = "mean_by_hour_14d"
DEFAULT_MODELS = ["lgbm", "lgbm_peak", "lgbm_shape", "blend", "same_time_2_days_ago"]
OUT = Path(__file__).resolve().parent.parent / "outputs"


def _dates(df: pd.DataFrame) -> pd.Index:
    ts = pd.DatetimeIndex(df["ts"] if "ts" in df.columns else df.index)
    return pd.Index(ts.date, name="day")


def daily_pnl(models: list[str], start=C.TEST_START, end=C.TEST_END) -> pd.DataFrame:
    """Rows = test days, columns = model names, values = £ P&L that day."""
    cols = {}
    for m in [BASELINE] + [m for m in models if m != BASELINE]:
        print(f"  backtesting {m} ...", flush=True)
        df = run("day_ahead", start, end, model_name=m)
        cols[m] = df["pnl_gbp"].groupby(_dates(df)).sum()
    return pd.DataFrame(cols).dropna()


def paired_bootstrap(diff: np.ndarray, n_boot: int = 10_000, block: int = 1,
                     seed: int = 0) -> dict:
    """Bootstrap the mean of daily P&L differences (model - baseline).

    block > 1 uses a circular block bootstrap, which keeps runs of consecutive
    days together in case good/bad days cluster (e.g. multi-day wind spells).
    """
    diff = np.asarray(diff, dtype=float)
    n = len(diff)
    rng = np.random.default_rng(seed)
    n_blocks = int(np.ceil(n / block))
    starts = rng.integers(0, n, size=(n_boot, n_blocks))
    idx = ((starts[:, :, None] + np.arange(block)) % n).reshape(n_boot, -1)[:, :n]
    means = diff[idx].mean(axis=1)
    lo, hi = np.percentile(means, [2.5, 97.5])
    return {
        "days": n,
        "mean_diff_per_day": diff.mean(),
        "ci95_lo": lo,
        "ci95_hi": hi,
        "total_diff": diff.sum(),
        "win_rate": (diff > 0).mean(),
        "p_not_better": (means <= 0).mean(),  # share of resamples where model <= baseline
        "significant": bool(lo > 0 or hi < 0),
    }


def bootstrap_table(pnl: pd.DataFrame, block: int = 1) -> pd.DataFrame:
    rows = {m: paired_bootstrap((pnl[m] - pnl[BASELINE]).to_numpy(), block=block)
            for m in pnl.columns if m != BASELINE}
    return pd.DataFrame(rows).T


def wind_context(days) -> pd.DataFrame:
    """Per test day: mean forecast wind and net demand known at decision time."""
    rows = {}
    for d in days:
        f = day_ahead_features(d)
        rows[d] = {"wind_mean_mw": f["wind_forecast_mw"].mean(),
                   "net_demand_mean_mw": f["net_demand_mw"].mean()}
    ctx = pd.DataFrame(rows).T
    w = ctx["wind_mean_mw"]
    ctx["wind_z"] = (w - w.mean()) / w.std()
    return ctx


def winloss_by_wind(pnl: pd.DataFrame, ctx: pd.DataFrame) -> pd.DataFrame:
    """Mean daily £ vs baseline, split into low / mid / high forecast-wind days,
    and into ordinary vs unusual-wind days (|z| terciles)."""
    df = pnl.join(ctx, how="inner")
    groupings = {
        "wind_level": pd.qcut(df["wind_z"], 3, labels=["low_wind", "mid_wind", "high_wind"]),
        "wind_unusualness": pd.qcut(df["wind_z"].abs(), 3, labels=["ordinary", "somewhat", "unusual"]),
    }
    out = []
    for gname, bins in groupings.items():
        for m in pnl.columns:
            if m == BASELINE:
                continue
            diff = df[m] - df[BASELINE]
            g = diff.groupby(bins, observed=True)
            out.append(pd.DataFrame({
                "grouping": gname, "model": m,
                "days": g.size(), "mean_diff_per_day": g.mean(), "win_rate": g.apply(lambda s: (s > 0).mean()),
            }))
    return pd.concat(out).rename_axis("bin").reset_index()


def main(models: list[str]) -> None:
    OUT.mkdir(exist_ok=True)
    pnl = daily_pnl(models)
    pnl.to_csv(OUT / "daily_pnl.csv")

    pd.set_option("display.width", 160)
    pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
    print(f"\nTotal P&L over {len(pnl)} days:")
    print(pnl.sum().sort_values(ascending=False).to_string())

    for block in (1, 3):
        bt = bootstrap_table(pnl, block=block)
        print(f"\nPaired bootstrap vs {BASELINE} (block={block} day{'s' if block > 1 else ''}), £/day:")
        print(bt.to_string())
        if block == 1:
            bt.to_csv(OUT / "bootstrap_vs_baseline.csv")

    wl = winloss_by_wind(pnl, wind_context(pnl.index))
    wl.to_csv(OUT / "winloss_by_wind.csv", index=False)
    print("\nMean £/day vs baseline by forecast wind:")
    print(wl.pivot_table(index=["grouping", "bin"], columns="model", values="mean_diff_per_day",
                         observed=True).to_string())


if __name__ == "__main__":
    main(sys.argv[1:] or DEFAULT_MODELS)