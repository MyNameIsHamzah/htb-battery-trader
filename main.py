"""Prints the pitch numbers. Must always run. `python main.py`"""
import warnings
from pathlib import Path
import pandas as pd
from src.backtest import run
from src.data import is_synthetic
from src.model import ALL_MODELS

warnings.filterwarnings("ignore", message=".*SYNTHETIC.*")
OUT = Path("outputs"); OUT.mkdir(exist_ok=True)

if is_synthetic():
    print("!! SYNTHETIC DATA — numbers are meaningless until data/prices.parquet exists\n")

<<<<<<< HEAD
LGBM_MODELS = ["lgbm", "lgbm_peak"]
results = (
    [run("naive"), run("perfect")]
    + [run("day_ahead", model_name=m) for m in BASELINES]
    + [run("day_ahead", model_name=m) for m in LGBM_MODELS]
)
try:
    results.append(run("day_ahead_rt"))
except NotImplementedError:
    pass
=======
results = [run("naive"), run("perfect")] + [run("day_ahead", model_name=m) for m in ALL_MODELS]
results.append(run("day_ahead_rt"))
>>>>>>> hamzah/main
bt = pd.concat(results)
bt.to_parquet(OUT / "backtest.parquet")

daily = bt.groupby(["strategy", bt.index.date])["pnl_gbp"].sum().unstack(0)
summary = pd.DataFrame({
    "total_£": daily.sum(),
    "mean_£/day": daily.mean(),
    "worst_day_£": daily.min(),
    "days": daily.count(),
})
summary["%_of_perfect"] = 100 * summary["total_£"] / summary.loc["perfect", "total_£"]
summary["vs_naive_%"] = 100 * (summary["total_£"] / summary.loc["naive", "total_£"] - 1)
print(summary.round(1).sort_values("total_£").to_string())

# Honest C - B: same model, with vs without real-time re-planning.
print()
for rt_name in [s for s in summary.index if s.startswith("day_ahead_rt:")]:
    base = rt_name.replace("day_ahead_rt:", "day_ahead:")
    if base in summary.index:
        c, b = summary.loc[rt_name, "total_£"], summary.loc[base, "total_£"]
        print(f"Real-time re-planning ({rt_name}): C-B = £{c - b:+,.1f} "
              f"(C £{c:,.0f} vs B £{b:,.0f}, {100 * (c / b - 1) if b else 0:+.1f}%)")
