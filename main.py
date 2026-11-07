"""Prints the pitch numbers. Must always run. `python main.py`"""
import warnings
from pathlib import Path
import pandas as pd
from src.backtest import run
from src.data import is_synthetic
from src.model import BASELINES

warnings.filterwarnings("ignore", message=".*SYNTHETIC.*")
OUT = Path("outputs"); OUT.mkdir(exist_ok=True)

if is_synthetic():
    print("!! SYNTHETIC DATA — numbers are meaningless until data/prices.parquet exists\n")

results = [run("naive"), run("perfect")] + [run("day_ahead", model_name=m) for m in BASELINES]
try:
    results.append(run("day_ahead_rt"))
except NotImplementedError:
    pass
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
