"""Model comparison: test MAE vs backtest GBP. OWNER: B.

Writes outputs/model_comparison.csv with columns (model, mae, total).
"""
from __future__ import annotations
from datetime import timedelta
from pathlib import Path
import pandas as pd
import sys; sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import config as C
from src.backtest import run
from src.data import load_prices, load_imbalance
from src.features import periods
from src.model import ALL_MODELS, train_day_ahead, predict_day_ahead

OUT = Path(__file__).resolve().parent.parent / "outputs"
TOTAL_COL = "total_\u00a3"


def compute_mae(model) -> float:
    """MAE over TEST_START..TEST_END, matching the day-skip rule used by backtest.run
    (skip days where mid or ssp has any NaN)."""
    mid_all = load_prices()["price"]
    ssp_all = load_imbalance()["ssp"]
    errs = []
    day = C.TEST_START
    while day <= C.TEST_END:
        idx = periods(day)
        mid, ssp = mid_all.reindex(idx), ssp_all.reindex(idx)
        if mid.isna().any() or ssp.isna().any():
            day += timedelta(days=1)
            continue
        fc = predict_day_ahead(model, day)
        errs.append((fc - mid).abs())
        day += timedelta(days=1)
    return float(pd.concat(errs).mean()) if errs else float("nan")


def main() -> None:
    OUT.mkdir(exist_ok=True)
    rows = []
    for name in ALL_MODELS:
        model = train_day_ahead(name, C.TRAIN_START, C.TRAIN_END)
        mae = compute_mae(model)
        total = float(run("day_ahead", model_name=name)["pnl_gbp"].sum())
        rows.append({"model": name, "mae": mae, TOTAL_COL: total})
    df = pd.DataFrame(rows, columns=["model", "mae", TOTAL_COL])
    df.to_csv(OUT / "model_comparison.csv", index=False)
    print(df.round(2).to_string(index=False))


if __name__ == "__main__":
    main()
