# Task D — Dashboard & demo
**Goal:** the 6 evidence charts + replay demo (SPEC.md → Dashboard).
**Your files:** `app/dashboard.py`, `app/*.py` (new)
**Input:** `outputs/backtest.parquet` (from `python main.py`), later `outputs/model_comparison.csv` (B), `src/replay.py` (C).

## Steps (priority order)
1. Chart 1: cumulative £ per strategy, drawdown shading. Title = conclusion computed from data.
2. Chart 4 (static first): pick a day → 3 stacked panels: price (mid; + forecast later) / charge-discharge bars / SoC.
3. Chart 2: histogram of daily £ vs naive + summary table (same numbers as main.py).
4. Sun: chart 3 (MAE vs £ scatter), chart 5 (heatmap hour × weekday), chart 6 (RT value), replay animation, Live tab.

## Rules
- Use plotly (`st.plotly_chart`) for stacked panels. Add `plotly` to requirements (tell team).
- Works on synthetic data now; real numbers appear automatically when A finishes.
- No maps, no 3D, no decoration.

## Done when
Results + Replay tabs work end to end on real data; every title states a conclusion.
