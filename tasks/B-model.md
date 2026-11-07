# Task B — Price forecast model
**Goal:** a day-ahead model that beats the baselines **in £**, not just MAE.
**Your files:** `src/model.py`, `src/evaluate.py` (new), `tests/test_model_B.py` (new)
**Don't touch:** signatures of `train_day_ahead` / `predict_day_ahead`, everything else.

## Steps
1. Write `src/evaluate.py`: for each model in BASELINES (+ yours), compute test MAE (TEST_START..TEST_END) and £ via `backtest.run("day_ahead", model_name=...)`. Save `outputs/model_comparison.csv` (model, mae, total_£).
2. Add model `"lgbm"` in `model.py`: LightGBM on `day_ahead_features`, trained ONLY on TRAIN_START..TRAIN_END. Register so `run("day_ahead", model_name="lgbm")` works and add it to the models looped in `main.py` (ask C/human — main.py is shared).
3. Try a ranking-aware variant (e.g. predict deviation from daily mean, or weight peak hours). Goal: show lower MAE ≠ more £.
4. Test file: lgbm returns 48 non-NaN values; training never touches dates ≥ TEST_START.

## Pitfalls
- No tuning on test. Use the last 3 weeks of train as validation.
- Features with NaN (early days): LightGBM handles NaN; don't drop rows silently.
- Wait for A's real data before trusting any number.

## Done when
`outputs/model_comparison.csv` exists with MAE + £ for ≥ 4 models; lgbm beats best baseline in £ (or you can show why not).
