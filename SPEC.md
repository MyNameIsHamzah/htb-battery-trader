# SPEC — Battery Trader (HRT track: Best Use of Data for Predictions & Decision Making)

## One-liner
**We turn wind forecasts into money.** A trading brain for a grid-scale battery that predicts GB electricity prices from physical + market data, commits a day-ahead plan, then **re-plans in real time** as the grid changes.

## Pitch numbers (everything we build serves these)
| Strategy | What it knows | Profit |
|---|---|---|
| Naive | Charge 00:00–04:00, discharge 17:00–21:00 | £A |
| Day-ahead model | Forecast made at 11:00 D-1 | £B |
| **Day-ahead + real-time re-plan** | Above + live grid data during the day | **£C** |
| Perfect hindsight | Real prices (upper bound) | £D |

Headline: "Real-time re-planning adds £(C−B). We capture C/D % of the theoretical maximum and beat naive by X%."

## How we hit HRT's brief
| HRT card | Our answer |
|---|---|
| Diverse inputs (market + alt data) | Prices + demand (market) **and** wind/solar forecasts, live generation mix (physical) |
| Real-time ML | Intraday re-forecast every 30 min from 5-min generation data |
| Automated decisions | LP optimiser outputs the battery schedule, no human in loop |

---

## Fixed assumptions (don't change without telling the team)
| Param | Value |
|---|---|
| Market | GB, half-hourly settlement periods (48/day, UTC; 46/50 on clock-change days) |
| Day-ahead price | Market Index Price, provider `APXMIDP` (£/MWh) |
| Intraday price (Phase 2) | Imbalance `systemSellPrice` (= `systemBuyPrice` under single pricing) |
| Battery | 50 MW / 100 MWh, 90% round-trip (√0.9 each way), ≤1 cycle/day, SoC 10–100 MWh |
| Day-ahead decision | 11:00 on D-1, only data known then |
| Intraday decision | At start of each period t, using only data published before t |
| Train / test | Train May–Sep 2026. Test 1 Oct – 6 Nov 2026. Never tune on test |

**No-lookahead rule:** every feature must carry its publish time, and we filter on it. Judges will check this.
- At 11:00 D-1 only periods **before 11:00 on D-1** are known. So D-1 afternoon/evening prices are NOT available.
- Price lags allowed: `price_lag_2d` (same period, D-2), `price_lag_7d`, `price_last_known` (last period before 11:00 D-1), `mean_by_hour_14d` computed only up to 11:00 D-1.
- Baseline renamed: `same_time_2_days_ago` (NOT "yesterday").
- Unit test: `assert features.published_at.max() < decision_time` for every day.

## Settlement model (two prices — this is what makes £C honest)
- **Day-ahead plan** is a commitment: each period's DA position (MW) settles at **MID** (our DA price proxy).
- **Any deviation** (real-time action − DA plan) settles at the **imbalance price** (`ssp`; GB has single imbalance pricing, so `ssp ≈ sbp`).
- P&L(t) = DA_pos × MID × 0.5 + (RT_pos − DA_pos) × SSP × 0.5, where pos = discharge − charge (MW).
- Strategies `naive`, `day_ahead`, `perfect` have deviation = 0. Only `day_ahead_rt` deviates, and pays/earns imbalance price for it. So C − B = true value of re-planning (can be negative — that's a valid finding).
- `perfect` = perfect foresight of MID, no deviations.

## Price data caveats (say these on a slide before judges do)
- MID is an index of short-term traded volume per period, **not** a day-ahead auction price you can lock in. We use it as a proxy for the DA price.
- Drop periods with `volume == 0` or missing price (do NOT treat as £0). Keep genuine negative prices.

## Evaluation: £ first, MAE second
Every model is reported with BOTH test MAE and backtest £. Expected finding: lower MAE ≠ more £ (the battery only needs to rank peaks and troughs correctly). This table is a core slide.

---

## Phases
- **Phase 1 (must have, by 19:30):** day-ahead pipeline end to end with baseline forecast.
- **Phase 2 (by 22:00):** LightGBM forecast beats baselines; dashboard backtest tab.
- **Phase 3 (Sun morning, the wow):** intraday re-planning + **replay demo**.

---

## Repo layout & owners
```
data/                    # parquet, gitignored                 — A
src/fetch.py             # Elexon pulls (hist + latest)        — A
src/features.py          # feature tables, publish-time safe   — A
src/model.py             # day-ahead + intraday forecasts      — B
src/optimise.py          # LP schedule                         — C
src/backtest.py          # naive / da / da+rt / perfect        — C
src/replay.py            # streams a past day period by period — C/D
app/dashboard.py         # Streamlit                           — D
main.py                  # prints the 4 pitch numbers
```

---

## Interfaces (the contract — stub these first, real later)

### Data (A) → parquet, index `ts` = period start, UTC
| file | columns |
|---|---|
| `prices.parquet` | `price` (MID APXMIDP, £/MWh) |
| `imbalance.parquet` | `ssp`, `sbp`, `niv` (net imbalance volume) |
| `gen.parquet` (FUELINST, 5-min) | `wind`, `ccgt`, `nuclear`, `solar_est`, …, `published_at` |
| `windfor.parquet` | `wind_forecast_mw`, `published_at` |

### Features (A)
```python
def day_ahead_features(day: date) -> pd.DataFrame:
    """48 rows for `day`, only data published before 11:00 on day-1.
    Min cols: hour, dow, price_lag_2d, price_lag_7d, price_last_known,
    wind_forecast_mw, demand_forecast_mw.
    Sunday extra: system margin / surplus forecast (spike driver, rarely used in literature)."""

def intraday_features(t: pd.Timestamp) -> pd.DataFrame:
    """Rows for remaining periods of the day from t, data published before t.
    Adds: wind_actual_now, wind_forecast_error_now (actual - forecast),
    gas_share_now, niv_last, ssp_last."""
```

### Model (B)
```python
def train_day_ahead(start, end) -> object
def predict_day_ahead(model, day) -> pd.Series      # 48 prices, index ts
def train_intraday(start, end) -> object
def predict_intraday(model, t) -> pd.Series         # remaining periods from t
```
Baselines first: `same_time_2_days_ago`, `same_time_last_week`, `mean_by_hour_14d` (all respecting the 11:00 D-1 cutoff). Report MAE **and £** per model on test.

### Optimiser (C)
```python
def schedule(prices: pd.Series, soc0_mwh: float, cycles_left: float = 1.0) -> pd.DataFrame:
    """MILP via scipy.optimize.milp (scipy >= 1.9). Binary per period:
    is_charging ∈ {0,1}; charge_mw <= 50*is_charging, discharge_mw <= 50*(1-is_charging).
    Prevents simultaneous charge+discharge (an LP will 'burn' energy via efficiency
    losses to get paid at negative prices — a real battery can't).
    Works for any horizon length (48 or fewer).
    Returns cols: ts, charge_mw, discharge_mw, soc_mwh.
    Test: assert (charge_mw * discharge_mw == 0).all()"""
```

### Backtest (C)
```python
def run(strategy: str, start: date, end: date) -> pd.DataFrame:
    """strategy in {'naive','day_ahead','day_ahead_rt','perfect'}.
    'day_ahead_rt': start from DA plan; at each period t re-forecast remaining
    periods, re-solve from current SoC, execute only period t.
    Settle against REAL prices. Returns: ts, price, charge_mw, discharge_mw,
    soc_mwh, pnl_gbp, strategy."""
```
P&L per period: see **Settlement model** above (DA at MID, deviations at SSP).

### Replay (C/D) — the demo engine
```python
def replay(day: date, speed: float = 60) -> Iterator[dict]:
    """Yields one event per period, as if live: {ts, new_data, forecast_remaining,
    plan_before, plan_after, action, soc, pnl_so_far}.
    speed=60 → a full day in ~24 min; speed=500 → ~3 min for the pitch."""
```

### Dashboard (D) — Streamlit

**Principle:** HRT engineers want evidence, not decoration. Every chart answers one question a judge would otherwise ask. **Chart title = the conclusion** ("Model beats naive by 34% over 37 test days"), not the description ("Cumulative P&L").

| # | Question it answers | Chart | Priority |
|---|---|---|---|
| 1 | Does it make money? | Cumulative P&L over test period, 4 lines (naive / DA / DA+RT / perfect). Shade drawdowns | Sat 19:30–22:00 |
| 2 | Is it luck? | Histogram of daily £ vs naive + table: total £, mean/day, worst day, % winning days, % of perfect | Sat 19:30–22:00 |
| 3 | Lower error = more money? | Scatter: x = test MAE, y = backtest £, one point per model. Core insight slide | Sun AM |
| 4 | What does the bot actually do? | One day, 3 stacked panels: forecast vs actual price / charge-discharge bars / SoC line. **Animated = replay demo** | Sat 19:30–22:00 |
| 5 | Where does the edge come from? | Heatmap £ by hour × weekday; bar £ on windy vs calm days and negative-price days | Sun AM |
| 6 | What did real-time add? | £C − £B per day, deviation P&L at imbalance price. Show honestly even if small/negative | Sun AM |

**Tabs**
1. **Results** (charts 1, 2, 3, 6) — the evidence.
2. **Replay** (chart 4 animated): pre-picked dramatic test day (wind drop / negative prices). Forecast updates, plan redrawn, SoC gauge, P&L ticker, banner on plan change ("Wind 2 GW below forecast → holding charge for 18:00 peak").
3. **Edge** (chart 5) — why it works.
4. **Live:** latest Elexon data → tomorrow's forecast + plan (proves it runs on real live data).

**Don't build:** maps, 3D, decorative animation, raw-data plots without a takeaway, any accuracy number without a baseline beside it.

**Why replay:** live data during judging may be boring. A pre-picked dramatic day guarantees the wow moment; the Live tab proves it's real.

**Demo order in pitch:** Chart 1 (it works) → Replay (how it works) → Chart 3 (the insight) → Chart 2 + caveats slide (it's honest).

---

## Timeline
| Time | Milestone |
|---|---|
| 17:00 | Spec agreed, repo pushed, stubs in every module |
| 18:00 | prices + imbalance + FUELINST parquet for 6 months. LP solves one day |
| **19:30** | `python main.py` prints naive / day-ahead (baseline) / perfect |
| **22:00** | LightGBM beats baselines; dashboard backtest tab |
| 23:00 | Go home. A picks 3 dramatic test days before leaving |
| Sun 08:00 | Intraday re-plan in backtest, replay + live tabs |
| **10:00** | Feature freeze |
| 10:00–12:00 | Slides, rehearse demo x2, README |

**Rule:** Phase 3 only starts once Phase 2 works. If Phase 3 isn't done by 10:00, we demo Phase 2 — still a full project.

## Stretch (only if everything above is done)
Probabilistic forecast + risk-aware schedule; degradation cost per MWh; trade both MID and imbalance.

## Out of scope
Real trading, multiple batteries, deep learning, frequency response markets.

## Data endpoints (Elexon Insights, no key) — base `https://data.elexon.co.uk/bmrs/api/v1`
| Data | Endpoint | Notes |
|---|---|---|
| Market price | `/datasets/MID?from=…&to=…` | ≤7 days/call; keep `APXMIDP` (N2EX often 0) ✅ tested |
| Imbalance price | `/balancing/settlement/system-prices/{date}` | one day/call ✅ tested |
| Gen by fuel (5-min) | `/datasets/FUELINST` | also `/datasets/FUELINST/stream` |
| Gen by fuel (30-min) | `/datasets/FUELHH` | |
| Wind forecast | `/datasets/WINDFOR` | keep publish time |
| Embedded solar (optional) | `https://api.pvlive.uk/pvlive/api/v4/gsp/0` | Sheffield Solar |
