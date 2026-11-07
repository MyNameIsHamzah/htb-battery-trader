"""Battery schedule optimiser (MILP). OWNER: C.

Contract:
    schedule(prices, soc0_mwh, cycles_left, soc_end_min) -> DataFrame
    cols: charge_mw, discharge_mw, soc_mwh (end-of-period), index = prices.index

Binary per period stops simultaneous charge+discharge (an LP would 'burn'
energy through efficiency losses to get paid at negative prices).
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from scipy.optimize import milp, LinearConstraint, Bounds
from scipy.sparse import lil_matrix
from src import config as C


def schedule(prices: pd.Series, soc0_mwh: float = C.SOC_START_MWH,
             cycles_left: float = C.MAX_CYCLES_PER_DAY,
             soc_end_min: float | None = C.SOC_START_MWH,
             charge_allowed=None, discharge_allowed=None) -> pd.DataFrame:
    p = prices.to_numpy(dtype=float)
    if np.isnan(p).any():
        raise ValueError("prices contain NaN — fill or drop before scheduling")
    n, h, P, eta = len(p), C.PERIOD_H, C.POWER_MW, C.ETA
    # variable layout: [c(n), d(n), b(n), s(n)]
    ic, id_, ib, is_ = 0, n, 2 * n, 3 * n
    nv = 4 * n

    # minimise -(revenue) + tiny throughput penalty
    cost = np.zeros(nv)
    cost[ic:ic + n] = p * h + C.THROUGHPUT_PENALTY
    cost[id_:id_ + n] = -p * h + C.THROUGHPUT_PENALTY

    rows, lo, hi = [], [], []
    A = lil_matrix((3 * n + 2, nv))
    r = 0
    for t in range(n):
        # SoC dynamics: s_t - s_{t-1} - eta*h*c_t + h/eta*d_t = 0
        A[r, is_ + t] = 1
        if t > 0:
            A[r, is_ + t - 1] = -1
        A[r, ic + t] = -eta * h
        A[r, id_ + t] = h / eta
        rhs = soc0_mwh if t == 0 else 0.0
        lo.append(rhs); hi.append(rhs); r += 1
        # c_t <= P*b_t ; d_t <= P*(1-b_t)
        A[r, ic + t] = 1; A[r, ib + t] = -P; lo.append(-np.inf); hi.append(0); r += 1
        A[r, id_ + t] = 1; A[r, ib + t] = P; lo.append(-np.inf); hi.append(P); r += 1
    # cycle limit: sum d*h <= cycles_left * E
    A[r, id_:id_ + n] = h; lo.append(-np.inf); hi.append(cycles_left * C.ENERGY_MWH); r += 1
    # end SoC floor (avoid selling the starting energy for free)
    A[r, is_ + n - 1] = 1
    lo.append(soc_end_min if soc_end_min is not None else C.SOC_MIN_MWH); hi.append(np.inf); r += 1

    lb = np.zeros(nv); ub = np.full(nv, P)
    ub[ib:ib + n] = 1
    lb[is_:] = C.SOC_MIN_MWH; ub[is_:] = C.SOC_MAX_MWH
    if charge_allowed is not None:
        ub[ic:ic + n] = np.where(np.asarray(charge_allowed), P, 0.0)
    if discharge_allowed is not None:
        ub[id_:id_ + n] = np.where(np.asarray(discharge_allowed), P, 0.0)
    integrality = np.zeros(nv); integrality[ib:ib + n] = 1

    res = milp(cost, constraints=LinearConstraint(A.tocsr(), lo, hi),
               bounds=Bounds(lb, ub), integrality=integrality,
               options={"time_limit": 10})
    if res.x is None:
        raise RuntimeError(f"MILP failed: {res.message}")
    x = res.x
    c = np.round(x[ic:ic + n], 6); d = np.round(x[id_:id_ + n], 6)
    c[c < 1e-6] = 0; d[d < 1e-6] = 0
    return pd.DataFrame({"charge_mw": c, "discharge_mw": d, "soc_mwh": x[is_:is_ + n]},
                        index=prices.index)


def naive_schedule(index: pd.DatetimeIndex) -> pd.DataFrame:
    """Naive rule: charge only in NAIVE_CHARGE_HOURS, discharge only in
    NAIVE_DISCHARGE_HOURS, as much as physically possible. Uses no prices."""
    ch = index.hour.isin(list(C.NAIVE_CHARGE_HOURS))
    dis = index.hour.isin(list(C.NAIVE_DISCHARGE_HOURS))
    signal = pd.Series(np.where(ch, -100.0, np.where(dis, 100.0, 0.0)), index=index)
    return schedule(signal, charge_allowed=ch, discharge_allowed=dis)
