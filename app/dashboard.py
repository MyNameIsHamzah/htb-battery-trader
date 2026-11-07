"""Streamlit dashboard. OWNER: D.   streamlit run app/dashboard.py
Reads outputs/backtest.parquet written by `python main.py`.
See SPEC.md 'Dashboard' for the 6 charts + 4 tabs. Chart titles = conclusions.
"""
from pathlib import Path
import pandas as pd
import streamlit as st

BT = Path(__file__).resolve().parent.parent / "outputs" / "backtest.parquet"
st.set_page_config(page_title="Battery Trader", layout="wide")
if not BT.exists():
    st.error("Run `python main.py` first"); st.stop()
bt = pd.read_parquet(BT)

tab_results, tab_replay, tab_edge, tab_live = st.tabs(["Results", "Replay", "Edge", "Live"])

with tab_results:
    cum = bt.groupby("strategy")["pnl_gbp"].cumsum()
    wide = bt.assign(cum=cum).reset_index().pivot_table(index="ts", columns="strategy", values="cum")
    st.subheader("TODO(D): title = conclusion, e.g. 'Model beats naive by X%'")
    st.line_chart(wide)  # chart 1 — TODO(D): drawdown shading, chart 2, 3, 6

with tab_replay:
    st.info("TODO(D): chart 4 — one day, price/forecast, charge-discharge bars, SoC; animated")

with tab_edge:
    st.info("TODO(D): chart 5 — £ heatmap hour x weekday; windy vs calm")

with tab_live:
    st.info("TODO(D/A): tomorrow's forecast + plan from latest Elexon data")
