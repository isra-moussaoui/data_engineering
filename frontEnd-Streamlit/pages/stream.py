from __future__ import annotations

import pandas as pd
import streamlit as st

from components.charts import render_deviation_chart, render_event_table, render_price_vwap_chart
from data.sources import get_live_events, get_live_series, get_live_snapshot

st.title("stream")
st.caption("Live crypto execution stream from Kafka and transformation pipeline")

st.sidebar.subheader("stream filters")
coin = st.sidebar.selectbox("asset", options=["BTC", "ETH", "SOL"], index=0)
lookback = st.sidebar.selectbox("lookback", options=["90m", "3h", "12h"], index=1)

minutes = 90 if lookback == "90m" else 180 if lookback == "3h" else 720
series = get_live_series(coin=coin, lookback_minutes=minutes)
snapshot = get_live_snapshot()
events = get_live_events(limit=40, coin=coin)

st.markdown(
    """
    <style>
        .stApp {
            background:
                radial-gradient(800px 300px at 10% -10%, rgba(15, 118, 110, 0.10), transparent 60%),
                radial-gradient(650px 240px at 100% 0%, rgba(249, 115, 22, 0.12), transparent 58%);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

metrics = st.columns(4)
if not snapshot.empty:
    row = snapshot[snapshot["coin"] == coin].head(1)
    if not row.empty:
        metrics[0].metric("last price", f"${row.iloc[0]['price_usd']:,.2f}")
        metrics[1].metric("vwap 1m", f"${row.iloc[0]['vwap_1min']:,.2f}")
        metrics[2].metric("signal", f"{row.iloc[0]['pct_from_vwap']:.2f}%")
        metrics[3].metric("last event", pd.to_datetime(row.iloc[0]["event_time"]).strftime("%H:%M:%S"))

render_price_vwap_chart(series, title=f"{coin} price vs VWAP")
render_deviation_chart(snapshot, title="cross-asset deviation")

st.subheader("recent ticks")
render_event_table(events)
