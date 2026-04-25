from __future__ import annotations

import streamlit as st

from components.charts import render_event_table, render_market_cards, render_rate_chart
from data.sources import get_batch_series, get_batch_snapshot

# daily batch FX and crypto rates dashboard
st.title("Market Data")
st.caption("Daily FX and crypto rates produced by the batch pipeline")

st.sidebar.subheader("batch filters")
pair = st.sidebar.selectbox(
    "pair",
    options=["EUR/USD", "GBP/USD", "USD/JPY", "BTC/USD", "ETH/USD", "SOL/USD"],
    index=0,
)
window = st.sidebar.selectbox("lookback", options=["7d", "14d", "30d"], index=2)

days = 7 if window == "7d" else 14 if window == "14d" else 30
snapshot = get_batch_snapshot(limit=12)
series = get_batch_series(currency_pair=pair, lookback_days=days)

st.markdown(
    """
    <style>
        .stApp {
            background:
                radial-gradient(850px 320px at 0% -15%, rgba(30, 64, 175, 0.08), transparent 58%),
                radial-gradient(700px 260px at 100% 5%, rgba(14, 116, 144, 0.10), transparent 60%);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

render_market_cards(
    snapshot.rename(
        columns={
            "currency_pair": "pair",
            "rate": "price_usd",
            "pct_change": "change_24h",
        }
    )
)
render_rate_chart(series, title=f"{pair} daily rate movement")

st.subheader("latest daily rates")
render_event_table(snapshot)
