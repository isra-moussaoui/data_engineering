from __future__ import annotations

import os
from datetime import datetime, timezone

import streamlit as st
from dotenv import load_dotenv

from components.charts import (
    render_deviation_chart,
    render_event_table,
    render_market_cards,
    render_price_vwap_chart,
    render_rate_chart,
)
from data.sources import (
    get_batch_series,
    get_batch_snapshot,
    get_dashboard_overview,
    get_live_events,
    get_live_series,
    get_live_snapshot,
    get_pipeline_health,
)

load_dotenv()

st.set_page_config(
    page_title="PulseFX",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
        .stApp {
            background:
                radial-gradient(1200px 500px at 0% -10%, rgba(20, 184, 166, 0.18), transparent 58%),
                radial-gradient(900px 400px at 100% -20%, rgba(249, 115, 22, 0.12), transparent 52%),
                linear-gradient(180deg, #f7fbff 0%, #eef4f9 100%);
        }
        h1, h2, h3 { font-family: 'Space Grotesk', sans-serif; color: #0f172a; }
        p, li, .stCaption, .stMarkdown { font-family: 'IBM Plex Mono', monospace; }
        .hero-box {
            padding: 1.1rem 1.2rem;
            border-radius: 16px;
            border: 1px solid #dce8f0;
            background: linear-gradient(92deg, #ffffff 0%, #f5fbff 100%);
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
        }
        .hero-kicker { color: #0f766e; letter-spacing: 0.08em; font-weight: 700; font-size: 0.74rem; }
        .hero-title { margin-top: 6px; font-size: 1.85rem; font-weight: 700; color: #0f172a; }
        .hero-sub { margin-top: 4px; color: #334155; font-size: 0.92rem; }
        .section-pill {
            display: inline-block;
            padding: 0.18rem 0.55rem;
            margin-bottom: 0.6rem;
            border-radius: 999px;
            background: rgba(15, 118, 110, 0.10);
            color: #0f766e;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.05em;
        }
        .tiny-pill {
            display: inline-block;
            margin-top: 10px;
            padding: 0.25rem 0.55rem;
            border-radius: 999px;
            border: 1px solid #bfdbfe;
            background: #eff6ff;
            color: #1d4ed8;
            font-size: 0.72rem;
            font-weight: 700;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

app_mode = os.getenv("APP_MODE", "live").lower()
overview = get_dashboard_overview()
stream_snapshot = get_live_snapshot()
batch_snapshot = get_batch_snapshot()
health = get_pipeline_health()

st.sidebar.header("Control Panel")
selected_coin = st.sidebar.selectbox("Live asset", options=["BTC", "ETH", "SOL"], index=0)
selected_window = st.sidebar.selectbox("Live window", options=["30m", "1h", "6h"], index=1)
selected_pair = st.sidebar.selectbox(
    "Batch pair",
    options=["EUR/USD", "GBP/USD", "USD/JPY", "BTC/USD", "ETH/USD", "SOL/USD"],
    index=0,
)
selected_pair_window = st.sidebar.selectbox("Batch lookback", options=["7d", "14d", "30d"], index=2)
st.sidebar.metric("Mode", app_mode.upper())
st.sidebar.metric("UTC", datetime.now(timezone.utc).strftime("%H:%M:%S"))
st.sidebar.caption("Tune filters and navigate with the page list below.")

left, right = st.columns([2.4, 1])
with left:
    st.markdown(
        f"""
        <div class="hero-box">
            <div class="hero-kicker">CURRENCY INTELLIGENCE PLATFORM</div>
            <div class="hero-title">PulseFX</div>
            <div class="hero-sub">
                Institutional-grade visibility over live crypto flow, daily FX rates, and ingestion reliability.
            </div>
            <span class="tiny-pill">Updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
with right:
    st.metric("Live assets", str(overview["live_assets"]))
    st.metric("Daily pairs", str(overview["batch_pairs"]))
    st.metric("Pipeline availability", f"{health['uptime_pct']:.2f}%")
    st.metric("Freshness SLA", f"{health['freshness_sla_s']}s")

k1, k2, k3, k4 = st.columns(4)
k1.metric("Stream rows", str(overview["stream_rows"]))
k2.metric("Batch rows", str(overview["batch_rows"]))
k3.metric("Avg stream gap", f"{overview['avg_stream_gap']:.2f}%")
k4.metric("Avg batch change", f"{overview['avg_batch_change']:.2f}%")

st.markdown('<span class="section-pill">Live market board</span>', unsafe_allow_html=True)
render_market_cards(stream_snapshot)

chart_col, side_col = st.columns([2.1, 1])
with chart_col:
    live_df = get_live_series(
        coin=selected_coin,
        lookback_minutes=30 if selected_window == "30m" else 60 if selected_window == "1h" else 360,
    )
    render_price_vwap_chart(live_df, title=f"{selected_coin} live price vs VWAP")
with side_col:
    render_deviation_chart(stream_snapshot, title="Current deviation by asset")

st.markdown('<span class="section-pill">Daily batch rates</span>', unsafe_allow_html=True)
batch_board = batch_snapshot.rename(
    columns={"currency_pair": "pair", "rate": "price_usd", "pct_change": "change_24h"}
)
render_market_cards(batch_board)

batch_chart_col, batch_table_col = st.columns([1.3, 1])
with batch_chart_col:
    batch_df = get_batch_series(
        currency_pair=selected_pair,
        lookback_days=7 if selected_pair_window == "7d" else 14 if selected_pair_window == "14d" else 30,
    )
    render_rate_chart(batch_df, title=f"{selected_pair} daily rate movement")
with batch_table_col:
    render_event_table(batch_snapshot)

st.markdown('<span class="section-pill">Recent live tape</span>', unsafe_allow_html=True)
events = get_live_events(limit=30)
render_event_table(events)

st.markdown('<span class="section-pill">Why teams choose PulseFX</span>', unsafe_allow_html=True)
story = st.columns(3)
story[0].markdown("""
<div class="hero-box">
    <div class="hero-kicker">LIVE CONFIDENCE</div>
    <div class="hero-sub">Monitor stream quality in real time with pricing, VWAP drift, and operational recency.</div>
</div>
""", unsafe_allow_html=True)
story[1].markdown("""
<div class="hero-box">
    <div class="hero-kicker">BATCH TRUST</div>
    <div class="hero-sub">Validate transformed market rates across sources and track day-over-day currency moves.</div>
</div>
""", unsafe_allow_html=True)
story[2].markdown("""
<div class="hero-box">
    <div class="hero-kicker">OPS CONTROL</div>
    <div class="hero-sub">Detect ingestion bottlenecks quickly through lag and write-throughput history from production tables.</div>
</div>
""", unsafe_allow_html=True)

if health["latest_stream_time"] is not None:
    st.caption(f"Latest live event: {health['latest_stream_time']}")
if health["latest_batch_date"] is not None:
    st.caption(f"Latest batch date: {health['latest_batch_date']}")
