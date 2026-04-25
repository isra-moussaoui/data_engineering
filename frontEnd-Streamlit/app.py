from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

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

APP_DIR = Path(__file__).resolve().parent
LOGO_PATH = APP_DIR / "assets" / "pulsefx_logo.png"
BANNER_PATH = APP_DIR / "assets" / "pulsefx_banner.png"

st.set_page_config(
    page_title="PulseFX",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.logo(str(LOGO_PATH))

st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;600;700&family=Inter:wght@400;500;600;700&display=swap');
        .stApp {
            background-color: #111417;
        }
        h1, h2, h3 { font-family: 'Space Grotesk', sans-serif; color: #E1E2E7; }
        p, li, .stCaption, .stMarkdown { font-family: 'Inter', sans-serif; }
        
        .hero-box {
            padding: 1.5rem;
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.05);
            background: rgba(24, 26, 32, 0.8);
            backdrop-filter: blur(16px);
            box-shadow: 0 4px 30px rgba(0, 0, 0, 0.1);
            transition: border-color 0.3s ease;
        }
        .hero-box:hover {
            border-color: rgba(2, 192, 118, 0.6);
            background: rgba(2, 192, 118, 0.05);
            box-shadow: 0 0 25px rgba(2, 192, 118, 0.25);
            transform: translateY(-2px);
        }
        .hero-kicker { color: #02C076; letter-spacing: 0.08em; font-weight: 600; font-size: 0.74rem; font-family: 'Space Grotesk', sans-serif; }
        .hero-title { margin-top: 6px; font-size: 2.2rem; font-weight: 700; color: #E1E2E7; }
        .hero-sub { margin-top: 4px; color: #BBCABD; font-size: 0.95rem; font-family: 'Inter', sans-serif; }
        
        .section-pill {
            display: inline-flex;
            align-items: center;
            padding: 0.25rem 0.75rem;
            margin-bottom: 0.8rem;
            border-radius: 999px;
            background: rgba(2, 192, 118, 0.15);
            border: 1px solid rgba(2, 192, 118, 0.3);
            color: #02C076;
            font-size: 0.75rem;
            font-weight: 600;
            letter-spacing: 0.05em;
            font-family: 'Space Grotesk', sans-serif;
            text-transform: uppercase;
        }
        .tiny-pill {
            display: inline-block;
            margin-top: 15px;
            padding: 0.25rem 0.55rem;
            border-radius: 4px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            background: rgba(255, 255, 255, 0.05);
            color: #E1E2E7;
            font-size: 0.72rem;
            font-weight: 500;
        }
        
        /* Status Dot Animation */
        .status-dot {
            height: 8px;
            width: 8px;
            background-color: #02C076;
            border-radius: 50%;
            display: inline-block;
            margin-right: 8px;
            box-shadow: 0 0 8px #02C076;
            animation: pulse 1.5s infinite;
        }
        @keyframes pulse {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(2, 192, 118, 0.7); }
            70% { transform: scale(1); box-shadow: 0 0 0 6px rgba(2, 192, 118, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(2, 192, 118, 0); }
        }

        /* Professional Metric Cards */
        .metric-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1rem;
            margin-top: 1.5rem;
            margin-bottom: 2rem;
        }
        .metric-card {
            background: rgba(24, 26, 32, 0.4);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 8px;
            padding: 1.2rem;
            display: flex;
            flex-direction: column;
            justify-content: center;
            backdrop-filter: blur(8px);
            transition: border-color 0.2s ease, transform 0.2s ease;
        }
        .metric-card:hover {
            border-color: rgba(2, 192, 118, 0.3);
            transform: translateY(-2px);
        }
        .metric-label {
            color: #BBCABD;
            font-size: 0.80rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.4rem;
        }
        .metric-value {
            color: #E1E2E7;
            font-size: 1.6rem;
            font-weight: 700;
            font-family: 'Space Grotesk', sans-serif;
        }
        .metric-highlight {
            color: #02C076;
        }
        
        /* Make st.logo take full sidebar width and act as a large banner */
        [data-testid="stSidebarHeader"] {
            padding: 0 !important;
            margin: 0 !important;
        }
        [data-testid="stSidebarHeader"] > div:first-child {
            width: 100% !important;
            display: flex;
            justify-content: center;
        }
        img[data-testid="stSidebarLogo"] {
            width: 100% !important;
            height: 120px !important;
            max-height: none !important;
            max-width: 100% !important;
            object-fit: contain !important;
            margin-top: 100px !important;
        }
        
        /* Push the navigation links down so they don't overlap with the large logo */
        [data-testid="stSidebarNav"] {
            margin-top: 100px !important;
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

st.image(str(BANNER_PATH), use_container_width=True)

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
    
    <div class="metric-grid">
        <div class="metric-card">
            <div class="metric-label">Live Assets</div>
            <div class="metric-value">{overview['live_assets']}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Daily Pairs</div>
            <div class="metric-value">{overview['batch_pairs']}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Pipeline Availability</div>
            <div class="metric-value metric-highlight">{health['uptime_pct']:.2f}%</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Freshness SLA</div>
            <div class="metric-value">{health['freshness_sla_s']}s</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Stream Rows</div>
            <div class="metric-value">{overview['stream_rows']}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Batch Rows</div>
            <div class="metric-value">{overview['batch_rows']}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Avg Stream Gap</div>
            <div class="metric-value">{overview['avg_stream_gap']:.2f}%</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Avg Batch Change</div>
            <div class="metric-value">{overview['avg_batch_change']:.2f}%</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown('<span class="section-pill"><span class="status-dot"></span>Live market board</span>', unsafe_allow_html=True)
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

st.markdown('<span class="section-pill"><span class="status-dot"></span>Recent live tape</span>', unsafe_allow_html=True)
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
</br></br></br></br></br>
""", unsafe_allow_html=True)

if health["latest_stream_time"] is not None:
    st.caption(f"Latest live event: {health['latest_stream_time']}")
if health["latest_batch_date"] is not None:
    st.caption(f"Latest batch date: {health['latest_batch_date']}")
