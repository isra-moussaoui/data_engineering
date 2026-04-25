from __future__ import annotations

import pandas as pd
import streamlit as st

from components.charts import render_event_table, render_health_timeline
from data.sources import get_batch_snapshot, get_live_events, get_ops_timeline, get_pipeline_health

st.title("Data Reliability & Freshness")
st.caption("Operational reliability and freshness from production stream and batch tables")

st.sidebar.subheader("health filters")
window = st.sidebar.selectbox("timeline window", options=["30m", "2h", "6h"], index=1)
limit = st.sidebar.slider("rows shown", min_value=10, max_value=80, value=35, step=5)
window_minutes = 30 if window == "30m" else 120 if window == "2h" else 360

health = get_pipeline_health()
events = get_live_events(limit=limit)
batch = get_batch_snapshot(limit=min(20, max(6, limit // 2)))
timeline = get_ops_timeline(window_minutes=window_minutes)

st.markdown(
    """
    <style>
        .stApp {
            background:
                radial-gradient(760px 260px at 5% -10%, rgba(22, 163, 74, 0.10), transparent 60%),
                radial-gradient(760px 260px at 95% -15%, rgba(220, 38, 38, 0.08), transparent 60%);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

cards = st.columns(4)
cards[0].metric("availability", f"{health['uptime_pct']:.2f}%")
cards[1].metric("freshness sla", f"{health['freshness_sla_s']}s")
cards[2].metric("last tick delay", f"{health['last_tick_delay_s']}s")
cards[3].metric("consumer lag", str(health["consumer_lag"]))

#status = st.columns(2)
#status[0].success(f"kafka: {health['kafka_status']}")
#status[1].success(f"consumer: {health['consumer_status']}")

if not timeline.empty:
    timeline["timestamp"] = pd.to_datetime(timeline["timestamp"], utc=True, errors="coerce")
    timeline["ingest_lag_s"] = pd.to_numeric(timeline["ingest_lag_s"], errors="coerce").fillna(0.0)
    timeline["writes_per_min"] = pd.to_numeric(timeline["writes_per_min"], errors="coerce").fillna(0.0)
    render_health_timeline(timeline[["timestamp", "ingest_lag_s", "writes_per_min"]], title="real freshness pulse")
else:
    st.info("No stream samples found in the selected window.")

st.subheader("latest stream rows")
render_event_table(events)

st.subheader("latest batch rows")
render_event_table(batch)

if health["latest_stream_time"] is not None:
    st.caption(f"latest stream timestamp: {health['latest_stream_time']}")
if health["latest_batch_date"] is not None:
    st.caption(f"latest batch date: {health['latest_batch_date']}")
