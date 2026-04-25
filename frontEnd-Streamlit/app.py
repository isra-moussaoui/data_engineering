from __future__ import annotations

from pathlib import Path

import streamlit as st

APP_DIR = Path(__file__).resolve().parent

st.set_page_config(
    page_title="PulseFX",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="expanded",
)

pages = st.navigation(
    [
        st.Page(str(APP_DIR / "pages" / "overview.py"), title="Overview", default=True),
        st.Page(str(APP_DIR / "pages" / "batch.py"), title="Market Data"),
        st.Page(str(APP_DIR / "pages" / "stream.py"), title="Live Crypto Feed"),
        st.Page(str(APP_DIR / "pages" / "health.py"), title="Data Reliability & Freshness"),
    ]
)

pages.run()
