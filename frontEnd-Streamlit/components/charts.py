from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


def render_market_cards(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No live market rows available yet.")
        return

    cols = st.columns(len(df))
    for idx, row in df.reset_index(drop=True).iterrows():
        has_change = "change_24h" in row.index and pd.notna(row.get("change_24h"))
        change_value = row.get("change_24h", row.get("pct_change", row.get("pct_from_vwap", 0.0)))
        change_label = "24h" if has_change else "signal"
        delta_color = "#0f766e" if float(change_value) >= 0 else "#b91c1c"
        direction = "+" if float(change_value) >= 0 else ""
        secondary_value = row.get("vwap_1min", row.get("prev_rate", None))
        secondary_label = "VWAP 1m" if "vwap_1min" in row.index else "Prev rate"
        cols[idx].markdown(
            f"""
            <div style="
                background: linear-gradient(180deg, #ffffff 0%, #f6fafc 100%);
                border: 1px solid #dbe6ed;
                border-radius: 16px;
                padding: 14px 16px;
                box-shadow: 0 10px 20px rgba(2, 6, 23, 0.05);
            ">
                <div style="font-size: 0.9rem; color: #475569;">{row['pair']}</div>
                <div style="font-size: 1.6rem; font-weight: 700; color: #0f172a; margin-top: 2px;">
                    {f'${row["price_usd"]:,.2f}' if pd.notna(row.get('price_usd')) else 'n/a'}
                </div>
                <div style="display: flex; justify-content: space-between; margin-top: 8px;">
                    <span style="font-size: 0.85rem; color: #334155;">{secondary_label}: {f'${secondary_value:,.2f}' if pd.notna(secondary_value) else 'n/a'}</span>
                    <span style="font-size: 0.85rem; color: {delta_color}; font-weight: 600;">
                        {direction}{float(change_value):.2f}% {change_label}
                    </span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_price_vwap_chart(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        st.info("No time-series data available for the selected filters.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["event_time"],
            y=df["price_usd"],
            mode="lines",
            name="Price",
            line={"color": "#0f766e", "width": 2.6},
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["event_time"],
            y=df["vwap_1min"],
            mode="lines",
            name="VWAP 1m",
            line={"color": "#1d4ed8", "width": 2, "dash": "dot"},
        )
    )
    fig.update_layout(
        title=title,
        template="plotly_white",
        legend={"orientation": "h", "y": 1.05, "x": 0.01},
        margin={"t": 60, "r": 20, "b": 20, "l": 20},
        hovermode="x unified",
    )
    fig.update_xaxes(title_text="")
    fig.update_yaxes(title_text="USD")
    st.plotly_chart(fig, use_container_width=True)


def render_rate_chart(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        st.info("No batch data available for the selected filters.")
        return

    fig = go.Figure(
        go.Bar(
            x=df["currency_pair"],
            y=df["rate"],
            marker={"color": ["#0f766e" if str(src) == "frankfurter" else "#1d4ed8" for src in df["source"]]},
            text=[f"{v:,.3f}" for v in df["rate"]],
            textposition="outside",
        )
    )
    fig.update_layout(
        title=title,
        template="plotly_white",
        margin={"t": 60, "r": 20, "b": 20, "l": 20},
        yaxis_title="Rate",
        xaxis_title="",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_deviation_chart(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        st.info("No deviation data available.")
        return

    if "pct_from_vwap" not in df.columns:
        st.info("Deviation data is unavailable for this selection.")
        return

    color = ["#0f766e" if x >= 0 else "#dc2626" for x in df["pct_from_vwap"]]
    fig = go.Figure(
        go.Bar(
            x=df["coin"],
            y=df["pct_from_vwap"],
            marker={"color": color},
            text=[f"{v:.2f}%" for v in df["pct_from_vwap"]],
            textposition="outside",
        )
    )
    fig.update_layout(
        title=title,
        template="plotly_white",
        height=320,
        margin={"t": 60, "r": 20, "b": 20, "l": 20},
    )
    fig.update_yaxes(title_text="% from VWAP")
    st.plotly_chart(fig, use_container_width=True)


def render_deviation_timeseries(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        st.info("No deviation series available for the selected filters.")
        return

    fig = go.Figure(
        go.Scatter(
            x=df["event_time"],
            y=df["pct_from_vwap"],
            mode="lines",
            fill="tozeroy",
            line={"color": "#ea580c", "width": 2},
            fillcolor="rgba(251, 146, 60, 0.2)",
            name="% from VWAP",
        )
    )
    fig.update_layout(
        title=title,
        template="plotly_white",
        margin={"t": 55, "r": 20, "b": 20, "l": 20},
        hovermode="x unified",
        showlegend=False,
    )
    fig.update_yaxes(title_text="%")
    st.plotly_chart(fig, use_container_width=True)


def render_health_timeline(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        st.info("No operational samples available yet.")
        return

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["ingest_lag_s"],
            mode="lines",
            name="Ingest lag (s)",
            line={"color": "#dc2626", "width": 2},
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["writes_per_min"],
            mode="lines",
            name="DB writes/min",
            line={"color": "#0f766e", "width": 2.4},
        ),
        secondary_y=True,
    )
    fig.update_layout(
        title=title,
        template="plotly_white",
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.05, "x": 0.01},
        margin={"t": 60, "r": 20, "b": 20, "l": 20},
    )
    fig.update_yaxes(title_text="Lag (s)", secondary_y=False)
    fig.update_yaxes(title_text="Writes/min", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)


def render_event_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No records found for the current selection.")
        return

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "event_time": st.column_config.TextColumn("Event Time (UTC)"),
            "coin": st.column_config.TextColumn("Coin"),
            "pair": st.column_config.TextColumn("Pair"),
            "price_usd": st.column_config.NumberColumn("Price", format="$%.2f"),
            "vwap_1min": st.column_config.NumberColumn("VWAP 1m", format="$%.2f"),
            "pct_from_vwap": st.column_config.NumberColumn("% from VWAP", format="%.3f%%"),
            "rate": st.column_config.NumberColumn("Rate", format="%.4f"),
            "pct_change": st.column_config.NumberColumn("% Change", format="%.3f%%"),
            "rate_date": st.column_config.TextColumn("Rate Date"),
            "volume": st.column_config.NumberColumn("Vol", format="%.2f"),
            "source": st.column_config.TextColumn("Source"),
        },
    )
