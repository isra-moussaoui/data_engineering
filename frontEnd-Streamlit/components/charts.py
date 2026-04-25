from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots


def render_market_cards(df: pd.DataFrame) -> None:
    import streamlit as st
    import pandas as pd

    if df.empty:
        st.info("No live market rows available yet.")
        return

    df = df.head(6).reset_index(drop=True)

    for i in range(0, len(df), 3):
        cols = st.columns(3)

        for j in range(len(df.iloc[i : i + 3])):
            row = df.iloc[i + j]

            # --- Change value ---
            has_change = "change_24h" in df.columns and pd.notna(row.get("change_24h"))
            change_value = row.get(
                "change_24h", row.get("pct_change", row.get("pct_from_vwap", 0.0))
            )

            try:
                change_value = float(change_value)
            except:
                change_value = 0.0

            change_label = "24h" if has_change else "signal"
            delta_color = "#02C076" if change_value >= 0 else "#F6465D"
            direction = "+" if change_value >= 0 else ""

            price = (
                f"${row['price_usd']:,.2f}" if pd.notna(row.get("price_usd")) else "n/a"
            )

            card_html = f"""
<div style="background:rgba(24,26,32,0.85);
border:1px solid rgba(255,255,255,0.05);
border-radius:10px;
padding:16px;
box-shadow:0 6px 25px rgba(0,0,0,0.15);
backdrop-filter:blur(12px);
transition:all 0.2s ease;"
onmouseover="this.style.borderColor='rgba(2,192,118,0.4)';this.style.transform='translateY(-3px)';"
onmouseout="this.style.borderColor='rgba(255,255,255,0.05)';this.style.transform='translateY(0)';">

<div style="font-size:0.9rem;color:#BBCABD;">{row["pair"]}</div>

<div style="font-size:1.7rem;font-weight:700;color:#E1E2E7;margin-top:4px;">
{price}
</div>

</div>
"""

            cols[j].markdown(card_html, unsafe_allow_html=True)


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
            line={"color": "#02C076", "width": 2.6},
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["event_time"],
            y=df["vwap_1min"],
            mode="lines",
            name="VWAP 1m",
            line={"color": "#6366F1", "width": 2, "dash": "dot"},
        )
    )
    fig.update_layout(
        title=title,
        template="plotly_dark",
        legend={"orientation": "h", "y": 1.05, "x": 0.01},
        margin={"t": 60, "r": 20, "b": 20, "l": 20},
        hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(
        title_text="",
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
        showspikes=True,
        spikemode="across",
        spikethickness=1,
        spikedash="solid",
        spikecolor="#02C076",
    )
    fig.update_yaxes(
        title_text="USD",
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
        showspikes=True,
        spikemode="across",
        spikethickness=1,
        spikedash="solid",
        spikecolor="#02C076",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_rate_chart(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        st.info("No batch data available for the selected filters.")
        return

    fig = go.Figure(
        go.Bar(
            x=df["currency_pair"],
            y=df["rate"],
            marker={
                "color": [
                    "#02C076" if str(src) == "frankfurter" else "#6366F1"
                    for src in df["source"]
                ]
            },
            text=[f"{v:,.3f}" for v in df["rate"]],
            textposition="outside",
        )
    )
    fig.update_layout(
        title=title,
        template="plotly_dark",
        margin={"t": 60, "r": 20, "b": 20, "l": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        yaxis_title="Rate",
        xaxis_title="",
    )
    fig.update_xaxes(
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
    )
    fig.update_yaxes(
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_deviation_chart(df: pd.DataFrame, title: str) -> None:
    if df.empty:
        st.info("No deviation data available.")
        return

    if "pct_from_vwap" not in df.columns:
        st.info("Deviation data is unavailable for this selection.")
        return

    color = ["#02C076" if x >= 0 else "#F6465D" for x in df["pct_from_vwap"]]
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
        template="plotly_dark",
        height=320,
        margin={"t": 60, "r": 20, "b": 20, "l": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
    )
    fig.update_yaxes(
        title_text="% from VWAP",
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
    )
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
            line={"color": "#F6465D", "width": 2},
            fillcolor="rgba(246, 70, 93, 0.2)",
            name="% from VWAP",
        )
    )
    fig.update_layout(
        title=title,
        template="plotly_dark",
        margin={"t": 55, "r": 20, "b": 20, "l": 20},
        hovermode="x unified",
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
        showspikes=True,
        spikemode="across",
        spikethickness=1,
        spikedash="solid",
        spikecolor="#F6465D",
    )
    fig.update_yaxes(
        title_text="%",
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
        showspikes=True,
        spikemode="across",
        spikethickness=1,
        spikedash="solid",
        spikecolor="#F6465D",
    )
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
            line={"color": "#F6465D", "width": 2},
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["writes_per_min"],
            mode="lines",
            name="DB writes/min",
            line={"color": "#02C076", "width": 2.4},
        ),
        secondary_y=True,
    )
    fig.update_layout(
        title=title,
        template="plotly_dark",
        hovermode="x unified",
        legend={"orientation": "h", "y": 1.05, "x": 0.01},
        margin={"t": 60, "r": 20, "b": 20, "l": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_xaxes(
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
        showspikes=True,
        spikemode="across",
        spikethickness=1,
        spikedash="solid",
        spikecolor="#6366F1",
    )
    fig.update_yaxes(
        title_text="Lag (s)",
        secondary_y=False,
        showline=True,
        linecolor="rgba(255,255,255,0.1)",
        showgrid=True,
        gridcolor="rgba(255,255,255,0.05)",
    )
    fig.update_yaxes(
        title_text="Writes/min", secondary_y=True, showline=False, showgrid=False
    )
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
            "pct_from_vwap": st.column_config.NumberColumn(
                "% from VWAP", format="%.3f%%"
            ),
            "rate": st.column_config.NumberColumn("Rate", format="%.4f"),
            "pct_change": st.column_config.NumberColumn("% Change", format="%.3f%%"),
            "rate_date": st.column_config.TextColumn("Rate Date"),
            "volume": st.column_config.NumberColumn("Vol", format="%.2f"),
            "source": st.column_config.TextColumn("Source"),
        },
    )
