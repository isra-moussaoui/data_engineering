from __future__ import annotations

from services.db import (
    fetch_batch_series,
    fetch_batch_snapshot,
    fetch_dashboard_overview,
    fetch_ops_timeline,
    fetch_pipeline_health,
    fetch_stream_events,
    fetch_stream_snapshot,
    fetch_stream_timeseries,
)


def get_dashboard_overview():
    return fetch_dashboard_overview()


def get_pipeline_health():
    return fetch_pipeline_health()


def get_ops_timeline(window_minutes: int = 120):
    return fetch_ops_timeline(window_minutes=window_minutes)


def get_live_snapshot(limit: int = 3):
    return fetch_stream_snapshot(limit=limit)


def get_live_series(coin: str = "BTC", lookback_minutes: int = 180):
    return fetch_stream_timeseries(coin=coin, lookback_minutes=lookback_minutes)


def get_live_events(limit: int = 30, coin: str | None = None):
    return fetch_stream_events(limit=limit, coin=coin)


def get_batch_snapshot(limit: int = 10):
    return fetch_batch_snapshot(limit=limit)


def get_batch_series(currency_pair: str = "EUR/USD", lookback_days: int = 30):
    return fetch_batch_series(currency_pair=currency_pair, lookback_days=lookback_days)
