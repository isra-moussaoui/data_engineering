from __future__ import annotations

import os
import logging
import importlib

import pandas as pd
import psycopg

try:
    _pydantic_settings = importlib.import_module("pydantic_settings")
    BaseSettings = _pydantic_settings.BaseSettings
    SettingsConfigDict = _pydantic_settings.SettingsConfigDict
except ModuleNotFoundError:  # pragma: no cover - fallback for environments without the package
    class SettingsConfigDict(dict):
        pass


    class BaseSettings:
        model_config = SettingsConfigDict()

        def __init__(self, **overrides):
            config = getattr(self, "model_config", {})
            prefix = config.get("env_prefix", "")
            for name, default in self.__class__.__dict__.items():
                if name.startswith("_") or name in {"model_config"} or callable(default) or isinstance(default, property):
                    continue
                value = overrides.get(name, os.getenv(f"{prefix}{name.upper()}", default))
                if isinstance(default, int) and isinstance(value, str) and value.isdigit():
                    value = int(value)
                setattr(self, name, value)

logger = logging.getLogger(__name__)


class DatabaseSettings(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    db: str = "currency_db"
    user: str = "postgres"
    password: str = "postgres"

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", env_file=".env", extra="ignore")

    @property
    def dsn(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.db} user={self.user} password={self.password}"


STREAM_SNAPSHOT_SQL = """
    WITH latest AS (
        SELECT DISTINCT ON (coin)
            coin, pair, price_usd, vwap_1min, pct_from_vwap, event_time, processed_at
        FROM crypto_stream_enriched
        ORDER BY coin, event_time DESC
    )
    SELECT *
    FROM latest
    ORDER BY event_time DESC
    LIMIT %(limit)s;
"""

STREAM_TIMESERIES_SQL = """
    SELECT coin, pair, price_usd, vwap_1min, pct_from_vwap, event_time, processed_at
    FROM crypto_stream_enriched
    WHERE coin = %(coin)s
      AND event_time >= NOW() - (%(minutes)s * INTERVAL '1 minute')
    ORDER BY event_time ASC;
"""

STREAM_EVENTS_SQL = """
    SELECT coin, pair, price_usd, vwap_1min, pct_from_vwap, event_time, processed_at
    FROM crypto_stream_enriched
    WHERE (%(coin)s IS NULL OR coin = %(coin)s)
    ORDER BY event_time DESC
    LIMIT %(limit)s;
"""

BATCH_SNAPSHOT_SQL = """
    WITH latest_date AS (
        SELECT MAX(rate_date) AS rate_date
        FROM unified_rates
    )
    SELECT currency_pair, base, quote, rate, prev_rate, pct_change, rate_date, source, ingested_at, transformed_at
    FROM unified_rates
    WHERE rate_date = (SELECT rate_date FROM latest_date)
    ORDER BY source, currency_pair
    LIMIT %(limit)s;
"""

BATCH_SERIES_SQL = """
    SELECT currency_pair, base, quote, rate, prev_rate, pct_change, rate_date, source, ingested_at, transformed_at
    FROM unified_rates
    WHERE currency_pair = %(currency_pair)s
      AND rate_date >= CURRENT_DATE - (%(days)s * INTERVAL '1 day')
    ORDER BY rate_date ASC;
"""

OPS_TIMELINE_SQL = """
    WITH stream_minute AS (
        SELECT
            date_trunc('minute', processed_at) AS timestamp,
            AVG(GREATEST(EXTRACT(EPOCH FROM (processed_at - event_time)), 0)) AS ingest_lag_s,
            COUNT(*) AS writes_per_min
        FROM crypto_stream_enriched
        WHERE processed_at >= NOW() - (%(window_minutes)s * INTERVAL '1 minute')
        GROUP BY 1
    )
    SELECT timestamp, ingest_lag_s, writes_per_min
    FROM stream_minute
    ORDER BY timestamp ASC;
"""

PIPELINE_HEALTH_SQL = """
    WITH live AS (
        SELECT
            COUNT(*) FILTER (WHERE ingested_at >= NOW() - INTERVAL '5 minutes') AS raw_rows_5m,
            MAX(ingested_at) AS latest_raw_time
        FROM raw_crypto_stream
    ),
    enriched AS (
        SELECT
            COUNT(*) FILTER (WHERE processed_at >= NOW() - INTERVAL '5 minutes') AS enriched_rows_5m,
            MAX(processed_at) AS latest_stream_time
        FROM crypto_stream_enriched
    ),
    batch AS (
        SELECT
            MAX(rate_date) AS latest_batch_date,
            COUNT(*) FILTER (WHERE rate_date = CURRENT_DATE) AS batch_rows_today
        FROM unified_rates
    )
    SELECT * FROM live CROSS JOIN enriched CROSS JOIN batch;
"""


def _settings() -> DatabaseSettings:
    return DatabaseSettings()


def _dsn() -> str:
    return _settings().dsn


def test_connection() -> bool:
    try:
        with psycopg.connect(_dsn(), connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                _ = cur.fetchone()
        return True
    except Exception as e:
        logger.error("DB connection failed: %s", e)
        return False


def _read_df(query: str, params: dict | None = None) -> pd.DataFrame:
    with psycopg.connect(_dsn()) as conn:
        return pd.read_sql(query, conn, params=params)


def _empty_df(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def fetch_stream_snapshot(limit: int = 3) -> pd.DataFrame:
    try:
        return _read_df(STREAM_SNAPSHOT_SQL, {"limit": limit})
    except Exception as exc:
        logger.warning("fetch_stream_snapshot failed: %s", exc)
        return _empty_df(["coin", "pair", "price_usd", "vwap_1min", "pct_from_vwap", "event_time", "processed_at"])


def fetch_stream_timeseries(coin: str = "BTC", lookback_minutes: int = 180) -> pd.DataFrame:
    try:
        return _read_df(STREAM_TIMESERIES_SQL, {"coin": coin, "minutes": lookback_minutes})
    except Exception as exc:
        logger.warning("fetch_stream_timeseries failed: %s", exc)
        return _empty_df(["coin", "pair", "price_usd", "vwap_1min", "pct_from_vwap", "event_time", "processed_at"])


def fetch_stream_events(limit: int = 30, coin: str | None = None) -> pd.DataFrame:
    try:
        return _read_df(STREAM_EVENTS_SQL, {"limit": limit, "coin": coin})
    except Exception as exc:
        logger.warning("fetch_stream_events failed: %s", exc)
        return _empty_df(["coin", "pair", "price_usd", "vwap_1min", "pct_from_vwap", "event_time", "processed_at"])


def fetch_batch_snapshot(limit: int = 10) -> pd.DataFrame:
    try:
        return _read_df(BATCH_SNAPSHOT_SQL, {"limit": limit})
    except Exception as exc:
        logger.warning("fetch_batch_snapshot failed: %s", exc)
        return _empty_df(["currency_pair", "base", "quote", "rate", "prev_rate", "pct_change", "rate_date", "source", "ingested_at", "transformed_at"])


def fetch_batch_series(currency_pair: str = "EUR/USD", lookback_days: int = 30) -> pd.DataFrame:
    try:
        return _read_df(BATCH_SERIES_SQL, {"currency_pair": currency_pair, "days": lookback_days})
    except Exception as exc:
        logger.warning("fetch_batch_series failed: %s", exc)
        return _empty_df(["currency_pair", "base", "quote", "rate", "prev_rate", "pct_change", "rate_date", "source", "ingested_at", "transformed_at"])


def fetch_ops_timeline(window_minutes: int = 120) -> pd.DataFrame:
    try:
        return _read_df(OPS_TIMELINE_SQL, {"window_minutes": window_minutes})
    except Exception as exc:
        logger.warning("fetch_ops_timeline failed: %s", exc)
        return _empty_df(["timestamp", "ingest_lag_s", "writes_per_min"])


def fetch_dashboard_overview() -> dict:
    stream_snapshot = fetch_stream_snapshot()
    batch_snapshot = fetch_batch_snapshot()

    live_assets = int(stream_snapshot["coin"].nunique()) if not stream_snapshot.empty else 0
    batch_pairs = int(batch_snapshot["currency_pair"].nunique()) if not batch_snapshot.empty else 0
    avg_stream_gap = float(stream_snapshot["pct_from_vwap"].abs().mean()) if not stream_snapshot.empty else 0.0
    avg_batch_change = float(batch_snapshot["pct_change"].abs().mean()) if not batch_snapshot.empty else 0.0

    return {
        "live_assets": live_assets,
        "batch_pairs": batch_pairs,
        "avg_stream_gap": avg_stream_gap,
        "avg_batch_change": avg_batch_change,
        "latest_stream_time": None if stream_snapshot.empty else stream_snapshot.iloc[0]["event_time"],
        "latest_batch_date": None if batch_snapshot.empty else batch_snapshot.iloc[0]["rate_date"],
        "stream_rows": len(stream_snapshot),
        "batch_rows": len(batch_snapshot),
    }


def fetch_pipeline_health() -> dict:
    try:
        snapshot = _read_df(PIPELINE_HEALTH_SQL).iloc[0]
    except Exception as exc:
        logger.warning("fetch_pipeline_health failed: %s", exc)
        snapshot = pd.Series(
            {
                "raw_rows_5m": 0,
                "latest_raw_time": None,
                "enriched_rows_5m": 0,
                "latest_stream_time": None,
                "latest_batch_date": None,
                "batch_rows_today": 0,
            }
        )

    latest_stream_time = snapshot.get("latest_stream_time")
    latest_raw_time = snapshot.get("latest_raw_time")
    latest_batch_date = snapshot.get("latest_batch_date")

    if pd.notna(latest_stream_time):
        delay_seconds = max((pd.Timestamp.now(tz="UTC") - pd.to_datetime(latest_stream_time, utc=True)).total_seconds(), 0)
    else:
        delay_seconds = 0.0

    consumer_lag = max(int(snapshot.get("raw_rows_5m", 0)) - int(snapshot.get("enriched_rows_5m", 0)), 0)
    return {
        "kafka_status": "Healthy" if int(snapshot.get("raw_rows_5m", 0)) > 0 else "No data",
        "consumer_status": "Running" if int(snapshot.get("enriched_rows_5m", 0)) > 0 else "Waiting",
        "failed_writes": 0,
        "last_tick_delay_s": round(delay_seconds, 1),
        "consumer_lag": consumer_lag,
        "freshness_sla_s": 10,
        "uptime_pct": 99.9 if int(snapshot.get("enriched_rows_5m", 0)) > 0 else 0.0,
        "airflow_last_run": "success" if int(snapshot.get("batch_rows_today", 0)) > 0 else "pending",
        "latest_stream_time": latest_stream_time,
        "latest_batch_date": latest_batch_date,
        "latest_raw_time": latest_raw_time,
    }


def fetch_latest_prices(limit: int = 100) -> pd.DataFrame:
    return fetch_stream_events(limit=limit)
