"""
streaming/consumer.py
Consumes currency-stream topic and writes validated ticks to Postgres.
Also maintains a rolling 1-minute VWAP (volume-weighted avg price) per coin.
Run:  python -m streaming.consumer
"""

from __future__ import annotations

import json
import time
import logging
import psycopg
import os
from datetime import datetime
from collections import defaultdict, deque

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [consumer] %(message)s"
)
logger = logging.getLogger(__name__)

# Host execution (uv/python): localhost:19092
# Container execution: set KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC           = "currency-stream"
GROUP_ID        = "currency-consumer-group"
MAX_BACKOFF     = 60
VWAP_WINDOW_S   = 60    # rolling window in seconds for VWAP calculation


# ── Postgres helpers ──────────────────────────────────────────────────────────

def get_pg_conn():
    return psycopg.connect(
        host="postgres", # ✅ pas localhost, car consumer tourne dans un conteneur Docker séparé
        port=5432,
        dbname="currency_db", user="postgres", password="postgres"
    )


def ensure_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_crypto_stream (
            id            SERIAL PRIMARY KEY,
            coin          VARCHAR(10)   NOT NULL,
            pair          VARCHAR(20)   NOT NULL,
            price_usd     NUMERIC(18,6) NOT NULL,
            event_time    TIMESTAMPTZ   NOT NULL,
            source        VARCHAR(50),
            ingested_at   TIMESTAMPTZ   DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto_stream_enriched (
            id            SERIAL PRIMARY KEY,
            coin          VARCHAR(10)   NOT NULL,
            pair          VARCHAR(20)   NOT NULL,
            price_usd     NUMERIC(18,6) NOT NULL,
            vwap_1min     NUMERIC(18,6),
            pct_from_vwap NUMERIC(10,4),
            event_time    TIMESTAMPTZ   NOT NULL,
            processed_at  TIMESTAMPTZ   DEFAULT NOW()
        )
    """)

    # Index for fast time-range queries on the dashboard
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_stream_coin_time
            ON raw_crypto_stream(coin, event_time DESC)
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_enriched_coin_time
            ON crypto_stream_enriched(coin, event_time DESC)
    """)

    conn.commit()
    logger.info("Tables and indexes ready")


def insert_raw(cur, record: dict):
    cur.execute("""
        INSERT INTO raw_crypto_stream (coin, pair, price_usd, event_time, source)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        record["coin"],
        record["pair"],
        record["price_usd"],
        record["timestamp"],
        record.get("source", "coinbase_stream"),
    ))


def insert_enriched(cur, coin: str, pair: str, price: float,
                    vwap: float | None, event_time: str):
    pct = round((price - vwap) / vwap * 100, 4) if vwap else None
    cur.execute("""
        INSERT INTO crypto_stream_enriched
            (coin, pair, price_usd, vwap_1min, pct_from_vwap, event_time)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (coin, pair, price, vwap, pct, event_time))


# ── Streaming transformation: rolling VWAP ───────────────────────────────────

class VWAPTracker:
    """
    Streaming T1: maintains a rolling 1-minute price window per coin.
    VWAP = mean of all prices seen in the last VWAP_WINDOW_S seconds.
    (No volume data from free API, so we treat each tick as equal weight.)
    """
    def __init__(self, window_seconds: int = VWAP_WINDOW_S):
        self.window = window_seconds
        # deque of (timestamp_epoch, price) per coin
        self._windows: dict[str, deque] = defaultdict(deque)

    def add(self, coin: str, price: float, ts: datetime) -> float | None:
        dq = self._windows[coin]
        cutoff = ts.timestamp() - self.window
        # evict old entries
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        dq.append((ts.timestamp(), price))
        if len(dq) < 2:
            return None   # not enough data yet
        return round(sum(p for _, p in dq) / len(dq), 6)


# ── Validation ────────────────────────────────────────────────────────────────

def validate(record: dict) -> bool:
    required = {"coin", "price_usd", "pair", "timestamp"}
    if not required.issubset(record):
        logger.warning(f"Missing fields: {record}")
        return False
    if record["price_usd"] <= 0:
        logger.warning(f"Non-positive price: {record}")
        return False
    if record["coin"] not in {"BTC", "ETH", "SOL"}:
        logger.warning(f"Unknown coin: {record['coin']}")
        return False
    return True


# ── Consumer loop ─────────────────────────────────────────────────────────────

def make_consumer(retries: int = 10) -> KafkaConsumer:
    wait = 2
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=-1,     # block indefinitely
                session_timeout_ms=30_000,
                heartbeat_interval_ms=10_000,
            )
            logger.info("Consumer connected to Kafka")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt}/{retries}), retrying in {wait}s…")
            time.sleep(wait)
            wait = min(wait * 2, MAX_BACKOFF)
    raise RuntimeError("Could not connect to Kafka")


def run():
    conn = get_pg_conn()
    ensure_tables(conn)

    consumer = make_consumer()
    vwap_tracker = VWAPTracker()

    processed = 0
    logger.info(f"Listening on topic '{TOPIC}'…")

    for message in consumer:
        record = message.value

        if not validate(record):
            continue

        try:
            event_time = datetime.fromisoformat(record["timestamp"])
        except ValueError:
            logger.warning(f"Bad timestamp: {record['timestamp']}")
            continue

        coin  = record["coin"]
        pair  = record["pair"]
        price = float(record["price_usd"])

        # Rolling VWAP (streaming transformation)
        vwap = vwap_tracker.add(coin, price, event_time)

        cur = conn.cursor()
        try:
            insert_raw(cur, record)
            insert_enriched(cur, coin, pair, price, vwap, record["timestamp"])
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"DB write failed: {e}")
            # reconnect if connection dropped
            try:
                conn.close()
            except Exception:
                pass
            conn = get_pg_conn()
            cur = conn.cursor()
            continue

        processed += 1
        vwap_str = f"VWAP=${vwap:,.2f}" if vwap else "VWAP=pending"
        logger.info(f"[{processed}] {pair} @ ${price:,.2f}  {vwap_str}")


if __name__ == "__main__":
    run()