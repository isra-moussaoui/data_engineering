"""
streaming/consumer.py
Consumes currency-stream topic and writes validated ticks to Postgres.
Also maintains a rolling 1-minute VWAP (volume-weighted avg price) per coin.
Run:  python -m streaming.consumer
"""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime
import json
import logging
import os
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from transformation.db import get_engine, get_session
from transformation.models import Base, CryptoStreamEnriched, RawCryptoStream
from transformation.settings import get_streaming_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [consumer] %(message)s",
)
logger = logging.getLogger(__name__)

STREAMING = get_streaming_settings()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", STREAMING.bootstrap_servers)
TOPIC = STREAMING.topic
GROUP_ID = STREAMING.group_id
MAX_BACKOFF = STREAMING.max_backoff_seconds
VWAP_WINDOW_S = STREAMING.vwap_window_seconds


def ensure_tables(engine):
    Base.metadata.create_all(engine, tables=[RawCryptoStream.__table__, CryptoStreamEnriched.__table__])
    logger.info("Tables and indexes ready")


def insert_raw(session, record: dict, event_time: datetime):
    session.add(
        RawCryptoStream(
            coin=record["coin"],
            pair=record["pair"],
            price_usd=record["price_usd"],
            event_time=event_time,
            source=record.get("source", "coinbase_stream"),
        )
    )


def insert_enriched(session, coin: str, pair: str, price: float, vwap: float | None, event_time: datetime):
    pct = round((price - vwap) / vwap * 100, 4) if vwap else None
    session.add(
        CryptoStreamEnriched(
            coin=coin,
            pair=pair,
            price_usd=price,
            vwap_1min=vwap,
            pct_from_vwap=pct,
            event_time=event_time,
        )
    )


class VWAPTracker:
    """
    Streaming T1: maintains a rolling 1-minute price window per coin.
    VWAP = mean of all prices seen in the last VWAP_WINDOW_S seconds.
    (No volume data from free API, so we treat each tick as equal weight.)
    """

    def __init__(self, window_seconds: int = VWAP_WINDOW_S):
        self.window = window_seconds
        self._windows: dict[str, deque] = defaultdict(deque)

    def add(self, coin: str, price: float, ts: datetime) -> float | None:
        dq = self._windows[coin]
        cutoff = ts.timestamp() - self.window
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        dq.append((ts.timestamp(), price))
        if len(dq) < 2:
            return None
        return round(sum(p for _, p in dq) / len(dq), 6)


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
                consumer_timeout_ms=-1,
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
    engine = get_engine()
    ensure_tables(engine)

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

        coin = record["coin"]
        pair = record["pair"]
        price = float(record["price_usd"])

        vwap = vwap_tracker.add(coin, price, event_time)

        session = get_session()
        try:
            insert_raw(session, record, event_time)
            insert_enriched(session, coin, pair, price, vwap, event_time)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"DB write failed: {e}")
            continue
        finally:
            session.close()

        processed += 1
        vwap_str = f"VWAP=${vwap:,.2f}" if vwap else "VWAP=pending"
        logger.info(f"[{processed}] {pair} @ ${price:,.2f}  {vwap_str}")


if __name__ == "__main__":
    run()