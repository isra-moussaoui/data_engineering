from __future__ import annotations

import json
import time
import logging
import requests
import os
from datetime import datetime, timezone
 
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [producer] %(message)s"
)
logger = logging.getLogger(__name__)
 
# Host execution (uv/python): localhost:19092
# Container execution: set KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC           = "currency-stream"
COINS           = ["BTC", "ETH", "SOL"]
INTERVAL        = 10        # seconds between polls
MAX_BACKOFF     = 60        # max reconnect wait in seconds
 
 
def make_producer(retries: int = 10) -> KafkaProducer:
    """Create a KafkaProducer, retrying with exponential back-off."""
    wait = 2
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",                 # wait for broker ack
                retries=5,
                request_timeout_ms=15_000,
            )
            logger.info("Connected to Kafka")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt}/{retries}), retrying in {wait}s…")
            time.sleep(wait)
            wait = min(wait * 2, MAX_BACKOFF)
    raise RuntimeError("Could not connect to Kafka after multiple attempts")
 
 
def fetch_spot(coin: str) -> dict | None:
    """Fetch a single spot price from Coinbase. Returns None on failure."""
    url = f"https://api.coinbase.com/v2/prices/{coin}-USD/spot"
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        amount = float(resp.json()["data"]["amount"])
        return {
            "coin":       coin,
            "price_usd":  amount,
            "pair":       f"{coin}/USD",
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "source":     "coinbase_stream",
        }
    except Exception as e:
        logger.warning(f"fetch_spot({coin}) failed: {e}")
        return None
 
 
def on_send_error(exc):
    logger.error(f"Kafka send error: {exc}")
 
 
def run():
    producer = make_producer()
    logger.info(f"Publishing to topic '{TOPIC}' every {INTERVAL}s")
 
    while True:
        cycle_start = time.time()
 
        for coin in COINS:
            record = fetch_spot(coin)
            if record is None:
                continue
            try:
                producer.send(TOPIC, value=record).add_errback(on_send_error)
                logger.info(f"  → {record['pair']} @ ${record['price_usd']:,.2f}")
            except KafkaTimeoutError as e:
                logger.error(f"Send timeout for {coin}: {e}")
 
        try:
            producer.flush(timeout=5)
        except KafkaTimeoutError:
            logger.warning("Flush timed out — broker may be slow")
 
        elapsed = time.time() - cycle_start
        sleep_for = max(0, INTERVAL - elapsed)
        time.sleep(sleep_for)
 
 
if __name__ == "__main__":
    run()