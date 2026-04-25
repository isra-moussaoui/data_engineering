import logging
import json
import time
from datetime import datetime
from typing import Optional

import requests
from kafka import KafkaProducer

from transformation.settings import get_streaming_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

STREAMING = get_streaming_settings()

producer = KafkaProducer(
    bootstrap_servers=STREAMING.bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

COINS = ["BTC", "ETH", "SOL"]
TOPIC = STREAMING.topic
INTERVAL_SECONDS = STREAMING.producer_interval_seconds

def fetch_rate(coin: str) -> Optional[dict]:
    try:
        url = f"https://api.coinbase.com/v2/prices/{coin}-USD/spot"
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()["data"]
        return {
            "coin": coin,
            "price_usd": float(data["amount"]),
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.warning(f"Failed to fetch {coin}: {e}")
        return None

logger.info(f"Starting producer — publishing to topic '{TOPIC}' every {INTERVAL_SECONDS}s")

while True:
    for coin in COINS:
        record = fetch_rate(coin)
        if record:
            producer.send(TOPIC, value=record)
            logger.info(f"Published: {record}")
    producer.flush()
    time.sleep(INTERVAL_SECONDS)