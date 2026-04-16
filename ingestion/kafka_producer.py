import time
import json
import requests
import logging
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

COINS = ["BTC", "ETH", "SOL"]
TOPIC = "currency-stream"
INTERVAL_SECONDS = 10

def fetch_rate(coin: str) -> dict | None:
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