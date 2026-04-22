import json
import logging
import psycopg
from kafka import KafkaConsumer
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

conn = psycopg.connect(
    host="postgres",
    port=5432,
    dbname="currency_db",
    user="postgres",
    password="postgres"
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS raw_crypto_stream (
    id SERIAL PRIMARY KEY,
    coin VARCHAR(10),
    price_usd NUMERIC(18, 6),
    event_time TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT NOW()
)
""")
conn.commit()

consumer = KafkaConsumer(
    "currency-stream",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

logger.info("Consumer started - listening for messages...")

for message in consumer:
    record = message.value
    cur.execute(
        """
        INSERT INTO raw_crypto_stream (coin, price_usd, event_time)
        VALUES (%s, %s, %s)
        """,
        (
            record["coin"],
            record["price_usd"],
            datetime.fromisoformat(record["timestamp"])
        )
    )
    conn.commit()
    logger.info(f"Stored: {record['coin']} @ ${record['price_usd']}")