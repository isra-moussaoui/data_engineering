import logging
import json
from datetime import date, datetime

import boto3
import requests
from botocore.client import Config

from transformation.settings import get_minio_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINIO = get_minio_settings()
BUCKET_NAME = MINIO.bucket_name

# Initialize MinIO client
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO.endpoint,
    aws_access_key_id=MINIO.access_key,
    aws_secret_access_key=MINIO.secret_key,
    config=Config(signature_version="s3v4"),
)


# Ensure the bucket exists
def ensure_bucket():
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if BUCKET_NAME not in existing:
        s3.create_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Created bucket: {BUCKET_NAME}")


# Save data to MinIO with a structured key
def save_to_minio(data: dict, source: str):
    today = date.today().isoformat()
    key = f"{source}/{today}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )
    logger.info(f"Saved {key} to MinIO")


def fetch_frankfurter(max_retries=3):
    """Source 1: Frankfurter — daily forex rates, EUR base"""
    url = "https://api.frankfurter.app/latest"
    params = {"from": "EUR", "to": "USD,GBP,TND,JPY,CHF,CAD,AUD,MAD"}
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            data["_ingested_at"] = datetime.utcnow().isoformat()
            data["_source"] = "frankfurter"
            logger.info(f"Frankfurter: fetched {len(data['rates'])} rates")
            return data
        except Exception as e:
            logger.warning(f"Frankfurter attempt {attempt} failed: {e}")
            if attempt == max_retries:
                raise


def fetch_coinbase(max_retries=3):
    """Source 2: Coinbase — BTC, ETH, SOL against USD"""
    results = {}
    coins = ["BTC", "ETH", "SOL"]
    for coin in coins:
        url = f"https://api.coinbase.com/v2/prices/{coin}-USD/spot"
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                results[coin] = resp.json()["data"]
                break
            except Exception as e:
                logger.warning(f"Coinbase {coin} attempt {attempt} failed: {e}")
                if attempt == max_retries:
                    raise
    payload = {
        "rates": results,
        "base": "USD",
        "_ingested_at": datetime.utcnow().isoformat(),
        "_source": "coinbase",
    }
    logger.info(f"Coinbase: fetched {len(results)} crypto rates")
    return payload


if __name__ == "__main__":
    ensure_bucket()

    forex_data = fetch_frankfurter()
    save_to_minio(forex_data, source="frankfurter")

    crypto_data = fetch_coinbase()
    save_to_minio(crypto_data, source="coinbase")

    logger.info("Batch ingestion complete")
