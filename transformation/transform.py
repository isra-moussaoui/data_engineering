import pandas as pd
import json
import boto3
import psycopg
import logging
from datetime import date, datetime
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = "http://localhost:9002"
BUCKET_NAME = "currency-raw"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
)

def get_pg_conn():
    return psycopg.connect(
        host="localhost", port=5432,
        dbname="currency_db", user="postgres", password="postgres"
    )

def load_from_minio(source: str) -> dict:
    key = f"{source}/{date.today().isoformat()}.json"
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    data = json.loads(obj["Body"].read())
    logger.info(f"Loaded {key} from MinIO")
    return data
def ensure_unified_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unified_rates (
            id SERIAL PRIMARY KEY,
            currency_pair VARCHAR(20),
            base VARCHAR(10),
            quote VARCHAR(10),
            rate NUMERIC(18, 6),
            prev_rate NUMERIC(18, 6),
            pct_change NUMERIC(10, 4),
            rate_date DATE,
            source VARCHAR(50),
            ingested_at TIMESTAMP,
            transformed_at TIMESTAMP
        )
    """)
    conn.commit()
# ─── Transformation 1: Clean & normalize ─────────────────────────────────────
# Standardizes column names, casts types, drops nulls, adds metadata columns.

def t1_clean_forex(raw: dict) -> pd.DataFrame:
    rows = [
        {"currency_pair": f"EUR/{k}", "rate": float(v), "base": "EUR", "quote": k}
        for k, v in raw["rates"].items()
        if v is not None
    ]
    df = pd.DataFrame(rows)
    df["rate_date"] = pd.to_datetime(raw.get("date", date.today().isoformat()))
    df["source"] = "frankfurter"
    df["ingested_at"] = pd.to_datetime(raw["_ingested_at"])
    df = df.dropna(subset=["rate"])
    df = df[df["rate"] > 0]
    logger.info(f"T1 forex: {len(df)} clean rows")
    return df

def t1_clean_crypto(raw: dict) -> pd.DataFrame:
    rows = [
        {"currency_pair": f"{coin}/USD", "rate": float(data["amount"]), "base": coin, "quote": "USD"}
        for coin, data in raw["rates"].items()
        if data.get("amount") is not None
    ]
    df = pd.DataFrame(rows)
    df["rate_date"] = pd.to_datetime(date.today().isoformat())
    df["source"] = "coinbase"
    df["ingested_at"] = pd.to_datetime(raw["_ingested_at"])
    df = df.dropna(subset=["rate"])
    df = df[df["rate"] > 0]
    logger.info(f"T1 crypto: {len(df)} clean rows")
    return df

# ─── Transformation 2: Compute daily % change ────────────────────────────────
# Pulls previous day's rates from Postgres and computes % change per pair.

def t2_daily_change(df_today: pd.DataFrame, conn) -> pd.DataFrame:
    pairs = df_today["currency_pair"].tolist()
    if not pairs:
        return df_today.assign(pct_change=None, prev_rate=None)

    placeholders = ",".join(["%s"] * len(pairs))
    query = f"""
        SELECT currency_pair, rate AS prev_rate
        FROM unified_rates
        WHERE rate_date = CURRENT_DATE - INTERVAL '1 day'
          AND currency_pair IN ({placeholders})
    """

    prev_df = pd.read_sql(query, conn, params=pairs)

    df = df_today.merge(prev_df, on="currency_pair", how="left")

    # force numeric types
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    df["prev_rate"] = pd.to_numeric(df["prev_rate"], errors="coerce")

    df["pct_change"] = (
        (df["rate"] - df["prev_rate"]) / df["prev_rate"] * 100
    ).round(4)

    logger.info(f"T2: computed % change for {df['pct_change'].notna().sum()} pairs")
    return df

# ─── Transformation 3: Unify forex + crypto into one table ───────────────────
# Merges both cleaned dataframes into a single unified schema and writes to PG.

def t3_unify_and_load(df_forex: pd.DataFrame, df_crypto: pd.DataFrame, conn):
    df = pd.concat([df_forex, df_crypto], ignore_index=True)
    df["transformed_at"] = datetime.utcnow()

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unified_rates (
            id SERIAL PRIMARY KEY,
            currency_pair VARCHAR(20),
            base VARCHAR(10),
            quote VARCHAR(10),
            rate NUMERIC(18, 6),
            prev_rate NUMERIC(18, 6),
            pct_change NUMERIC(10, 4),
            rate_date DATE,
            source VARCHAR(50),
            ingested_at TIMESTAMP,
            transformed_at TIMESTAMP
        )
    """)
    cur.execute(
        "DELETE FROM unified_rates WHERE rate_date = %s",
        (date.today(),)
    )

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO unified_rates
              (currency_pair, base, quote, rate, prev_rate, pct_change,
               rate_date, source, ingested_at, transformed_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row["currency_pair"], row["base"], row["quote"],
            row["rate"],
            row.get("prev_rate"),
            row.get("pct_change"),
            row["rate_date"].date() if hasattr(row["rate_date"], "date") else row["rate_date"],
            row["source"],
            row["ingested_at"],
            row["transformed_at"]
        ))

    conn.commit()
    logger.info(f"T3: loaded {len(df)} rows into unified_rates")
    return df

def run_all():
    raw_forex  = load_from_minio("frankfurter")
    raw_crypto = load_from_minio("coinbase")

    df_forex  = t1_clean_forex(raw_forex)
    df_crypto = t1_clean_crypto(raw_crypto)

    conn = get_pg_conn()
    ensure_unified_table(conn)
    df_forex  = t2_daily_change(df_forex,  conn)
    df_crypto = t2_daily_change(df_crypto, conn)

    t3_unify_and_load(df_forex, df_crypto, conn)
    conn.close()
    logger.info("All transformations complete")

if __name__ == "__main__":
    run_all()