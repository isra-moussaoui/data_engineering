import json
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal

import boto3
import pandas as pd
from botocore.client import Config
from sqlalchemy import select

from transformation.db import get_engine, get_session
from transformation.models import UnifiedRate
from transformation.settings import get_minio_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINIO = get_minio_settings()
BUCKET_NAME = MINIO.bucket_name

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO.endpoint,
    aws_access_key_id=MINIO.access_key,
    aws_secret_access_key=MINIO.secret_key,
    config=Config(signature_version="s3v4"),
)


def load_from_minio(source: str) -> dict:
    key = f"{source}/{date.today().isoformat()}.json"
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    data = json.loads(obj["Body"].read())
    logger.info(f"Loaded {key} from MinIO")
    return data


def ensure_unified_table(engine):
    UnifiedRate.__table__.create(bind=engine, checkfirst=True)


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
        {
            "currency_pair": f"{coin}/USD",
            "rate": float(data["amount"]),
            "base": coin,
            "quote": "USD",
        }
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


def _append_derived_forex_pairs(df_today: pd.DataFrame) -> pd.DataFrame:
    if df_today.empty:
        return df_today

    required = {"EUR/USD", "EUR/GBP", "EUR/JPY"}
    by_pair = df_today.set_index("currency_pair")

    if not required.issubset(set(by_pair.index)):
        return df_today

    eur_usd = pd.to_numeric(by_pair.at["EUR/USD", "rate"], errors="coerce")
    eur_gbp = pd.to_numeric(by_pair.at["EUR/GBP", "rate"], errors="coerce")
    eur_jpy = pd.to_numeric(by_pair.at["EUR/JPY", "rate"], errors="coerce")

    if (
        pd.isna(eur_usd)
        or pd.isna(eur_gbp)
        or pd.isna(eur_jpy)
        or eur_gbp == 0
        or eur_usd == 0
    ):
        return df_today

    template = by_pair.loc["EUR/USD"].to_dict()
    derived_rows = [
        {
            **template,
            "currency_pair": "GBP/USD",
            "base": "GBP",
            "quote": "USD",
            "rate": float(eur_usd / eur_gbp),
            "source": "frankfurter_derived",
        },
        {
            **template,
            "currency_pair": "USD/JPY",
            "base": "USD",
            "quote": "JPY",
            "rate": float(eur_jpy / eur_usd),
            "source": "frankfurter_derived",
        },
    ]

    derived_df = pd.DataFrame(derived_rows)
    combined = pd.concat([df_today, derived_df], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["currency_pair", "rate_date", "source"], keep="last"
    )
    logger.info("T1 forex: added %s derived cross pairs", len(derived_df))
    return combined


def t2_daily_change(df_today: pd.DataFrame, engine) -> pd.DataFrame:
    ensure_unified_table(engine)

    pairs = df_today["currency_pair"].tolist()
    if not pairs:
        return df_today.assign(pct_change=None, prev_rate=None)

    stmt = select(
        UnifiedRate.currency_pair,
        UnifiedRate.rate.label("prev_rate"),
    ).where(
        UnifiedRate.rate_date == date.today() - timedelta(days=1),
        UnifiedRate.currency_pair.in_(pairs),
    )

    prev_df = pd.read_sql(stmt, engine)

    df = df_today.merge(prev_df, on="currency_pair", how="left")
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    df["prev_rate"] = pd.to_numeric(df["prev_rate"], errors="coerce")
    df.loc[df["prev_rate"] <= 0, "prev_rate"] = pd.NA
    df["pct_change"] = ((df["rate"] - df["prev_rate"]) / df["prev_rate"] * 100).round(4)
    df["pct_change"] = df["pct_change"].replace([float("inf"), float("-inf")], pd.NA)

    logger.info(f"T2: computed % change for {df['pct_change'].notna().sum()} pairs")
    return df


def _normalize_nullable_numeric(value):
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() == "nan":
        return None
    if isinstance(value, Decimal) and value.is_nan():
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def t3_unify_and_load(df_forex: pd.DataFrame, df_crypto: pd.DataFrame, engine):
    df = pd.concat([df_forex, df_crypto], ignore_index=True)
    df["transformed_at"] = datetime.utcnow()

    session = get_session()
    try:
        session.query(UnifiedRate).filter(UnifiedRate.rate_date == date.today()).delete(
            synchronize_session=False
        )

        for _, row in df.iterrows():
            prev_rate = _normalize_nullable_numeric(row.get("prev_rate"))
            pct_change = _normalize_nullable_numeric(row.get("pct_change"))
            session.add(
                UnifiedRate(
                    currency_pair=row["currency_pair"],
                    base=row["base"],
                    quote=row["quote"],
                    rate=row["rate"],
                    prev_rate=prev_rate,
                    pct_change=pct_change,
                    rate_date=row["rate_date"].date()
                    if hasattr(row["rate_date"], "date")
                    else row["rate_date"],
                    source=row["source"],
                    ingested_at=row["ingested_at"],
                    transformed_at=row["transformed_at"],
                )
            )

        session.commit()
    finally:
        session.close()

    logger.info(f"T3: loaded {len(df)} rows into unified_rates")
    return df


def run_all():
    raw_forex = load_from_minio("frankfurter")
    raw_crypto = load_from_minio("coinbase")

    df_forex = t1_clean_forex(raw_forex)
    df_forex = _append_derived_forex_pairs(df_forex)
    df_crypto = t1_clean_crypto(raw_crypto)

    engine = get_engine()
    ensure_unified_table(engine)
    df_forex = t2_daily_change(df_forex, engine)
    df_crypto = t2_daily_change(df_crypto, engine)

    t3_unify_and_load(df_forex, df_crypto, engine)
    logger.info("All transformations complete")


if __name__ == "__main__":
    run_all()
