from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import sys

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ═══════════════════════════════════════════════════════════════
# DAG 1 — Daily batch pipeline
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id="currency_batch_pipeline",
    default_args=default_args,
    description="Daily forex + crypto batch ingestion and transformation",
    schedule_interval="0 8 * * *",  # every day at 8am
    start_date=days_ago(1),
    catchup=False,
    tags=["currency", "batch"],
) as batch_dag:

    def task_ensure_bucket():
        from ingestion.batch_ingest import ensure_bucket

        ensure_bucket()

    def task_fetch_frankfurter():
        from ingestion.batch_ingest import fetch_frankfurter, save_to_minio

        data = fetch_frankfurter()
        save_to_minio(data, source="frankfurter")

    def task_fetch_coinbase():
        from ingestion.batch_ingest import fetch_coinbase, save_to_minio

        data = fetch_coinbase()
        save_to_minio(data, source="coinbase")

    def task_run_transforms():
        from transformation.transform import (
            load_from_minio,
            t1_clean_forex,
            t1_clean_crypto,
            t2_daily_change,
            t3_unify_and_load,
            _append_derived_forex_pairs,
        )
        from transformation.db import get_engine

        raw_forex = load_from_minio("frankfurter")
        raw_crypto = load_from_minio("coinbase")
        df_forex = t1_clean_forex(raw_forex)
        df_forex = _append_derived_forex_pairs(df_forex)
        df_crypto = t1_clean_crypto(raw_crypto)
        engine = get_engine()
        df_forex = t2_daily_change(df_forex, engine)
        df_crypto = t2_daily_change(df_crypto, engine)
        t3_unify_and_load(df_forex, df_crypto, engine)

    t0 = PythonOperator(task_id="ensure_bucket", python_callable=task_ensure_bucket)
    t1 = PythonOperator(
        task_id="fetch_frankfurter", python_callable=task_fetch_frankfurter
    )
    t2 = PythonOperator(task_id="fetch_coinbase", python_callable=task_fetch_coinbase)
    t3 = PythonOperator(task_id="run_transforms", python_callable=task_run_transforms)

    t0 >> [t1, t2] >> t3


# ═══════════════════════════════════════════════════════════════
# DAG 2 — Stream health monitor (every 5 minutes)
# ═══════════════════════════════════════════════════════════════

with DAG(
    dag_id="currency_stream_monitor",
    default_args=default_args,
    description="Check Kafka consumer lag and recent row counts",
    schedule_interval="*/5 * * * *",  # every 5 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=["currency", "streaming", "monitoring"],
) as stream_dag:

    def task_check_stream_lag():
        """
        Verifies the consumer is alive by checking that new rows have arrived
        in raw_crypto_stream within the last 2 minutes.
        Raises an exception (triggering Airflow retry/alert) if stale.
        """
        from transformation.db import get_engine
        from transformation.sql import (
            RAW_STREAM_RECENT_COUNT_SQL,
            RAW_STREAM_TABLE_EXISTS_SQL,
        )

        engine = get_engine()
        with engine.connect() as conn:
            table_name = conn.execute(RAW_STREAM_TABLE_EXISTS_SQL).scalar()
            if table_name is None:
                raise ValueError(
                    "Table public.raw_crypto_stream does not exist in currency_db. "
                    "Start the streaming consumer first (python -m streaming.consumer) "
                    "and confirm it writes into the same Postgres service."
                )

            recent_count = conn.execute(RAW_STREAM_RECENT_COUNT_SQL).scalar() or 0

        if recent_count == 0:
            raise ValueError(
                "Stream appears stalled — no rows in raw_crypto_stream in last 2 minutes"
            )
        logger.info(f"Stream healthy: {recent_count} ticks in last 2 minutes")

    def task_check_enriched_lag():
        """Verifies enriched table is also being populated."""
        from transformation.db import get_engine
        from transformation.sql import ENRICHED_RECENT_ROWS_SQL

        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(ENRICHED_RECENT_ROWS_SQL).all()

        if not rows:
            raise ValueError(
                "No enriched rows in last 5 minutes — consumer may be down"
            )
        for coin, ticks, avg in rows:
            logger.info(f"  {coin}: {ticks} ticks, avg ${avg}")

    c1 = PythonOperator(
        task_id="check_stream_lag", python_callable=task_check_stream_lag
    )
    c2 = PythonOperator(
        task_id="check_enriched_lag", python_callable=task_check_enriched_lag
    )

    c1 >> c2
