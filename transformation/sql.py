from sqlalchemy import text


RAW_STREAM_TABLE_EXISTS_SQL = text("SELECT to_regclass('public.raw_crypto_stream')")
RAW_STREAM_RECENT_COUNT_SQL = text(
    """
    SELECT COUNT(*)
    FROM raw_crypto_stream
    WHERE ingested_at > NOW() - INTERVAL '2 minutes'
    """
)

ENRICHED_RECENT_ROWS_SQL = text(
    """
    SELECT coin, COUNT(*) AS ticks, ROUND(AVG(price_usd)::numeric, 2) AS avg_price
    FROM crypto_stream_enriched
    WHERE processed_at > NOW() - INTERVAL '5 minutes'
    GROUP BY coin
    """
)

STREAM_SNAPSHOT_SQL = text(
    """
    WITH latest AS (
        SELECT DISTINCT ON (coin)
            coin, pair, price_usd, vwap_1min, pct_from_vwap, event_time, processed_at
        FROM crypto_stream_enriched
        ORDER BY coin, event_time DESC
    )
    SELECT *
    FROM latest
    ORDER BY event_time DESC
    LIMIT :limit
    """
)

STREAM_TIMESERIES_SQL = text(
    """
    SELECT coin, pair, price_usd, vwap_1min, pct_from_vwap, event_time, processed_at
    FROM crypto_stream_enriched
    WHERE coin = :coin
      AND event_time >= NOW() - (:minutes * INTERVAL '1 minute')
    ORDER BY event_time ASC
    """
)

STREAM_EVENTS_SQL = text(
    """
    SELECT coin, pair, price_usd, vwap_1min, pct_from_vwap, event_time, processed_at
    FROM crypto_stream_enriched
    WHERE (:coin IS NULL OR coin = :coin)
    ORDER BY event_time DESC
    LIMIT :limit
    """
)

BATCH_SNAPSHOT_SQL = text(
    """
    WITH latest_date AS (
        SELECT MAX(rate_date) AS rate_date
        FROM unified_rates
    )
    SELECT currency_pair, base, quote, rate, prev_rate, pct_change, rate_date, source, ingested_at, transformed_at
    FROM unified_rates
    WHERE rate_date = (SELECT rate_date FROM latest_date)
    ORDER BY source, currency_pair
    LIMIT :limit
    """
)

BATCH_SERIES_SQL = text(
    """
    SELECT currency_pair, base, quote, rate, prev_rate, pct_change, rate_date, source, ingested_at, transformed_at
    FROM unified_rates
    WHERE currency_pair = :currency_pair
      AND rate_date >= CURRENT_DATE - (:days * INTERVAL '1 day')
    ORDER BY rate_date ASC
    """
)

OPS_TIMELINE_SQL = text(
    """
    WITH stream_minute AS (
        SELECT
            date_trunc('minute', processed_at) AS timestamp,
            AVG(GREATEST(EXTRACT(EPOCH FROM (processed_at - event_time)), 0)) AS ingest_lag_s,
            COUNT(*) AS writes_per_min
        FROM crypto_stream_enriched
        WHERE processed_at >= NOW() - (:window_minutes * INTERVAL '1 minute')
        GROUP BY 1
    )
    SELECT timestamp, ingest_lag_s, writes_per_min
    FROM stream_minute
    ORDER BY timestamp ASC
    """
)

PIPELINE_HEALTH_SQL = text(
    """
    WITH live AS (
        SELECT
            COUNT(*) FILTER (WHERE ingested_at >= NOW() - INTERVAL '5 minutes') AS raw_rows_5m,
            MAX(ingested_at) AS latest_raw_time
        FROM raw_crypto_stream
    ),
    enriched AS (
        SELECT
            COUNT(*) FILTER (WHERE processed_at >= NOW() - INTERVAL '5 minutes') AS enriched_rows_5m,
            MAX(processed_at) AS latest_stream_time
        FROM crypto_stream_enriched
    ),
    batch AS (
        SELECT
            MAX(rate_date) AS latest_batch_date,
            COUNT(*) FILTER (WHERE rate_date = CURRENT_DATE) AS batch_rows_today
        FROM unified_rates
    )
    SELECT * FROM live CROSS JOIN enriched CROSS JOIN batch
    """
)