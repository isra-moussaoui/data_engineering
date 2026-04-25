import psycopg

DSN = "host=localhost port=55432 dbname=currency_db user=postgres password=postgres"

SQL_CLEAN_NAN = """
UPDATE unified_rates
SET prev_rate = NULL
WHERE prev_rate::text = 'NaN';

UPDATE unified_rates
SET pct_change = NULL
WHERE pct_change::text = 'NaN';
"""

SQL_INSERT_GBPUSD = """
INSERT INTO unified_rates (
    currency_pair, base, quote, rate, prev_rate, pct_change,
    rate_date, source, ingested_at, transformed_at
)
SELECT
    'GBP/USD' AS currency_pair,
    'GBP' AS base,
    'USD' AS quote,
    eur_usd.rate / eur_gbp.rate AS rate,
    CASE
        WHEN eur_usd.prev_rate IS NOT NULL AND eur_gbp.prev_rate IS NOT NULL AND eur_gbp.prev_rate <> 0
        THEN eur_usd.prev_rate / eur_gbp.prev_rate
        ELSE NULL
    END AS prev_rate,
    CASE
        WHEN eur_usd.prev_rate IS NOT NULL AND eur_gbp.prev_rate IS NOT NULL AND eur_gbp.prev_rate <> 0
             AND (eur_usd.prev_rate / eur_gbp.prev_rate) <> 0
        THEN (((eur_usd.rate / eur_gbp.rate) - (eur_usd.prev_rate / eur_gbp.prev_rate)) / (eur_usd.prev_rate / eur_gbp.prev_rate)) * 100
        ELSE NULL
    END AS pct_change,
    eur_usd.rate_date,
    'derived_frankfurter' AS source,
    eur_usd.ingested_at,
    NOW()::timestamp AS transformed_at
FROM unified_rates eur_usd
JOIN unified_rates eur_gbp ON eur_usd.rate_date = eur_gbp.rate_date
WHERE eur_usd.currency_pair = 'EUR/USD'
  AND eur_gbp.currency_pair = 'EUR/GBP'
  AND eur_gbp.rate <> 0
  AND NOT EXISTS (
      SELECT 1
      FROM unified_rates target
      WHERE target.currency_pair = 'GBP/USD'
        AND target.rate_date = eur_usd.rate_date
  );
"""

SQL_INSERT_USDJPY = """
INSERT INTO unified_rates (
    currency_pair, base, quote, rate, prev_rate, pct_change,
    rate_date, source, ingested_at, transformed_at
)
SELECT
    'USD/JPY' AS currency_pair,
    'USD' AS base,
    'JPY' AS quote,
    eur_jpy.rate / eur_usd.rate AS rate,
    CASE
        WHEN eur_jpy.prev_rate IS NOT NULL AND eur_usd.prev_rate IS NOT NULL AND eur_usd.prev_rate <> 0
        THEN eur_jpy.prev_rate / eur_usd.prev_rate
        ELSE NULL
    END AS prev_rate,
    CASE
        WHEN eur_jpy.prev_rate IS NOT NULL AND eur_usd.prev_rate IS NOT NULL AND eur_usd.prev_rate <> 0
             AND (eur_jpy.prev_rate / eur_usd.prev_rate) <> 0
        THEN (((eur_jpy.rate / eur_usd.rate) - (eur_jpy.prev_rate / eur_usd.prev_rate)) / (eur_jpy.prev_rate / eur_usd.prev_rate)) * 100
        ELSE NULL
    END AS pct_change,
    eur_usd.rate_date,
    'derived_frankfurter' AS source,
    eur_usd.ingested_at,
    NOW()::timestamp AS transformed_at
FROM unified_rates eur_usd
JOIN unified_rates eur_jpy ON eur_usd.rate_date = eur_jpy.rate_date
WHERE eur_usd.currency_pair = 'EUR/USD'
  AND eur_jpy.currency_pair = 'EUR/JPY'
  AND eur_usd.rate <> 0
  AND NOT EXISTS (
      SELECT 1
      FROM unified_rates target
      WHERE target.currency_pair = 'USD/JPY'
        AND target.rate_date = eur_usd.rate_date
  );
"""

SQL_VERIFY = """
SELECT max(rate_date) AS latest_rate_date FROM unified_rates;
"""

with psycopg.connect(DSN) as conn:
    with conn.cursor() as cur:
        cur.execute(SQL_CLEAN_NAN)
        cleaned_rows = cur.rowcount

        cur.execute(SQL_INSERT_GBPUSD)
        gbpusd_inserted = cur.rowcount

        cur.execute(SQL_INSERT_USDJPY)
        usdjpy_inserted = cur.rowcount

        cur.execute(SQL_VERIFY)
        latest_rate_date = cur.fetchone()[0]

        cur.execute(
            """
            SELECT currency_pair, rate_date, prev_rate::text, pct_change::text, source
            FROM unified_rates
            WHERE currency_pair IN ('GBP/USD', 'USD/JPY', 'BTC/USD', 'ETH/USD', 'SOL/USD')
            ORDER BY rate_date DESC, currency_pair;
            """
        )
        rows = cur.fetchall()

        cur.execute("SELECT count(*) FROM unified_rates WHERE prev_rate::text='NaN'")
        prev_nan = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM unified_rates WHERE pct_change::text='NaN'")
        pct_nan = cur.fetchone()[0]

    conn.commit()

print("cleaned_nan_updates", cleaned_rows)
print("inserted_gbpusd", gbpusd_inserted)
print("inserted_usdjpy", usdjpy_inserted)
print("latest_rate_date", latest_rate_date)
print("rows", rows)
print("remaining_prev_nan", prev_nan)
print("remaining_pct_nan", pct_nan)
