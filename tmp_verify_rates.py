import psycopg

conn = psycopg.connect(
    "host=localhost port=55432 dbname=currency_db user=postgres password=postgres"
)
cur = conn.cursor()

cur.execute("select max(rate_date) from unified_rates")
latest = cur.fetchone()[0]
print("latest_rate_date", latest)

cur.execute(
    """
    select currency_pair, prev_rate::text, pct_change::text, source
    from unified_rates
    where rate_date = %s
      and currency_pair in ('BTC/USD','ETH/USD','SOL/USD','GBP/USD','USD/JPY')
    order by currency_pair
    """,
    (latest,),
)
print("rows", cur.fetchall())

cur.execute(
    "select count(*) from unified_rates where rate_date=%s and prev_rate::text='NaN'",
    (latest,),
)
print("prev_rate_nan_rows", cur.fetchone()[0])

cur.execute(
    "select count(*) from unified_rates where rate_date=%s and pct_change::text='NaN'",
    (latest,),
)
print("pct_change_nan_rows", cur.fetchone()[0])

cur.close()
conn.close()
