import psycopg

conn = psycopg.connect("dbname=currency_db user=postgres password=postgres host=localhost port=55432")
cur = conn.cursor()

# Get latest rate_date
cur.execute("SELECT MAX(rate_date) FROM exchange_rates;")
latest_date = cur.fetchone()[0]
print(f"Latest rate_date: {latest_date}\n")

# Get rows for specified currency pairs with prev_rate, pct_change, source
currency_pairs = ('BTC/USD', 'ETH/USD', 'SOL/USD', 'GBP/USD', 'USD/JPY')
print("Currency pair data on latest rate_date:")
cur.execute("""
    SELECT currency_pair, prev_rate, pct_change, source 
    FROM exchange_rates 
    WHERE rate_date = %s AND currency_pair = ANY(%s)
    ORDER BY currency_pair;
""", (latest_date, list(currency_pairs)))

for row in cur.fetchall():
    print(f"  {row[0]}: prev_rate={row[1]}, pct_change={row[2]}, source={row[3]}")

# Count where prev_rate and pct_change are 'NaN'
print()
cur.execute("""
    SELECT COUNT(*) 
    FROM exchange_rates 
    WHERE rate_date = %s AND prev_rate::text = 'NaN' AND pct_change::text = 'NaN';
""", (latest_date,))
nan_count = cur.fetchone()[0]
print(f"Count where prev_rate='NaN' and pct_change='NaN' on latest rate_date: {nan_count}")

conn.close()
