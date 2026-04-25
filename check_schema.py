import psycopg

conn = psycopg.connect(
    "dbname=currency_db user=postgres password=postgres host=localhost port=55432"
)
cur = conn.cursor()

# Get schema for unified_rates
cur.execute("""
    SELECT column_name, data_type FROM information_schema.columns 
    WHERE table_name = 'unified_rates'
    ORDER BY ordinal_position
""")

print("Columns in unified_rates table:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
