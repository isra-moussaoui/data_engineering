import psycopg

conn = psycopg.connect(
    "dbname=currency_db user=postgres password=postgres host=localhost port=55432"
)
cur = conn.cursor()

# Get list of tables
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public'
""")

print("Available tables:")
for row in cur.fetchall():
    print(f"  {row[0]}")

conn.close()
