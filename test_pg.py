import psycopg

conn = psycopg.connect(
    host="127.0.0.1",
    port=5432,
    dbname="currency_db",
    user="postgres",
    password="postgres"
)

print("Connected successfully")
conn.close()