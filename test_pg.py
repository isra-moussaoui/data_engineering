import psycopg

cconn = psycopg.connect(
    host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "currency_db"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)

print("Connected successfully")
conn.close()
