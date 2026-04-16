Currency Data Engineering Pipeline
Overview

This project is an end-to-end data engineering pipeline for currency and crypto market data. It combines:

Batch ingestion of daily forex and crypto snapshots
Streaming ingestion of live crypto prices
Real-time enrichment with rolling metrics (VWAP, price deviation)
Workflow orchestration with Apache Airflow
Object storage with MinIO
Message streaming with Kafka
Relational storage with PostgreSQL

The goal is to demonstrate a production-style pipeline that supports both historical and near real-time analytics.

Architecture
Batch Pipeline
Fetches daily forex rates from Frankfurter API
Fetches crypto spot prices from Coinbase API
Stores raw JSON files in MinIO bucket: currency-raw
Scheduled daily at 08:00 UTC
Streaming Pipeline
Produces live BTC / ETH / SOL prices into Kafka
Consumes events and enriches them with:
1-minute VWAP
percentage deviation from VWAP
Stores enriched records in PostgreSQL
Monitored every 5 minutes via Airflow
Tech Stack
Python
Docker / Docker Compose
Apache Airflow
Apache Kafka + Zookeeper
PostgreSQL
MinIO (S3-compatible object storage)
Pandas / SQL
Project Structure
.
├── dags/                    # Airflow DAGs
├── ingestion/               # Batch ingestion scripts
├── streaming/               # Kafka producer / consumer
├── sql/                     # DB initialization scripts
├── Transformation
├──  tests
├── docker-compose.yml
├── requirements.txt
└── README.md
Prerequisites

Make sure you have installed:

Docker Desktop
Docker Compose
Git

Recommended:

At least 8 GB RAM available for Docker
Setup Instructions
1. Clone the repository
git clone <your-repo-url>
cd <repo-folder>
2. Start all services
docker compose up -d

This starts:

Airflow webserver
Airflow scheduler
PostgreSQL
Kafka
Zookeeper
MinIO
3. Wait for containers to initialize

Check status:

docker ps

Make sure all containers are Up.

Access Services
Airflow
URL: http://localhost:8080
Default login:
username: airflow
password: airflow
MinIO
API: http://localhost:9002
Console: http://localhost:9003
Credentials:
user: minioadmin
password: minioadmin
PostgreSQL
Host: localhost
Port: 5432
User: postgres
Password: postgres
Database: currency_db
Running the Pipelines
Batch Pipeline

In Airflow:

Enable DAG: currency_batch_pipeline
Trigger manually or wait for daily schedule

What it does:

creates MinIO bucket if missing
fetches daily forex + crypto snapshots
stores raw JSON in MinIO
Streaming Pipeline

In Airflow:

Enable DAG: currency_stream_monitor

What it does:

checks streaming consumer health every 5 minutes
monitors Kafka lag / latest rows
Verify the System
Check batch files in MinIO

Open MinIO console and verify files exist in:

currency-raw/
Check streaming data in PostgreSQL
docker exec -it <postgres-container> psql -U postgres -d currency_db -c "SELECT coin, price_usd, vwap_1min, pct_from_vwap, event_time FROM crypto_stream_enriched ORDER BY event_time DESC LIMIT 10;"

Expected:

recent BTC / ETH / SOL rows
timestamps updating every few seconds
Common Issues
MinIO connection refused

Cause:

using localhost inside Docker containers

Fix:

use Docker service name:
MINIO_ENDPOINT = "http://minio:9000"
Airflow UI not loading

Fix:

docker compose restart airflow-webserver airflow-scheduler
Containers unhealthy

Check logs:

docker logs <container-name>
Future Improvements
Add Grafana dashboard for live charts
Add anomaly alerts
Add historical warehouse layer
Add Spark Structured Streaming
Add CI/CD deployment
Contributing
Fork the repo
Create a feature branch
Commit your changes
Open a pull request
License

MIT License