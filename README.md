# Currency Data Engineering Pipeline

## Overview

This project is an end-to-end data engineering pipeline for currency and crypto market data.
It combines:

- Batch ingestion of daily forex and crypto snapshots
- Streaming ingestion of live crypto prices
- Real-time enrichment with rolling metrics (VWAP, price deviation)
- Workflow orchestration with Apache Airflow
- Object storage with MinIO
- Message streaming with Kafka
- Relational storage with PostgreSQL
- Interactive data visualization with Streamlit

The goal is to demonstrate a production-style pipeline that supports both historical and near real-time analytics.

## Architecture

### Batch Pipeline

- Fetches daily forex rates from Frankfurter API
- Fetches crypto spot prices from Coinbase API
- Stores raw JSON files in MinIO bucket: `currency-raw`
- Scheduled daily at 08:00 UTC

### Streaming Pipeline

- Produces live BTC / ETH / SOL prices into Kafka
- Consumes events and enriches them with:
  - 1-minute VWAP
  - Percentage deviation from VWAP
- Stores enriched records in PostgreSQL
- Monitored every 5 minutes via Airflow

### Frontend Dashboard (PulseFX)

- Interactive dashboard built with Streamlit
- Visualizes real-time metrics, pipeline health, and historical rates
- Directly queries live enriched data from PostgreSQL

## Tech Stack

- Python
- uv (package manager)
- Docker / Docker Compose
- Apache Airflow
- Apache Kafka + Zookeeper
- PostgreSQL
- MinIO (S3-compatible object storage)
- Streamlit (Frontend Dashboard)
- Pandas / SQL

## Project Structure

```text
.
├── dags/                    # Airflow DAGs
├── ingestion/               # Batch ingestion scripts
├── streaming/               # Kafka producer / consumer
├── transformation/          # Data transformation logic
├── tests/                   # Tests
├── docker-compose.yml
├── pyproject.toml           # Python dependencies (source of truth)
├── uv.lock                  # Locked dependency graph
├── requirements.txt         # Export for Docker/Airflow containers
└── README.md
```

## Prerequisites

Install:

- Docker Desktop
- Docker Compose
- Git
- uv

Recommended:

- At least 8 GB RAM available for Docker

## Dependency Management With uv

This repository now uses `uv` as the package manager.
Dependencies are defined in `pyproject.toml` and locked in `uv.lock`.

### Install dependencies

```bash
uv sync
```

### Run Python commands inside the project environment

```bash
uv run python main.py
uv run pytest
```

### Add or remove dependencies

```bash
uv add <package>
uv remove <package>
```

### Update lock file after dependency changes

```bash
uv lock
```

### Export Docker requirements (important)

Airflow services in `docker-compose.yml` still install from `requirements.txt`.
After changing dependencies, regenerate it with:

```bash
uv export --no-hashes --format requirements-txt -o requirements.txt
```

## Setup Instructions

1. Clone the repository

```bash
git clone <your-repo-url>
cd <repo-folder>
```

2. Sync Python dependencies with uv

```bash
uv sync
```

3. Start all services

```bash
docker compose up -d
```

This starts:

- Airflow webserver
- Airflow scheduler
- PostgreSQL
- Kafka
- Zookeeper
- MinIO
- Streamlit Dashboard

5. Wait for containers to initialize and verify status

```bash
docker ps
```

Make sure all containers are `Up`.

## Collaborator Run Guide (Recommended Order)

Use this sequence each time you want to run the application.

1. Install and sync local Python dependencies:

```bash
uv sync
```

2. Start all services:

```bash
docker compose up -d
```

4. Confirm services are running:

```bash
docker ps
```

5. Open Airflow and enable DAGs:

- `currency_batch_pipeline`
- `currency_stream_monitor`

6. Trigger the batch DAG once (optional) to load initial snapshots.

7. Verify enriched stream records in PostgreSQL:

```bash
docker exec -it data_engineering-postgres-1 psql -U postgres -d currency_db -c "SELECT coin, price_usd, vwap_1min, pct_from_vwap, event_time FROM crypto_stream_enriched ORDER BY event_time DESC LIMIT 10;"
```

8. For local scripts and tests, always run through uv:

```bash
uv run python main.py
uv run pytest
```

Dependency rule:

- `pyproject.toml` and `uv.lock` are the source of truth for your local development environment.
- **Docker Dependencies**: Docker containers will get their dependencies exclusively from the static `requirements.txt` file. Do not auto-export to this file, as doing so may crush or conflict with the strict Python environment required by Apache Airflow.

## Access Services

### PulseFX Dashboard (Streamlit)

- URL: http://localhost:8501
- Provides a real-time view of pipeline health, live crypto flows, and daily FX rates.

### Airflow

- URL: http://localhost:8087
- Default login:
  - Username: `admin`
  - Password: `admin`

### MinIO

- Console: http://localhost:9003
- S3 API (inside Docker network): `http://minio:9000`
- Credentials:
  - User: `minioadmin`
  - Password: `minioadmin`

### Kafka

- Bootstrap server (from host): `localhost:19092`
- Bootstrap server (inside Docker network): `kafka:29092`

### PostgreSQL

- Host: `localhost`
- Port: `5432`
- User: `postgres`
- Password: `postgres`
- Database: `currency_db`

## Running the Pipelines

### Batch Pipeline

In Airflow:

- Enable DAG: `currency_batch_pipeline`
- Trigger manually or wait for daily schedule

What it does:

- Creates MinIO bucket if missing
- Fetches daily forex + crypto snapshots
- Stores raw JSON in MinIO

### Streaming Pipeline

In Airflow:

- Enable DAG: `currency_stream_monitor`

What it does:

- Checks streaming consumer health every 5 minutes
- Monitors Kafka lag / latest rows

## Verify the System

### Check batch files in MinIO

Open the MinIO console and verify files exist in:

- `currency-raw/`

### Check streaming data in PostgreSQL

```bash
docker exec -it <postgres-container> psql -U postgres -d currency_db -c "SELECT coin, price_usd, vwap_1min, pct_from_vwap, event_time FROM crypto_stream_enriched ORDER BY event_time DESC LIMIT 10;"
```

Expected:

- Recent BTC / ETH / SOL rows
- Timestamps updating every few seconds

## Common Issues

### MinIO connection refused

Cause:

- Using `localhost` inside Docker containers

Fix:

- Use Docker service name:

```python
MINIO_ENDPOINT = "http://minio:9000"
```

### Airflow UI not loading

Fix:

```bash
docker compose restart airflow-webserver airflow-scheduler
```

### Containers unhealthy

Check logs:

```bash
docker logs <container-name>
```

## Future Improvements

- Add anomaly alerts
- Add historical warehouse layer
- Add Spark Structured Streaming
- Add CI/CD deployment

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Open a pull request

## License

MIT License
