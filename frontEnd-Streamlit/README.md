# PulseFX Frontend

This folder contains the Streamlit client for the currency platform.

## What the app shows

- Live crypto flow from `crypto_stream_enriched`
- Daily market rates from `unified_rates`
- Pipeline freshness and operational health
- A sidebar with navigation pages and user controls

## Data sources

- Stream table: `crypto_stream_enriched`
- Raw stream table: `raw_crypto_stream`
- Batch table: `unified_rates`

## Run locally

```bash
uv sync
cd frontEnd-Streamlit
uv run streamlit run app.py
```

If Windows reserves a port, use the configured safe port:

```bash
uv run streamlit run app.py --server.port 20001
```

## Folder structure

```text
frontEnd-Streamlit/
	app.py
	pages/overview.py
	pages/
		stream.py
		batch.py
		health.py
	components/
	data/
	services/
	.streamlit/config.toml
```

## Product notes

- The frontend now reads from real Postgres tables populated by the stream and batch pipelines.
- Sidebar navigation is now explicitly labeled as Overview, Market Data, Live Crypto Feed, and Data Reliability & Freshness.
- The health timeline is computed from real Kafka-enriched rows (`event_time`, `processed_at`) and no synthetic values are used.
- Sidebar controls are available on all pages for live filtering.

## Requirement verification

- Sidebar navigation remains active with page links and page-specific parameters.
- Frontend design was improved with stronger visual identity, color gradients, and client-focused product messaging.
- Pages were renamed to lowercase and short names: `stream`, `batch`, `health`.
- Mock timeline data was removed and replaced with real timestamps and throughput from the stream and batch pipeline tables.
