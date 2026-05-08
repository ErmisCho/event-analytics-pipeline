# event-analytics-pipeline

A small, interview-ready Python data engineering demo that simulates an event analytics pipeline for product analytics, advertising analytics, SaaS usage analytics, or backend event processing roles.

## Purpose

This project demonstrates a compact local pipeline with common data engineering patterns:

- raw event generation
- data validation
- transformation and metric calculation
- Parquet storage
- SQL analytics with DuckDB
- API access with FastAPI
- automated tests with pytest

## Architecture

```text
Local: raw CSV -> validation -> transformation -> Parquet -> DuckDB SQL -> FastAPI
```

Pipeline files:

- `src/event_analytics_pipeline/generate_events.py` creates `data/raw/events.csv`
- `src/event_analytics_pipeline/validation.py` validates required schema and metric quality
- `src/event_analytics_pipeline/transform.py` rejects invalid rows, deduplicates valid events, parses `event_timestamp`, adds `event_date`, computes `ctr` and `cpc`, writes partitioned Parquet under `data/processed/events/`, creates `data/processed/data-quality-report.json`, and writes `data/processed/rejected-events.csv`
- `src/event_analytics_pipeline/analytics.py` queries Parquet with DuckDB
- `src/event_analytics_pipeline/api.py` exposes analytics through FastAPI

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows PowerShell/CMD
# source .venv/bin/activate  # macOS/Linux
python -m pip install -e ".[dev]"
```

## Run the pipeline

```bash
python -m event_analytics_pipeline.generate_events
python -m event_analytics_pipeline.transform
```

## Data quality and rejected records

The transform step writes partitioned Parquet files under `data/processed/events/event_date=YYYY-MM-DD/`. This mirrors common S3 data lake partitioning, where queries can scan only the date folders they need instead of a single large file.

It also writes `data/processed/data-quality-report.json` with records read, valid/invalid counts, duplicates removed, metric totals, timestamp range, and report generation time. Invalid rows are written to `data/processed/rejected-events.csv` with a `rejection_reason`.

In production data pipelines, this pattern helps teams monitor data freshness and volume, identify duplicate spikes, quarantine bad records without losing them, and prevent invalid inputs from silently corrupting downstream analytics.

## Run analytics in Python

```bash
python -c "from event_analytics_pipeline.analytics import entity_metrics, summary_metrics; print(entity_metrics().head()); print(summary_metrics())"
```

## Run the API

```bash
python -m uvicorn event_analytics_pipeline.api:app --reload
```

API endpoints:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/analytics/summary
curl http://127.0.0.1:8000/analytics/entities
# In Windows PowerShell, use curl.exe if curl is aliased to Invoke-WebRequest.
```

`/analytics/summary` returns compact portfolio-level totals, safe overall CTR/CPC calculations, and the top source by total cost.

Optional analytics filters use exact matches and return a typed JSON response with entity/source metrics:

```bash
# Default limit is 100. Valid range is 1 to 1000.
curl "http://127.0.0.1:8000/analytics/entities?limit=10"
curl "http://127.0.0.1:8000/analytics/entities?source=search"
curl "http://127.0.0.1:8000/analytics/entities?entity_id=entity_001"
curl "http://127.0.0.1:8000/analytics/entities?source=social&limit=5"
```

## Tests

```bash
python -m pytest
```

The tests cover:

- missing columns fail validation
- invalid metric values fail validation
- duplicates are removed
- `ctr` and `cpc` calculations are correct
- analytics query groups correctly
- summary analytics totals, safe rates, and API response shape

## Interview talking points

1. **What the project demonstrates**: a compact local analytics pipeline with raw ingestion, validation, transformation, partitioned Parquet storage, SQL analytics, API serving, and tests.
2. **Why validation happens before transformation**: schema and metric checks prevent missing columns, invalid timestamps, negative values, and impossible click/impression relationships from corrupting downstream outputs.
3. **Why invalid records are separated**: rejected rows are kept with a reason so bad data can be reviewed without blocking all valid data from being processed.
4. **Why data quality reporting matters**: the report makes record counts, invalid rows, duplicate removal, metric totals, and timestamp ranges visible for debugging and pipeline monitoring.
5. **Why Parquet is used**: Parquet is a common columnar analytics format that scans efficiently, compresses well, and supports partitioned data lake layouts.
6. **Why DuckDB is useful locally**: DuckDB can run analytical SQL directly on local Parquet files without a database server or cloud warehouse.
7. **Why FastAPI is used as a serving layer**: FastAPI provides a small typed HTTP interface for exposing analytics results to other tools or services.
8. **How this maps to AWS**: local CSV maps to S3 raw data, pandas transformation maps conceptually to Glue or EMR jobs, partitioned Parquet maps to S3 data lake storage, DuckDB SQL maps to Athena or Redshift, FastAPI maps to an API or dashboard backend, and local reports/logs map to CloudWatch-style monitoring.
9. **What would change in production**: add orchestration, incremental loads, stronger schema evolution, partition management, observability, alerting, access control, secrets management, CI/CD, and cost/performance tuning.
10. **Honest limitations**: this is a compact local demo of production-style data engineering patterns; it is not production-ready, does not use real AWS services, does not run Spark, and should not be presented as production AWS or Spark experience.

## AWS mapping

This demo does not call real AWS services. A production-style AWS version could map as:

```text
AWS: S3 -> Glue/EMR Spark -> Parquet on S3 -> Athena/Redshift -> API/dashboard -> CloudWatch
```

Local-to-AWS concepts:

- `data/raw/events.csv` -> raw events in S3
- pandas validation/transformation -> Glue or EMR Spark jobs
- local `data/processed/events/event_date=.../` Parquet partitions -> partitioned Parquet datasets on S3
- DuckDB SQL -> Athena or Redshift queries
- FastAPI -> API service or dashboard backend
- local logs/tests -> CloudWatch monitoring and production data quality checks

## What this is not

- Not production AWS experience
- Not a real Spark cluster
- Not a fully managed production data platform
- A compact local demo of data engineering patterns
