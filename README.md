# Campaign Event Analytics Pipeline

A local campaign reporting pipeline for a fictional Austrian SME. It turns raw advertising delivery records into trustworthy campaign metrics while keeping the sample workflow local and free of personal data.

## The problem

Marketing teams often receive exports from several advertising channels with duplicate records, malformed timestamps, and impossible metrics such as more clicks than impressions. Loading those records directly into reports makes CTR, CPC, and spend totals unreliable.

This project provides a bounded batch workflow that:

- validates the incoming schema and metric invariants
- quarantines invalid records with explicit reasons
- removes duplicate events deterministically
- merges new and late-arriving events safely across reruns
- writes analytics-friendly, date-partitioned Parquet
- queries campaign metrics directly with DuckDB
- serves typed summary and campaign results through FastAPI

The generated dataset is synthetic. It contains campaign identifiers and coarse country codes, but no names, email addresses, device identifiers, or other personal data.

## Architecture

```text
Local: raw CSV -> validation -> transformation -> Parquet -> DuckDB SQL -> FastAPI
```

Pipeline files:

- `src/event_analytics_pipeline/generate_events.py` creates reproducible sample input at `data/raw/events.csv`
- `src/event_analytics_pipeline/validation.py` enforces the input contract and metric invariants
- `src/event_analytics_pipeline/transform.py` quarantines invalid rows, incrementally merges valid events, calculates CTR/CPC, and replaces partitioned Parquet only after its successor is written
- `src/event_analytics_pipeline/analytics.py` aggregates Parquet data with DuckDB SQL
- `src/event_analytics_pipeline/api.py` exposes the aggregates through FastAPI

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows PowerShell/CMD
# source .venv/bin/activate  # macOS/Linux
python -m pip install -e ".[dev]"
```

## Run the pipeline

```bash
python -m event_analytics_pipeline
```

This generates deterministic sample data, transforms it, and prints verified summary analytics. To run the stages separately:

```bash
python -m event_analytics_pipeline.generate_events
python -m event_analytics_pipeline.transform
```

The transform writes:

- partitioned events under `data/processed/events/event_date=YYYY-MM-DD/`
- `data/processed/data-quality-report.json` with input, rejection, duplicate, metric, and timestamp totals
- `data/processed/rejected-events.csv` with each invalid record and its rejection reason

Date partitions let analytical queries avoid scanning unrelated days. Keeping rejected records makes quality failures reviewable without allowing bad rows to corrupt valid aggregates.

## Query analytics

```bash
python -c "from event_analytics_pipeline.analytics import entity_metrics, summary_metrics; print(entity_metrics().head()); print(summary_metrics())"
```

## Run the API

```bash
python -m uvicorn event_analytics_pipeline.api:app --reload
```

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/analytics/summary
curl "http://127.0.0.1:8000/analytics/entities?source=search&limit=10"
```

The interactive OpenAPI documentation is available at `http://127.0.0.1:8000/docs`. Entity filters are exact matches; `limit` defaults to 100 and accepts values from 1 to 1000.

## Tests

```bash
python -m pytest
```

Tests cover schema and metric validation, rejected records, deterministic deduplication, safe CTR/CPC calculations, partitioned Parquet queries, API filtering, typed responses, and missing-data errors. GitHub Actions runs the suite on Python 3.10 and 3.13.

## Local benchmark

Run a fresh deterministic CSV-to-Parquet measurement:

```bash
python -m event_analytics_pipeline.benchmark --rows 100000 --seed 42
```

Input generation is excluded from the timer. The command reports the transform runtime, generated/written rows, Parquet bytes, and machine/package context as JSON.

One observed run on 2026-07-23 produced:

| Rows | Transform time | Parquet size | Environment |
|---:|---:|---:|---|
| 100,000 | 13.426296 s | 5,372,741 bytes (5.12 MiB) | Windows 11, AMD64 Family 25 Model 97, 24 logical CPUs, Python 3.13.1, pandas 2.3.2, NumPy 2.3.2, PyArrow 24.0.0 |

This is one local observation of a bounded workload, not a throughput, scalability, production-capacity, or SLA claim. Deterministic input makes results reproducible logically; runtime and output bytes still vary by hardware, filesystem, package versions, and system load.

## Engineering decisions

- **Invalid schema vs. invalid row:** missing required columns stop the batch because the contract is unknown; malformed rows are quarantined so valid records can continue.
- **Aggregate rates:** summary CTR and CPC are calculated from aggregate totals rather than averaged row-level rates.
- **Parquet and DuckDB:** they provide columnar storage and analytical SQL without operating a database server, which fits this local bounded workload.
- **Idempotent reruns:** accepted event IDs are immutable; repeated IDs keep their first accepted value, while unique late events are inserted regardless of event date. The replacement dataset is fully written before the prior dataset is removed.
- **Deterministic input:** the generator has a fixed default seed so failures and measurements can be reproduced.
- **Local data minimisation:** the sample model excludes direct identifiers. This reduces privacy risk but is not, by itself, a claim of GDPR compliance.

## Scaling and trade-offs

- **Batch transform:** pandas loads the raw CSV and, on incremental runs, the existing Parquet dataset into memory. Rejection reasons are calculated row by row with `DataFrame.apply`, then the merged dataset is sorted and fully rewritten to a temporary partitioned dataset before replacement. This avoids replacing the dataset with an incomplete write, but makes memory, CPU time, and rewrite I/O grow with the full dataset rather than only the new batch.
- **Parquet layout:** the full rewrite emits files under one directory per `event_date`. Partitioning can reduce reads only when a date predicate reaches the Parquet scan; the current analytics queries use a recursive glob with no date predicate, so they scan every date partition. As dates accumulate, small per-date files can make file discovery and planning significant relative to useful data reads.
- **DuckDB and API queries:** every analytics request opens a fresh in-memory DuckDB connection and scans Parquet. The entities endpoint aggregates all entity/source groups first; its `source`, `entity_id`, and `limit` filters are then applied to the returned pandas frame, so they do not reduce the underlying scan or aggregation. FastAPI handlers are synchronous and perform this local blocking work during each request; this is suitable for a local demo, not a production serving claim.

Use measurements to decide when to change the design rather than choosing an arbitrary row-count threshold:

1. Record transform wall time and peak RSS for representative fresh and incremental batches, and compare them with available memory and the required batch window. If either constraint is missed, first vectorise the row-wise rejection checks, read only needed columns or chunks, and rewrite only affected partitions where the immutable-ID contract permits it.
2. Profile DuckDB queries and track files and bytes scanned versus files and bytes needed, plus file-discovery/planning time versus execution time. Scan amplification calls for predicate and column pushdown; planning dominated by many small files calls for compaction or a coarser partition layout.
3. Load-test endpoint p95 latency, errors, and resource saturation at the expected concurrency. If the measured service target is missed, first push API filters and limits into SQL, add suitable pre-aggregations or caching, and avoid redundant scans before changing the serving architecture.

Optimise and remeasure the single-machine path first. Consider distributed processing or cloud services only when those local changes still cannot meet measured memory, batch-window, scan, or latency/concurrency requirements. The AWS mapping below is conceptual only; this repository does not claim AWS or Spark implementation experience.

## Privacy and operations

- Event timestamps are normalised to UTC before date partitions are derived. Explicit offsets are converted to UTC; timestamps without an offset are defined by this input contract as UTC.
- Generated files stay local and contain no direct identifiers. Country is the coarsest location field.
- The demo applies no automatic retention: its synthetic raw, processed, and rejected files are disposable and remain until the operator deletes or regenerates them. Real data requires an explicit purpose-based retention policy.
- Rejected rows retain their original values for diagnosis, so a real deployment must protect and expire that quarantine like the raw source.
- A failed dataset write leaves the last valid Parquet dataset in place; the batch can be rerun from raw input.
- GDPR compliance depends on the real data, processing purpose, access controls, retention, and deployment. This local sample does not claim compliance.

## Current boundaries

- Each incremental run reads the existing dataset into memory before merging, so the implementation remains bounded to one machine.
- Correcting an accepted event requires event-version semantics that are outside the current immutable-ID contract.
- The sample data is synthetic and small enough for one machine.
- The API has no authentication because it is only intended for local use.
- The dataset directory, rejected CSV, and quality report are not a cross-file transaction; concurrent writes and process interruption during the directory swap are outside this demo's guarantees.
- There is no scheduler, deployment configuration, or production monitoring.
- Privacy and retention obligations would depend on the real data and deployment context.

These are deliberate boundaries, not production claims. A change in scale or operating requirements should be justified with measurements before introducing distributed processing or managed infrastructure.

## AWS mapping

This repository does not call AWS services. The conceptual production mapping is:

```text
AWS: S3 -> Glue/EMR Spark -> Parquet on S3 -> Athena/Redshift -> API/dashboard -> CloudWatch
```

- local raw CSV -> raw events in S3
- pandas validation and transformation -> Glue or EMR Spark
- partitioned local Parquet -> partitioned Parquet on S3
- DuckDB SQL -> Athena or Redshift
- FastAPI -> an API or dashboard backend
- local quality reports and logs -> CloudWatch monitoring and production data-quality checks

This mapping explains equivalent responsibilities; it does not represent hands-on AWS or Spark implementation in this repository.
