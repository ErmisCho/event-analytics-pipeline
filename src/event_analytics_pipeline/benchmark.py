from __future__ import annotations

import argparse
import json
import os
import platform
import tempfile
import time
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd
import pyarrow

from event_analytics_pipeline.generate_events import generate_events
from event_analytics_pipeline.transform import transform_events


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("rows must be at least 1")
    return parsed


def _machine_context() -> dict[str, str | int]:
    return {
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "processor": platform.processor() or "unknown",
        "logical_cpus": os.cpu_count() or 0,
        "python_version": platform.python_version(),
        "numpy_version": np.__version__,
        "pandas_version": pd.__version__,
        "pyarrow_version": pyarrow.__version__,
    }


def run_benchmark(
    rows: int = 100_000,
    seed: int = 42,
    temp_parent: str | Path | None = None,
) -> dict[str, object]:
    """Benchmark one fresh CSV-to-partitioned-Parquet transformation."""
    if rows < 1:
        raise ValueError("rows must be at least 1")

    parent = Path.cwd() if temp_parent is None else Path(temp_parent)
    with tempfile.TemporaryDirectory(prefix="event-analytics-benchmark-", dir=parent) as temp_dir:
        root = Path(temp_dir)
        raw_path = root / "data" / "raw" / "events.csv"
        output_path = root / "data" / "processed" / "events"
        report_path = root / "data" / "processed" / "data-quality-report.json"
        rejected_path = root / "data" / "processed" / "rejected-events.csv"

        # Input generation prepares the deterministic fixture and is intentionally untimed.
        generated = generate_events(raw_path, rows=rows, seed=seed)

        started = time.perf_counter()
        written = transform_events(raw_path, output_path, report_path, rejected_path)
        runtime_seconds = time.perf_counter() - started

        rows_generated = len(generated)
        rows_written = len(written)
        if rows_written != rows_generated:
            raise RuntimeError(
                f"benchmark row-count mismatch: generated {rows_generated}, wrote {rows_written}"
            )

        output_size_bytes = sum(
            path.stat().st_size for path in output_path.rglob("*") if path.is_file()
        )

    return {
        "schema_version": 1,
        "benchmark": "csv_to_partitioned_parquet",
        "seed": seed,
        "rows_generated": rows_generated,
        "rows_written": rows_written,
        "runtime_seconds": round(runtime_seconds, 6),
        "output_size_bytes": output_size_bytes,
        "machine": _machine_context(),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark the local CSV-to-partitioned-Parquet transform."
    )
    parser.add_argument("--rows", type=_positive_int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    result = run_benchmark(rows=args.rows, seed=args.seed)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
