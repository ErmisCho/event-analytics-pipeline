import pytest

from event_analytics_pipeline.benchmark import main, run_benchmark


def test_benchmark_reports_expected_schema_and_rejects_invalid_rows(tmp_path):
    result = run_benchmark(rows=20, seed=7, temp_parent=tmp_path)

    assert set(result) == {
        "schema_version",
        "benchmark",
        "seed",
        "rows_generated",
        "rows_written",
        "runtime_seconds",
        "output_size_bytes",
        "machine",
    }
    assert result["schema_version"] == 1
    assert result["benchmark"] == "csv_to_partitioned_parquet"
    assert result["seed"] == 7
    assert result["rows_generated"] == result["rows_written"] == 20
    assert result["runtime_seconds"] >= 0
    assert result["output_size_bytes"] > 0
    assert set(result["machine"]) == {
        "platform",
        "architecture",
        "processor",
        "logical_cpus",
        "python_version",
        "numpy_version",
        "pandas_version",
        "pyarrow_version",
    }

    with pytest.raises(SystemExit):
        main(["--rows", "0"])
