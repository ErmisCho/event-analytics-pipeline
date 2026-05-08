import pandas as pd

from event_analytics_pipeline.analytics import entity_metrics, summary_metrics


def test_analytics_query_groups_correctly(tmp_path):
    parquet_path = tmp_path / "events.parquet"
    df = pd.DataFrame(
        {
            "event_id": ["evt_1", "evt_2", "evt_3"],
            "entity_id": ["entity_1", "entity_1", "entity_2"],
            "source": ["search", "search", "social"],
            "event_type": ["click", "view", "click"],
            "country": ["US", "US", "CA"],
            "impressions": [100, 50, 20],
            "clicks": [10, 5, 4],
            "cost": [20.0, 5.0, 12.0],
            "event_timestamp": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
            "ctr": [0.1, 0.1, 0.2],
            "cpc": [2.0, 1.0, 3.0],
        }
    )
    df.to_parquet(parquet_path, index=False)

    result = entity_metrics(parquet_path)

    assert len(result) == 2
    first = result.iloc[0]
    assert first["entity_id"] == "entity_1"
    assert first["source"] == "search"
    assert first["total_impressions"] == 150
    assert first["total_clicks"] == 15
    assert first["total_cost"] == 25.0
    assert first["ctr"] == 0.1
    assert first["cpc"] == 25.0 / 15


def test_analytics_query_reads_partitioned_parquet_directory(tmp_path):
    parquet_path = tmp_path / "events"
    df = pd.DataFrame(
        {
            "event_id": ["evt_1", "evt_2"],
            "entity_id": ["entity_1", "entity_2"],
            "source": ["search", "social"],
            "event_type": ["click", "view"],
            "country": ["US", "CA"],
            "impressions": [100, 50],
            "clicks": [10, 5],
            "cost": [20.0, 10.0],
            "event_timestamp": pd.to_datetime(["2025-01-01", "2025-01-02"]),
            "event_date": ["2025-01-01", "2025-01-02"],
            "ctr": [0.1, 0.1],
            "cpc": [2.0, 2.0],
        }
    )
    df.to_parquet(parquet_path, index=False, partition_cols=["event_date"])

    result = entity_metrics(parquet_path)

    assert len(result) == 2
    assert result["total_impressions"].sum() == 150
    assert result["total_clicks"].sum() == 15


def test_summary_metrics_calculates_totals_and_rates(tmp_path):
    parquet_path = tmp_path / "events.parquet"
    df = pd.DataFrame(
        {
            "event_id": ["evt_1", "evt_2", "evt_3"],
            "entity_id": ["entity_1", "entity_1", "entity_2"],
            "source": ["search", "search", "social"],
            "event_type": ["click", "view", "click"],
            "country": ["US", "US", "CA"],
            "impressions": [100, 50, 0],
            "clicks": [10, 5, 0],
            "cost": [20.0, 5.0, 12.0],
            "event_timestamp": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
            "ctr": [0.1, 0.1, 0.0],
            "cpc": [2.0, 1.0, 0.0],
        }
    )
    df.to_parquet(parquet_path, index=False)

    result = summary_metrics(parquet_path)

    assert result == {
        "total_entities": 2,
        "total_sources": 2,
        "total_impressions": 150,
        "total_clicks": 15,
        "total_cost": 37.0,
        "overall_ctr": 0.1,
        "overall_cpc": 37.0 / 15,
        "top_source_by_cost": "search",
    }


def test_summary_metrics_safely_handles_zero_denominators(tmp_path):
    parquet_path = tmp_path / "events.parquet"
    df = pd.DataFrame(
        {
            "event_id": ["evt_1"],
            "entity_id": ["entity_1"],
            "source": ["search"],
            "event_type": ["view"],
            "country": ["US"],
            "impressions": [0],
            "clicks": [0],
            "cost": [0.0],
            "event_timestamp": pd.to_datetime(["2025-01-01"]),
            "ctr": [0.0],
            "cpc": [0.0],
        }
    )
    df.to_parquet(parquet_path, index=False)

    result = summary_metrics(parquet_path)

    assert result["overall_ctr"] == 0.0
    assert result["overall_cpc"] == 0.0
