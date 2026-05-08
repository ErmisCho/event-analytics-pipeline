import json
from datetime import datetime

import pandas as pd

from event_analytics_pipeline.analytics import entity_metrics
from event_analytics_pipeline.transform import transform_events


def test_duplicates_are_removed_and_metrics_are_correct(tmp_path):
    raw_path = tmp_path / "events.csv"
    parquet_path = tmp_path / "events"
    report_path = tmp_path / "data-quality-report.json"
    rejected_path = tmp_path / "rejected-events.csv"
    df = pd.DataFrame(
        {
            "event_id": ["evt_1", "evt_1", "evt_2", "evt_3"],
            "entity_id": ["entity_1", "entity_1", "entity_2", "entity_3"],
            "source": ["search", "search", "social", "direct"],
            "event_type": ["click", "click", "view", "view"],
            "country": ["US", "US", "CA", "GB"],
            "impressions": [100, 100, 0, 10],
            "clicks": [10, 10, 0, 0],
            "cost": [25.0, 25.0, 3.0, 0.0],
            "event_timestamp": ["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-03"],
        }
    )
    df.to_csv(raw_path, index=False)

    result = transform_events(raw_path, parquet_path, report_path, rejected_path)

    assert len(result) == 3
    assert result["event_id"].tolist() == ["evt_1", "evt_2", "evt_3"]
    evt_1 = result[result["event_id"] == "evt_1"].iloc[0]
    assert evt_1["ctr"] == 0.1
    assert evt_1["cpc"] == 2.5
    evt_2 = result[result["event_id"] == "evt_2"].iloc[0]
    assert evt_2["ctr"] == 0.0
    assert evt_2["cpc"] == 0.0
    assert result["event_date"].tolist() == ["2025-01-01", "2025-01-02", "2025-01-03"]
    assert parquet_path.exists()
    assert (parquet_path / "event_date=2025-01-01").is_dir()
    assert (parquet_path / "event_date=2025-01-02").is_dir()
    assert list(parquet_path.rglob("*.parquet"))

    analytics = entity_metrics(parquet_path)
    assert len(analytics) == 3
    assert analytics["total_impressions"].sum() == 110

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["records_read"] == 4
    assert report["records_valid"] == 4
    assert report["records_invalid"] == 0
    assert report["records_after_deduplication"] == 3
    assert report["duplicates_removed"] == 1
    assert report["total_impressions"] == 110
    assert report["total_clicks"] == 10
    assert report["total_cost"] == 28.0
    assert report["min_event_timestamp"] == "2025-01-01T00:00:00"
    assert report["max_event_timestamp"] == "2025-01-03T00:00:00"
    datetime.fromisoformat(report["generated_at"])
    assert rejected_path.exists()
    assert pd.read_csv(rejected_path).empty


def test_data_quality_report_and_rejected_records_count_invalid_and_duplicates(tmp_path):
    raw_path = tmp_path / "events.csv"
    parquet_path = tmp_path / "events"
    report_path = tmp_path / "data-quality-report.json"
    rejected_path = tmp_path / "rejected-events.csv"
    df = pd.DataFrame(
        {
            "event_id": ["evt_1", "evt_1", "evt_2", "evt_3", "evt_4"],
            "entity_id": ["entity_1", "entity_1", "entity_2", "entity_3", "entity_4"],
            "source": ["search", "search", "social", "direct", "programmatic"],
            "event_type": ["click", "click", "view", "view", "click"],
            "country": ["US", "US", "CA", "GB", "DE"],
            "impressions": [100, 100, 10, -1, 5],
            "clicks": [10, 10, 20, 0, 1],
            "cost": [25.0, 25.0, 3.0, 1.0, 2.0],
            "event_timestamp": ["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-03", "bad-date"],
        }
    )
    df.to_csv(raw_path, index=False)

    result = transform_events(raw_path, parquet_path, report_path, rejected_path)

    assert result["event_id"].tolist() == ["evt_1"]
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["records_read"] == 5
    assert report["records_valid"] == 2
    assert report["records_invalid"] == 3
    assert report["records_after_deduplication"] == 1
    assert report["duplicates_removed"] == 1
    assert report["total_impressions"] == 100
    assert report["total_clicks"] == 10
    assert report["total_cost"] == 25.0

    rejected = pd.read_csv(rejected_path)
    assert len(rejected) == 3
    assert set(rejected["event_id"]) == {"evt_2", "evt_3", "evt_4"}
    assert "rejection_reason" in rejected.columns
