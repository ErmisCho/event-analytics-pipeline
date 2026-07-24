import duckdb
import pandas as pd
from fastapi.testclient import TestClient

from event_analytics_pipeline import api


def sample_metrics(source=None, entity_id=None, limit=None) -> pd.DataFrame:
    df = pd.DataFrame(
        [
            {
                "entity_id": "entity_1",
                "source": "search",
                "total_impressions": 100,
                "total_clicks": 10,
                "total_cost": 25.0,
                "ctr": 0.1,
                "cpc": 2.5,
            },
            {
                "entity_id": "entity_2",
                "source": "social",
                "total_impressions": 200,
                "total_clicks": 20,
                "total_cost": 15.0,
                "ctr": 0.1,
                "cpc": 0.75,
            },
            {
                "entity_id": "entity_1",
                "source": "direct",
                "total_impressions": 50,
                "total_clicks": 5,
                "total_cost": 5.0,
                "ctr": 0.1,
                "cpc": 1.0,
            },
        ]
    )
    if source is not None:
        df = df[df["source"] == source]
    if entity_id is not None:
        df = df[df["entity_id"] == entity_id]
    return df if limit is None else df.head(limit)


def sample_summary() -> dict[str, int | float | str]:
    return {
        "total_entities": 2,
        "total_sources": 3,
        "total_impressions": 350,
        "total_clicks": 35,
        "total_cost": 45.0,
        "overall_ctr": 0.1,
        "overall_cpc": 45.0 / 35,
        "top_source_by_cost": "search",
    }


def client_with_fake_metrics(monkeypatch) -> TestClient:
    monkeypatch.setattr(api, "entity_metrics", sample_metrics)
    monkeypatch.setattr(api, "summary_metrics", sample_summary)
    return TestClient(api.app)


def test_analytics_summary_returns_compact_totals(monkeypatch):
    client = client_with_fake_metrics(monkeypatch)

    response = client.get("/analytics/summary")

    assert response.status_code == 200
    assert response.json() == {
        "total_entities": 2,
        "total_sources": 3,
        "total_impressions": 350,
        "total_clicks": 35,
        "total_cost": 45.0,
        "overall_ctr": 0.1,
        "overall_cpc": 45.0 / 35,
        "top_source_by_cost": "search",
    }


def test_analytics_entities_default_behavior_returns_clean_json(monkeypatch):
    client = client_with_fake_metrics(monkeypatch)

    response = client.get("/analytics/entities")

    assert response.status_code == 200
    assert len(response.json()) == 3
    assert response.json()[0] == {
        "entity_id": "entity_1",
        "source": "search",
        "total_impressions": 100,
        "total_clicks": 10,
        "total_cost": 25.0,
        "ctr": 0.1,
        "cpc": 2.5,
    }


def test_analytics_entities_filters_by_source(monkeypatch):
    client = client_with_fake_metrics(monkeypatch)

    response = client.get("/analytics/entities?source=social")

    assert response.status_code == 200
    assert [row["source"] for row in response.json()] == ["social"]


def test_analytics_entities_filters_by_entity_id(monkeypatch):
    client = client_with_fake_metrics(monkeypatch)

    response = client.get("/analytics/entities?entity_id=entity_1")

    assert response.status_code == 200
    assert [row["entity_id"] for row in response.json()] == ["entity_1", "entity_1"]


def test_analytics_entities_applies_limit(monkeypatch):
    client = client_with_fake_metrics(monkeypatch)

    response = client.get("/analytics/entities?limit=2")

    assert response.status_code == 200
    assert len(response.json()) == 2


def test_analytics_entities_validates_limit(monkeypatch):
    client = client_with_fake_metrics(monkeypatch)

    assert client.get("/analytics/entities?limit=0").status_code == 422
    assert client.get("/analytics/entities?limit=-1").status_code == 422
    assert client.get("/analytics/entities?limit=1001").status_code == 422


def write_processed_events(tmp_path):
    events_path = tmp_path / "data" / "processed" / "events"
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
            "event_date": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "ctr": [0.1, 0.1, 0.2],
            "cpc": [2.0, 1.0, 3.0],
        }
    )
    df.to_parquet(events_path, index=False, partition_cols=["event_date"])


def test_analytics_summary_endpoint_works_with_processed_data(tmp_path, monkeypatch):
    write_processed_events(tmp_path)
    monkeypatch.chdir(tmp_path)
    client = TestClient(api.app)

    response = client.get("/analytics/summary")

    assert response.status_code == 200
    assert response.json() == {
        "total_entities": 2,
        "total_sources": 2,
        "total_impressions": 170,
        "total_clicks": 19,
        "total_cost": 37.0,
        "overall_ctr": 19 / 170,
        "overall_cpc": 37.0 / 19,
        "top_source_by_cost": "search",
    }


def test_analytics_entities_endpoint_works_with_processed_data(tmp_path, monkeypatch):
    write_processed_events(tmp_path)
    monkeypatch.chdir(tmp_path)
    client = TestClient(api.app)

    response = client.get("/analytics/entities?source=search")

    assert response.status_code == 200
    assert response.json() == [
        {
            "entity_id": "entity_1",
            "source": "search",
            "total_impressions": 150,
            "total_clicks": 15,
            "total_cost": 25.0,
            "ctr": 0.1,
            "cpc": 25.0 / 15,
        }
    ]


def test_analytics_summary_returns_clear_error_when_data_is_missing(monkeypatch):
    def missing_summary():
        raise duckdb.IOException('IO Error: No files found that match the pattern "data/processed/events/**/*.parquet"')

    monkeypatch.setattr(api, "summary_metrics", missing_summary)
    client = TestClient(api.app)

    response = client.get("/analytics/summary")

    assert response.status_code == 503
    assert "Run `python -m event_analytics_pipeline.generate_events`" in response.json()["detail"]


def test_analytics_entities_returns_clear_error_when_data_is_missing(monkeypatch):
    def missing_entities(**kwargs):
        raise duckdb.IOException('IO Error: No files found that match the pattern "data/processed/events/**/*.parquet"')

    monkeypatch.setattr(api, "entity_metrics", missing_entities)
    client = TestClient(api.app)

    response = client.get("/analytics/entities")

    assert response.status_code == 503
    assert "Run `python -m event_analytics_pipeline.generate_events`" in response.json()["detail"]


def test_analytics_entities_response_model_is_documented():
    schema = api.app.openapi()

    response_schema = schema["paths"]["/analytics/entities"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert response_schema["items"]["$ref"] == "#/components/schemas/EntityAnalyticsResponse"


def test_analytics_summary_response_model_is_documented():
    schema = api.app.openapi()

    response_schema = schema["paths"]["/analytics/summary"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert response_schema["$ref"] == "#/components/schemas/SummaryAnalyticsResponse"
