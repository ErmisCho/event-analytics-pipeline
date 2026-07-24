from __future__ import annotations

from typing import Annotated

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from event_analytics_pipeline.analytics import entity_metrics, summary_metrics

app = FastAPI(title="Campaign Event Analytics Pipeline")


class EntityAnalyticsResponse(BaseModel):
    entity_id: str
    source: str
    total_impressions: int
    total_clicks: int
    total_cost: float
    ctr: float
    cpc: float


class SummaryAnalyticsResponse(BaseModel):
    total_entities: int
    total_sources: int
    total_impressions: int
    total_clicks: int
    total_cost: float
    overall_ctr: float
    overall_cpc: float
    top_source_by_cost: str | None


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _data_not_ready_error() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail="Processed analytics data was not found. Run `python -m event_analytics_pipeline.generate_events` and `python -m event_analytics_pipeline.transform` first.",
    )


@app.get("/analytics/summary", response_model=SummaryAnalyticsResponse)
def analytics_summary() -> dict[str, int | float | str | None]:
    try:
        return summary_metrics()
    except duckdb.IOException as exc:
        if "No files found" in str(exc):
            raise _data_not_ready_error() from exc
        raise


@app.get("/analytics/entities", response_model=list[EntityAnalyticsResponse])
def analytics_entities(
    source: str | None = None,
    entity_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> list[dict[str, object]]:
    try:
        df = entity_metrics(source=source, entity_id=entity_id, limit=limit)
    except duckdb.IOException as exc:
        if "No files found" in str(exc):
            raise _data_not_ready_error() from exc
        raise

    records = df.to_dict(orient="records")
    return jsonable_encoder(records)
