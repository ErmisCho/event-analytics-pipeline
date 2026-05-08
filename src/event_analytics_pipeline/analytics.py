from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def _parquet_scan_path(parquet_path: str | Path) -> str:
    """Return a file or recursive glob path for DuckDB Parquet scans."""
    path = Path(parquet_path)
    if path.suffix == ".parquet":
        return str(path)
    return str(path / "**" / "*.parquet")


def entity_metrics(parquet_path: str | Path = "data/processed/events") -> pd.DataFrame:
    """Return entity/source metrics from processed Parquet, sorted by spend."""
    parquet_path = _parquet_scan_path(parquet_path)
    query = """
        SELECT
            entity_id,
            source,
            SUM(impressions)::BIGINT AS total_impressions,
            SUM(clicks)::BIGINT AS total_clicks,
            ROUND(SUM(cost), 2) AS total_cost,
            CASE WHEN SUM(impressions) = 0 THEN 0 ELSE SUM(clicks)::DOUBLE / SUM(impressions) END AS ctr,
            CASE WHEN SUM(clicks) = 0 THEN 0 ELSE SUM(cost)::DOUBLE / SUM(clicks) END AS cpc
        FROM read_parquet(?)
        GROUP BY entity_id, source
        ORDER BY total_cost DESC
    """
    with duckdb.connect(database=":memory:") as conn:
        return conn.execute(query, [parquet_path]).fetchdf()


def summary_metrics(parquet_path: str | Path = "data/processed/events") -> dict[str, int | float | str | None]:
    """Return compact summary metrics from processed Parquet."""
    parquet_path = _parquet_scan_path(parquet_path)
    query = """
        WITH events AS (
            SELECT * FROM read_parquet(?)
        ),
        totals AS (
            SELECT
                COUNT(DISTINCT entity_id)::BIGINT AS total_entities,
                COUNT(DISTINCT source)::BIGINT AS total_sources,
                COALESCE(SUM(impressions), 0)::BIGINT AS total_impressions,
                COALESCE(SUM(clicks), 0)::BIGINT AS total_clicks,
                ROUND(COALESCE(SUM(cost), 0), 2) AS total_cost
            FROM events
        ),
        source_costs AS (
            SELECT source, SUM(cost) AS source_cost
            FROM events
            GROUP BY source
            ORDER BY source_cost DESC, source
            LIMIT 1
        )
        SELECT
            total_entities,
            total_sources,
            total_impressions,
            total_clicks,
            total_cost,
            CASE WHEN total_impressions = 0 THEN 0 ELSE total_clicks::DOUBLE / total_impressions END AS overall_ctr,
            CASE WHEN total_clicks = 0 THEN 0 ELSE total_cost::DOUBLE / total_clicks END AS overall_cpc,
            (SELECT source FROM source_costs) AS top_source_by_cost
        FROM totals
    """
    with duckdb.connect(database=":memory:") as conn:
        row = conn.execute(query, [parquet_path]).fetchdf().iloc[0].to_dict()

    return {
        "total_entities": int(row["total_entities"]),
        "total_sources": int(row["total_sources"]),
        "total_impressions": int(row["total_impressions"]),
        "total_clicks": int(row["total_clicks"]),
        "total_cost": float(row["total_cost"]),
        "overall_ctr": float(row["overall_ctr"]),
        "overall_cpc": float(row["overall_cpc"]),
        "top_source_by_cost": row["top_source_by_cost"],
    }
