from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from event_analytics_pipeline.validation import REQUIRED_COLUMNS, validate_events


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return (numerator / denominator.where(denominator != 0)).fillna(0.0)


def _rejection_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    if pd.isna(row["event_id"]) or str(row["event_id"]).strip() == "":
        reasons.append("missing event_id")

    impressions = pd.to_numeric(pd.Series([row["impressions"]]), errors="coerce").iloc[0]
    clicks = pd.to_numeric(pd.Series([row["clicks"]]), errors="coerce").iloc[0]
    cost = pd.to_numeric(pd.Series([row["cost"]]), errors="coerce").iloc[0]

    if pd.isna(impressions) or impressions < 0:
        reasons.append("invalid impressions")
    if pd.isna(clicks) or clicks < 0:
        reasons.append("invalid clicks")
    if pd.isna(cost) or cost < 0:
        reasons.append("invalid cost")
    if not pd.isna(impressions) and not pd.isna(clicks) and clicks > impressions:
        reasons.append("clicks greater than impressions")
    if pd.isna(pd.to_datetime(row["event_timestamp"], errors="coerce", utc=True, format="mixed")):
        reasons.append("invalid event_timestamp")

    return "; ".join(reasons)


def _replace_parquet_dataset(df: pd.DataFrame, output_path: Path) -> None:
    """Replace the dataset only after its successor was written successfully."""
    with tempfile.TemporaryDirectory(prefix=f".{output_path.name}-", dir=output_path.parent) as temp_dir:
        temporary = Path(temp_dir) / "new"
        previous = Path(temp_dir) / "previous"
        df.to_parquet(temporary, index=False, partition_cols=["event_date"])
        if output_path.exists():
            output_path.replace(previous)
        try:
            temporary.replace(output_path)
        except Exception:
            if previous.exists():
                previous.replace(output_path)
            raise


def transform_events(
    input_path: str | Path = "data/raw/events.csv",
    output_path: str | Path = "data/processed/events",
    report_path: str | Path = "data/processed/data-quality-report.json",
    rejected_path: str | Path = "data/processed/rejected-events.csv",
) -> pd.DataFrame:
    """Read raw CSV, reject bad rows, deduplicate, enrich metrics, and write outputs."""
    input_path = Path(input_path)
    output_path = Path(output_path)
    report_path = Path(report_path)
    rejected_path = Path(rejected_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path, dtype={"event_id": "string"})
    records_read = len(df)

    # Keep schema problems as hard failures; row-level data issues are rejected below.
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        validate_events(df)

    df = df.copy()
    df["rejection_reason"] = df.apply(_rejection_reason, axis=1)
    rejected = df[df["rejection_reason"] != ""].copy()
    valid = df[df["rejection_reason"] == ""].drop(columns=["rejection_reason"]).copy()
    rejected.to_csv(rejected_path, index=False)

    records_valid = len(valid)
    records_invalid = len(rejected)
    validate_events(valid)

    # Event IDs are immutable; add event versions if in-place corrections are required.
    valid = valid.drop_duplicates(subset=["event_id"], keep="first").copy()
    records_after_deduplication = len(valid)
    valid["event_timestamp"] = pd.to_datetime(valid["event_timestamp"], utc=True, format="mixed")
    valid["event_date"] = valid["event_timestamp"].dt.strftime("%Y-%m-%d")
    valid["impressions"] = pd.to_numeric(valid["impressions"])
    valid["clicks"] = pd.to_numeric(valid["clicks"])
    valid["cost"] = pd.to_numeric(valid["cost"])
    valid["ctr"] = _safe_divide(valid["clicks"], valid["impressions"])
    valid["cpc"] = _safe_divide(valid["cost"], valid["clicks"])

    records_existing = 0
    records_inserted = len(valid)
    if output_path.exists():
        existing = pd.read_parquet(output_path)
        existing["event_id"] = existing["event_id"].astype("string")
        records_existing = len(existing)
        existing_ids = set(existing["event_id"])
        records_inserted = len(set(valid["event_id"]) - existing_ids)
        valid = pd.concat([existing, valid], ignore_index=True).drop_duplicates("event_id", keep="first")
    valid = valid.sort_values("event_id").reset_index(drop=True)
    if valid.empty:
        raise ValueError("No valid events to write")

    min_timestamp = valid["event_timestamp"].min() if not valid.empty else None
    max_timestamp = valid["event_timestamp"].max() if not valid.empty else None
    report = {
        "records_read": records_read,
        "records_valid": records_valid,
        "records_invalid": records_invalid,
        "records_after_deduplication": records_after_deduplication,
        "duplicates_removed": records_valid - records_after_deduplication,
        "records_existing": records_existing,
        "records_inserted": records_inserted,
        "records_written": len(valid),
        "total_impressions": int(valid["impressions"].sum()),
        "total_clicks": int(valid["clicks"].sum()),
        "total_cost": round(float(valid["cost"].sum()), 2),
        "min_event_timestamp": min_timestamp.isoformat() if min_timestamp is not None else None,
        "max_event_timestamp": max_timestamp.isoformat() if max_timestamp is not None else None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    _replace_parquet_dataset(valid, output_path)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return valid


if __name__ == "__main__":
    transform_events()
    print("Wrote partitioned Parquet to data/processed/events/")
