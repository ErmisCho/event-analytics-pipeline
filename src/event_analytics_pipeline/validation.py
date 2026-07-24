from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = {
    "event_id",
    "entity_id",
    "source",
    "event_type",
    "country",
    "impressions",
    "clicks",
    "cost",
    "event_timestamp",
}


def validate_events(df: pd.DataFrame) -> None:
    """Validate raw event data.

    Raises:
        ValueError: if required columns are missing or data quality checks fail.
    """
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    if df["event_id"].isna().any() or (df["event_id"].astype(str).str.strip() == "").any():
        raise ValueError("event_id must be present")

    for column in ["impressions", "clicks", "cost"]:
        values = pd.to_numeric(df[column], errors="coerce")
        if values.isna().any():
            raise ValueError(f"{column} must be numeric")
        if (values < 0).any():
            raise ValueError(f"{column} must be >= 0")

    impressions = pd.to_numeric(df["impressions"], errors="coerce")
    clicks = pd.to_numeric(df["clicks"], errors="coerce")
    if (clicks > impressions).any():
        raise ValueError("clicks must be <= impressions")

    timestamps = pd.to_datetime(df["event_timestamp"], errors="coerce", utc=True, format="mixed")
    if timestamps.isna().any():
        raise ValueError("event_timestamp must be parseable")
