import pandas as pd
import pytest

from event_analytics_pipeline.validation import validate_events


def valid_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_id": ["evt_1"],
            "entity_id": ["entity_1"],
            "source": ["search"],
            "event_type": ["click"],
            "country": ["US"],
            "impressions": [10],
            "clicks": [2],
            "cost": [1.5],
            "event_timestamp": ["2025-01-01T00:00:00"],
        }
    )


def test_missing_columns_fail_validation():
    df = valid_df().drop(columns=["cost"])

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_events(df)


@pytest.mark.parametrize("column", ["impressions", "clicks", "cost"])
def test_negative_metric_values_fail_validation(column):
    df = valid_df()
    df.loc[0, column] = -1

    with pytest.raises(ValueError, match=f"{column} must be >= 0"):
        validate_events(df)


def test_clicks_greater_than_impressions_fails_validation():
    df = valid_df()
    df.loc[0, "clicks"] = 11

    with pytest.raises(ValueError, match="clicks must be <= impressions"):
        validate_events(df)


@pytest.mark.parametrize("event_id", [None, ""])
def test_missing_event_id_fails_validation(event_id):
    df = valid_df()
    df.loc[0, "event_id"] = event_id

    with pytest.raises(ValueError, match="event_id must be present"):
        validate_events(df)


def test_invalid_timestamp_fails_validation():
    df = valid_df()
    df.loc[0, "event_timestamp"] = "not-a-timestamp"

    with pytest.raises(ValueError, match="event_timestamp must be parseable"):
        validate_events(df)
