from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

SOURCES = ["programmatic", "social", "search", "direct"]
EVENT_TYPES = ["view", "click", "conversion"]
COUNTRIES = ["AT", "DE", "CH", "CZ", "IT", "SI"]


def generate_events(output_path: str | Path = "data/raw/events.csv", rows: int = 1_000, seed: int = 42) -> pd.DataFrame:
    """Generate fake raw event data and write it to CSV."""
    rng = np.random.default_rng(seed)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    impressions = rng.integers(0, 1_000, size=rows)
    clicks = np.array([rng.integers(0, value + 1) for value in impressions])
    cost = np.round(rng.uniform(0, 25, size=rows) * np.maximum(clicks, 1), 2)
    start = pd.Timestamp("2025-01-01", tz="UTC")
    timestamps = start + pd.to_timedelta(rng.integers(0, 60 * 60 * 24 * 30, size=rows), unit="s")

    df = pd.DataFrame(
        {
            "event_id": [f"evt_{i:06d}" for i in range(rows)],
            "entity_id": [f"entity_{i:03d}" for i in rng.integers(1, 51, size=rows)],
            "source": rng.choice(SOURCES, size=rows),
            "event_type": rng.choice(EVENT_TYPES, size=rows, p=[0.7, 0.25, 0.05]),
            "country": rng.choice(COUNTRIES, size=rows),
            "impressions": impressions,
            "clicks": clicks,
            "cost": cost,
            "event_timestamp": timestamps.astype(str),
        }
    )
    df.to_csv(output_path, index=False)
    return df


if __name__ == "__main__":
    generate_events()
    print("Wrote data/raw/events.csv")
