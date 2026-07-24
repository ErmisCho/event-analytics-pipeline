import json

from event_analytics_pipeline.analytics import summary_metrics
from event_analytics_pipeline.generate_events import generate_events
from event_analytics_pipeline.transform import transform_events


def main() -> None:
    generate_events()
    transform_events()
    print(json.dumps(summary_metrics(), indent=2))


if __name__ == "__main__":
    main()
