"""Manual driver for intake, classification, splitting, and routing."""

from __future__ import annotations

from pathlib import Path
import json
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from apps.orchestra.classifier import classify_work_item
from apps.orchestra.intake import intake_raw_message
from apps.orchestra.router import route_work_item
from apps.orchestra.splitter import split_work_item


def main() -> None:
    """Run a small mixed-message route example and print child payloads."""

    work_item = intake_raw_message(
        {
            "text": (
                "Sales:\n"
                "Revenue 100\n"
                "Cash up complete\n"
                "Supervisor Control:\n"
                "Checklist approved\n"
                "Inspection complete"
            )
        },
        received_at_utc="2026-04-06T00:00:00+00:00",
    )
    classify_work_item(work_item)
    split_result = split_work_item(work_item)

    routed_children = []
    for child in split_result.child_work_items:
        route_work_item(child)
        routed_children.append(
            {
                "kind": child.kind,
                "payload": child.payload,
            }
        )

    print(json.dumps(routed_children, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
