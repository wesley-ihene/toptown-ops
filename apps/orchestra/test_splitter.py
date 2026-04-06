"""Manual driver for intake, classification, and splitting."""

from __future__ import annotations

from pathlib import Path
import json
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from apps.orchestra.classifier import classify_work_item
from apps.orchestra.intake import intake_raw_message
from apps.orchestra.splitter import split_work_item


def main() -> None:
    """Run a small mixed-message split example and print child payloads."""

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

    print(
        json.dumps(
            [
                {
                    "kind": child.kind,
                    "payload": child.payload,
                }
                for child in split_result.child_work_items
            ],
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
