"""Build one branch comparison analytics file."""

from __future__ import annotations

import argparse
import json

from apps.branch_comparison_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build one branch comparison analytics file.")
    parser.add_argument("--date", required=True, help="ISO report date in YYYY-MM-DD format.")
    parser.add_argument("--branch", action="append", default=[], help="Optional branch filter; may be repeated.")
    parser.add_argument("--root", help="Repository root override.")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting the output file.")
    parser.add_argument("--print-json", action="store_true", help="Print the analytics payload to stdout.")
    args = parser.parse_args(argv)

    result = process_work_item(
        WorkItem(
            kind="analytics_request",
            payload={
                "report_date": args.date,
                "branches": args.branch,
                "root": args.root,
                "overwrite": args.overwrite,
            },
        )
    )
    if result.payload["status"] == "invalid_input":
        parser.error(result.payload["warnings"][0]["message"])
    if args.print_json:
        print(json.dumps(result.payload["analytics_payload"], indent=2, sort_keys=True))
    else:
        print(result.payload["output_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
