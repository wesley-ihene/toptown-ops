"""Export read-only TAOP operations summaries for one date."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from packages.taop_ops.exports import (
    default_export_directory,
    display_export_path,
    export_attendance_csv,
    export_bale_release_csv,
    export_daily_operations_json,
    export_sales_csv,
)


def main(argv: list[str] | None = None) -> int:
    """Run the TAOP ops export CLI."""

    parser = argparse.ArgumentParser(description="Export read-only TAOP operations summaries.")
    parser.add_argument("--date", required=True, help="ISO report date, for example 2026-05-01.")
    parser.add_argument("--branch", help="Optional branch filter.")
    parser.add_argument("--root", help="Optional repo root override for tests or alternate workspaces.")
    args = parser.parse_args(argv)

    output_dir = default_export_directory(args.date, branch=args.branch, root=args.root)
    sales_path = export_sales_csv(args.date, output_dir, branch=args.branch, root=args.root)
    bale_release_path = export_bale_release_csv(args.date, output_dir, branch=args.branch, root=args.root)
    attendance_path = export_attendance_csv(args.date, output_dir, branch=args.branch, root=args.root)
    daily_path = export_daily_operations_json(args.date, output_dir, branch=args.branch, root=args.root)

    payload = {
        "date": args.date,
        "branch": args.branch,
        "output_dir": display_export_path(output_dir, root=args.root),
        "written_files": [
            display_export_path(sales_path, root=args.root),
            display_export_path(bale_release_path, root=args.root),
            display_export_path(attendance_path, root=args.root),
            display_export_path(daily_path, root=args.root),
        ],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
