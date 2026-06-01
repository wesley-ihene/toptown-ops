"""Admin CLI for adaptive SOP learning registry review actions."""

from __future__ import annotations

import argparse
import json

from apps.adaptive_sop_engine.registry import list_entries, update_mapping_status


def main(argv: list[str] | None = None) -> int:
    """List, approve, or reject adaptive SOP mappings."""

    parser = argparse.ArgumentParser(description="Manage adaptive SOP learning mappings.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List current adaptive SOP mappings.")
    list_parser.add_argument("--root", default=None, help="Optional output root containing records/.")

    approve_parser = subparsers.add_parser("approve", help="Approve one adaptive SOP mapping.")
    approve_parser.add_argument("--root", default=None, help="Optional output root containing records/.")
    approve_parser.add_argument("--report-type", required=True, help="Adaptive report type, for example sales_income.")
    approve_parser.add_argument("--source", required=True, help="Observed source label to approve.")
    approve_parser.add_argument("--target", required=True, help="Approved canonical target field.")
    approve_parser.add_argument("--confidence", type=float, default=0.94, help="Approved mapping confidence.")

    reject_parser = subparsers.add_parser("reject", help="Reject one adaptive SOP mapping.")
    reject_parser.add_argument("--root", default=None, help="Optional output root containing records/.")
    reject_parser.add_argument("--report-type", required=True, help="Adaptive report type, for example sales_income.")
    reject_parser.add_argument("--source", required=True, help="Observed source label to reject.")

    args = parser.parse_args(argv)

    if args.command == "list":
        print(json.dumps(list_entries(output_root=args.root), indent=2, sort_keys=True))
        return 0

    if args.command == "approve":
        updated = update_mapping_status(
            args.report_type,
            args.source,
            status="approved",
            target=args.target,
            confidence=args.confidence,
            output_root=args.root,
        )
        print(json.dumps(updated, indent=2, sort_keys=True))
        return 0

    if args.command == "reject":
        updated = update_mapping_status(
            args.report_type,
            args.source,
            status="rejected",
            output_root=args.root,
        )
        print(json.dumps(updated, indent=2, sort_keys=True))
        return 0

    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
