"""Build cashier and supervisor adjustment risk intelligence outputs."""

from __future__ import annotations

import argparse

from apps.cashier_supervisor_risk_agent.worker import build_risk_outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build cashier/supervisor risk intelligence from structured sales records.")
    parser.add_argument("--branch", help="Canonical branch slug or recognized branch label.")
    parser.add_argument("--start", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end", help="End date in YYYY-MM-DD format.")
    parser.add_argument("--date", help="Single date in YYYY-MM-DD format.")
    parser.add_argument("--root", help="Repository root override.")
    args = parser.parse_args(argv)

    if args.date:
        start_date = end_date = args.date
    elif args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        parser.error("Provide --date or both --start and --end.")

    result = build_risk_outputs(
        start_date=start_date,
        end_date=end_date,
        branch=args.branch,
        output_root=args.root,
    )
    print(result["output_paths"]["aggregate"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
