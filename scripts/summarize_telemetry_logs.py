"""CLI for daily telemetry intelligence summaries."""

from __future__ import annotations

import argparse
import json

from packages.telemetry_intelligence import resolve_report_dates, summarize_telemetry_for_dates


def main(argv: list[str] | None = None) -> int:
    """Summarize phase-1 runtime logs into compact daily telemetry artifacts."""

    parser = argparse.ArgumentParser(description="Summarize runtime logs into telemetry intelligence artifacts.")
    parser.add_argument("--date", dest="report_date", help="One report date in YYYY-MM-DD format.")
    parser.add_argument("--since", help="Inclusive trailing day window, for example 7d.")
    args = parser.parse_args(argv)

    report_dates = resolve_report_dates(report_date=args.report_date, since=args.since)
    results = summarize_telemetry_for_dates(report_dates)
    print(
        json.dumps(
            {
                "report_dates": report_dates,
                "outputs": [
                    {
                        "report_date": result["report_date"],
                        "events_path": result["events_path"],
                        "summary_path": result["summary_path"],
                        "event_count": result["summary_payload"]["event_count"],
                        "occurrence_count": result["summary_payload"]["occurrence_count"],
                    }
                    for result in results
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
