"""Focused tests for upstream date resolution."""

from __future__ import annotations

from apps.date_resolver_agent.worker import resolve_report_date
from apps.header_normalizer_agent.worker import normalize_headers


def test_resolve_report_date_supports_weekday_colon_attendance_header() -> None:
    header_result = normalize_headers(
        "\n".join(
            [
                "SUNDAY:26/04/26",
                "STAFFS ATTENDANCE",
            ]
        )
    )

    resolved = resolve_report_date(header_result)

    assert resolved.iso_date == "2026-04-26"
    assert resolved.raw_date == "SUNDAY:26/04/26"
    assert resolved.confidence == 1.0
