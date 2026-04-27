"""Focused tests for report-family classification."""

from __future__ import annotations

from apps.header_normalizer_agent.worker import normalize_headers
from apps.report_family_classifier_agent.worker import classify_report_family
from packages.report_registry import route_for_family


def test_classifier_accepts_staffs_attendance_alias() -> None:
    text = "\n".join(
        [
            "SUNDAY:26/04/26",
            "STAFFS ATTENDANCE",
            "1. Alice Koko = P",
            "2. Grace Masson = Off",
            "3. Fidelma Wobilo = Leave",
        ]
    )

    classification = classify_report_family(text, normalize_headers(text))
    route = route_for_family(classification.report_family)

    assert classification.report_family == "attendance"
    assert route.target_agent == "hr_agent"
    assert route.specialist_type == "staff_attendance"
    assert route.storage_bucket == "hr_attendance"


def test_classifier_detects_attendance_from_staff_rows_without_exact_title() -> None:
    text = "\n".join(
        [
            "Waigani Branch",
            "MONDAY 01/01/26",
            "1. Alice Koko = P",
            "2. Grace Masson = Off",
            "3. Fidelma Wobilo = Leave",
            "4. David Yaro = A",
        ]
    )

    classification = classify_report_family(text, normalize_headers(text))

    assert classification.report_family == "attendance"
    assert classification.confidence >= 0.45


def test_classifier_labels_supervisor_control_as_intelligence() -> None:
    text = "\n".join(
        [
            "Supervisor Control Report",
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "Cash variance: No",
            "Staffing issues: Yes",
        ]
    )

    classification = classify_report_family(text, normalize_headers(text))
    route = route_for_family(classification.report_family)

    assert classification.report_family == "intelligence"
    assert classification.report_type == "supervisor_control"
    assert route.target_agent == "supervisor_control_agent"
    assert route.specialist_type == "supervisor_control"
