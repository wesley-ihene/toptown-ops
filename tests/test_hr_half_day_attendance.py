"""Regression coverage for half-day attendance normalization."""

from __future__ import annotations

from pathlib import Path

from apps.hr_agent.parser import parse_work_item
from apps.hr_agent.worker import process_work_item
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from packages.signal_contracts.work_item import WorkItem
from packages.sop_validation.attendance import validate_attendance


def test_half_day_variants_normalize_to_p_half() -> None:
    variants = (
        "P (1/2 day)",
        "P(1/2 day)",
        "P half day",
        "Present half day",
    )

    for raw_status in variants:
        parsed = parse_work_item(
            _attendance_work_item(
                [
                    "Branch: Waigani Branch",
                    "Date: 07/04/2026",
                    f"John Doe - {raw_status}",
                ]
            )
        )

        assert len(parsed.records) == 1
        assert parsed.records[0].status == "present_half"
        assert parsed.records[0].normalized_status == "P_HALF"


def test_half_day_metrics_add_separate_full_half_and_effective_present(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime_paths(monkeypatch, tmp_path)

    result = process_work_item(
        _attendance_work_item(
            [
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "Mary Kila - P (1/2 day)",
                "Peter Ake - Off",
                "Total Staff: 3",
            ]
        )
    )

    assert result.payload["status"] == "accepted"
    assert result.payload["metrics"]["present_count"] == 1
    assert result.payload["metrics"]["present_half_count"] == 1
    assert result.payload["metrics"]["present"] == 1
    assert result.payload["metrics"]["present_full"] == 1
    assert result.payload["metrics"]["present_half"] == 1
    assert result.payload["metrics"]["effective_present"] == 1.5
    assert result.payload["metrics"]["off_count"] == 1
    assert {item["staff_name"]: item["status"] for item in result.payload["items"]} == {
        "John Doe": "present",
        "Mary Kila": "present_half",
        "Peter Ake": "off",
    }


def test_effective_present_counts_full_plus_half_weight(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime_paths(monkeypatch, tmp_path)

    result = process_work_item(
        _attendance_work_item(
            [
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - P",
                "Mary Kila - P half day",
                "Peter Ake - Present half day",
                "Total Staff: 3",
            ]
        )
    )

    assert result.payload["status"] == "accepted"
    assert result.payload["metrics"]["present_full"] == 1
    assert result.payload["metrics"]["present_half"] == 2
    assert result.payload["metrics"]["effective_present"] == 2.0


def test_existing_present_status_behavior_is_unchanged(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime_paths(monkeypatch, tmp_path)

    result = process_work_item(
        _attendance_work_item(
            [
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - P",
                "Total Staff: 1",
            ]
        )
    )

    assert result.payload["status"] == "accepted"
    assert result.payload["items"] == [{"staff_name": "John Doe", "status": "present"}]
    assert result.payload["metrics"]["present_count"] == 1
    assert result.payload["metrics"]["present_half_count"] == 0
    assert result.payload["metrics"]["present_full"] == 1
    assert result.payload["metrics"]["present_half"] == 0
    assert result.payload["metrics"]["effective_present"] == 1.0


def test_governance_validation_accepts_present_half_as_supported_status() -> None:
    result = validate_attendance(
        {
            "branch": "waigani",
            "report_date": "2026-04-07",
            "metrics": {
                "total_staff_listed": 2,
                "present_count": 1,
                "absent_count": 0,
                "off_count": 0,
                "leave_count": 0,
                "present_half_count": 1,
                "present_full": 1,
                "present_half": 1,
                "effective_present": 1.5,
            },
            "items": [
                {"staff_name": "John", "status": "present"},
                {"staff_name": "Mary", "status": "present_half"},
            ],
        }
    )

    assert result.accepted is True
    assert result.rejection_codes == []


def _attendance_work_item(lines: list[str]) -> WorkItem:
    return WorkItem(
        kind="raw_message",
        payload={
            "classification": {"report_type": "staff_attendance"},
            "raw_message": {"text": "\n".join(lines)},
        },
    )


def _patch_runtime_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "DUPLICATES_DIR", records_dir / "duplicates" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setattr("apps.hr_agent.worker.OUTBOX_PATH", tmp_path / "outbox")
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))
