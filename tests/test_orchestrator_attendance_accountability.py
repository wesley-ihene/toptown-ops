"""Focused orchestrator accountability regressions for HR attendance."""

from __future__ import annotations

import json
from pathlib import Path

import apps.orchestrator_agent.worker as orchestrator_worker
from apps.orchestrator_agent.worker import process_work_item
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from packages.signal_contracts.work_item import WorkItem


def test_orchestrator_auto_accepts_attendance_after_normalization(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        _attendance_work_item(
            "\n".join(
                [
                    "ATTENDANCE REPORT",
                    "Branch: Waigani Branch",
                    "Date: 01/05/2026",
                    "1. Alice Koko = ✓",
                    "2. Grace Masson = check",
                    "3. Fidelma Wobilo = on leave",
                    "4. David Yaro = no show",
                    "Press /= 2",
                    "Leave /= 1",
                    "Absent /= 1",
                    "Total Staff /= 4",
                ]
            )
        )
    )

    assert result.agent_name == "hr_agent"
    assert result.payload["status"] == "accepted"
    assert result.payload.get("accountability") is None
    assert result.payload.get("validation_error_code") is None
    assert result.payload.get("validation_error_message") is None
    assert {item["staff_name"]: item["status"] for item in result.payload["items"]} == {
        "Alice Koko": "present",
        "Grace Masson": "present",
        "Fidelma Wobilo": "leave",
        "David Yaro": "absent",
    }
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "unknown_attendance_status" not in warning_codes
    assert "attendance_totals_mismatch" not in warning_codes
    assert "data_mismatch" not in warning_codes

    structured_path = tmp_path / "records" / "structured" / "hr_attendance" / "waigani" / "2026-05-01.json"
    assert structured_path.exists()
    assert not _paths(tmp_path / "records" / "review", "**/*.json")


def test_orchestrator_accepts_high_confidence_attendance_when_candidate_review_has_no_blocking_issue(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    original_dispatch = orchestrator_worker._dispatch_to_specialist

    def _dispatch_with_stale_review_status(work_item, *, target_agent):
        result = original_dispatch(work_item, target_agent=target_agent)
        result.payload["status"] = "needs_review"
        result.payload.pop("accountability", None)
        result.payload.pop("validation_error_code", None)
        result.payload.pop("validation_error_message", None)
        if isinstance(result.metadata, dict):
            result.metadata.pop("accountability", None)
            validation = result.metadata.get("validation")
            if isinstance(validation, dict):
                validation = dict(validation)
                details = validation.get("details")
                if isinstance(details, dict):
                    details = dict(details)
                    details["final_status"] = "needs_review"
                    details.pop("accountability", None)
                    details.pop("validation_error_code", None)
                    details.pop("validation_error_message", None)
                    validation["details"] = details
                result.metadata["validation"] = validation
        return result

    monkeypatch.setattr(orchestrator_worker, "_dispatch_to_specialist", _dispatch_with_stale_review_status)

    result = process_work_item(_attendance_work_item(_sample_text("2026-05-01__unknown__be920772cf5e.txt")))

    assert result.payload["status"] == "accepted"
    assert result.payload["confidence"] >= 0.9
    assert result.payload.get("accountability") is None
    assert result.payload.get("validation_error_code") is None
    assert result.payload.get("validation_error_message") is None

    structured_path = tmp_path / "records" / "structured" / "hr_attendance" / "lae_5th_street" / "2026-05-01.json"
    assert structured_path.exists()
    assert not _paths(tmp_path / "records" / "review", "**/*.json")


def test_orchestrator_review_payload_includes_exact_attendance_accountability(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        _attendance_work_item(
            "\n".join(
                [
                    "ATTENDANCE REPORT",
                    "Branch: Waigani Branch",
                    "Date: 01/05/2026",
                    "1. Alice Koko = ✓",
                    "2. Grace Masson = check",
                    "3. Fidelma Wobilo = on leave",
                    "4. David Yaro = no show",
                    "Pres /= 2",
                    "Leave /= 1",
                    "Absent /= 1",
                    "Total Staff /= 5",
                ]
            )
        )
    )

    assert result.payload["status"] == "needs_review"
    assert result.payload["validation_error_code"] == "declared_summary_total_mismatch"
    assert result.payload["validation_error_message"] == (
        "Declared attendance summary PRESENT + LEAVE + ABSENT = 4 but TOTAL_STAFF = 5."
    )
    accountability = result.payload["accountability"]
    assert accountability["failing_layer"] == "hr_validation"
    assert accountability["failing_rule"] == "declared_summary_total_mismatch"
    assert accountability["validation_error_code"] == "declared_summary_total_mismatch"
    assert accountability["validation_error_message"] == result.payload["validation_error_message"]
    assert accountability["calculated_totals"]["present"] == 2
    assert accountability["calculated_totals"]["leave"] == 1
    assert accountability["calculated_totals"]["absent"] == 1
    assert accountability["calculated_totals"]["total_staff"] == 4
    assert accountability["declared_totals"]["present"] == 2
    assert accountability["declared_totals"]["leave"] == 1
    assert accountability["declared_totals"]["absent"] == 1
    assert accountability["declared_totals"]["declared_reconciled_total"] == 4
    assert accountability["declared_totals"]["total_staff"] == 5
    assert accountability["final_confidence_score"] == result.payload["confidence"]
    assert accountability["reason_detail"] == result.payload["validation_error_message"]
    assert "PRESENT + LEAVE + ABSENT = TOTAL_STAFF 5" in accountability["recommended_correction"]
    assert any(
        attempt["field"] == "summary_label" and attempt["normalized_value"] == "Total_Present"
        for attempt in accountability["normalized_values_attempted"]
    )
    assert any(
        attempt["field"] == "attendance_status"
        and attempt["raw_value"] == "check"
        and attempt["normalized_value"] == "PRESENT"
        for attempt in accountability["normalized_values_attempted"]
    )
    assert any(
        attempt["field"] == "attendance_status"
        and attempt["raw_value"] == "no show"
        and attempt["normalized_value"] == "ABSENT"
        for attempt in accountability["normalized_values_attempted"]
    )

    review_paths = _paths(tmp_path / "records" / "review" / "2026_05_01" / "waigani" / "staff_attendance", "*.json")
    assert len(review_paths) == 1
    review_payload = _read_json(review_paths[0])
    assert review_payload["candidate_payload"]["validation_error_code"] == "declared_summary_total_mismatch"
    assert review_payload["candidate_payload"]["validation_error_message"] == result.payload["validation_error_message"]
    assert review_payload["candidate_payload"]["accountability"]["failing_rule"] == "declared_summary_total_mismatch"
    validation_accountability = review_payload["validation"]["details"]["accountability"]
    assert validation_accountability["failing_layer"] == "hr_validation"
    assert review_payload["validation"]["details"]["validation_error_code"] == "declared_summary_total_mismatch"
    assert review_payload["validation"]["details"]["validation_error_message"] == result.payload["validation_error_message"]
    assert validation_accountability["declared_totals"]["total_staff"] == 5
    assert validation_accountability["calculated_totals"]["total_staff"] == 4


def test_orchestrator_rejected_attendance_keeps_missing_branch_accountability(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(orchestrator_worker, "_should_attempt_specialist_fallback", lambda **kwargs: False)

    result = process_work_item(
        _attendance_work_item(
            "\n".join(
                [
                    "ATTENDANCE REPORT",
                    "Date: 01/05/2026",
                    "1. Alice Koko = ✓",
                    "2. Grace Masson = check",
                    "3. Fidelma Wobilo = on leave",
                    "4. David Yaro = no show",
                    "Present /= 2",
                    "Leave /= 1",
                    "Absent /= 1",
                    "Total Staff /= 4",
                ]
            ),
            metadata={"received_at": "2026-05-01T09:00:00Z", "sender": "attendance-missing-branch"},
        )
    )

    assert result.payload["status"] == "rejected"
    assert result.payload["validation_error_code"] == "branch_unresolved"
    assert "branch" in result.payload["validation_error_message"].lower()
    accountability = result.payload["accountability"]
    assert accountability["failing_layer"] == "orchestrator_routing"
    assert accountability["failing_rule"] == "branch_unresolved"
    assert accountability["validation_error_code"] == "branch_unresolved"
    assert accountability["validation_error_message"] == result.payload["validation_error_message"]
    assert accountability["declared_totals"] == {}
    assert accountability["calculated_totals"] == {}
    assert accountability["final_confidence_score"] == 0.0
    assert "branch" in accountability["recommended_correction"].lower()

    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp", "**/*.meta.json")
    assert len(rejected_meta_paths) == 1
    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["accountability"]["failing_rule"] == "branch_unresolved"


def _attendance_work_item(text: str, *, metadata: dict[str, object] | None = None) -> WorkItem:
    return WorkItem(
        kind="raw_message",
        payload={
            "source": "whatsapp",
            "raw_message": {"text": text},
            "metadata": {
                "received_at": "2026-05-01T09:00:00Z",
                "sender": "attendance-accountability",
                **(metadata or {}),
            },
        },
    )


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
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


def _paths(directory: Path, pattern: str) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob(pattern))


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sample_text(filename: str) -> str:
    return (
        Path(__file__).resolve().parents[1]
        / "records"
        / "raw"
        / "whatsapp"
        / "unknown"
        / filename
    ).read_text(encoding="utf-8")
