"""Regression coverage for HR attendance acceptance and review routing."""

from __future__ import annotations

from pathlib import Path

from apps.conversation_router import route_conversation_response
from apps.orchestrator_agent.worker import process_work_item
from apps.response_engine import render_whatsapp_response
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

_BROKEN_LINE_SAMPLE = "2026-05-01__unknown__ef9252425300.txt"
_JOINED_LINE_SAMPLE = "2026-05-01__unknown__d48d9c4cf0a9.txt"
_CORRECTED_SAMPLE = "2026-05-01__unknown__be920772cf5e.txt"


def test_lae_split_line_report_reviews_with_exact_summary_mismatch(tmp_path: Path, monkeypatch) -> None:
    _patch_runtime_paths(monkeypatch, tmp_path)

    result = process_work_item(_attendance_work_item(_sample_text(_BROKEN_LINE_SAMPLE), sender="lae-split-line"))

    assert result.payload["status"] == "needs_review"
    assert result.payload["confidence"] >= 0.9
    assert result.payload["validation_error_code"] == "declared_summary_total_mismatch"
    assert "TOTAL_STAFF = 18" in result.payload["validation_error_message"]
    assert len(result.payload["items"]) == 18
    assert any(item["staff_name"] == "Joycelyn Alu" and item["status"] == "present" for item in result.payload["items"])

    routed = route_conversation_response(
        result,
        source_message_id="wamid.lae-split-line",
        sender_phone="67570000001",
    )
    rendered = render_whatsapp_response(routed)

    assert routed["response_type"] == "review_ack"
    assert "TAOP could not identify the exact validation issue" not in rendered["response_text"]
    assert "Declared attendance summary" in rendered["response_text"]


def test_lae_joined_line_report_still_reviews_when_summary_totals_do_not_reconcile(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_runtime_paths(monkeypatch, tmp_path)

    result = process_work_item(_attendance_work_item(_sample_text(_JOINED_LINE_SAMPLE), sender="lae-joined-line"))

    assert result.payload["status"] == "needs_review"
    assert result.payload["confidence"] >= 0.9
    assert result.payload["validation_error_code"] == "declared_summary_total_mismatch"
    assert result.payload["validation_error_message"] == (
        "Declared attendance summary PRESENT + SUSPEND + LAY_OFF = 17 but TOTAL_STAFF = 18."
    )
    assert result.payload["metrics"]["present"] == 16
    assert result.payload["metrics"]["suspend"] == 1
    assert result.payload["metrics"]["lay_off"] == 1
    assert result.payload["metrics"]["total_staff"] == 18


def test_lae_corrected_report_is_accepted_ack(tmp_path: Path, monkeypatch) -> None:
    _patch_runtime_paths(monkeypatch, tmp_path)

    result = process_work_item(_attendance_work_item(_sample_text(_CORRECTED_SAMPLE), sender="lae-corrected"))

    assert result.payload["status"] == "accepted"
    assert result.payload["confidence"] >= 0.9
    assert result.payload.get("validation_error_code") is None
    assert result.payload.get("validation_error_message") is None
    assert result.payload["metrics"]["present"] == 16
    assert result.payload["metrics"]["suspend"] == 1
    assert result.payload["metrics"]["lay_off"] == 1
    assert result.payload["metrics"]["total_staff"] == 18

    structured_path = tmp_path / "records" / "structured" / "hr_attendance" / "lae_5th_street" / "2026-05-01.json"
    routed = route_conversation_response(
        AgentResult(
            agent_name=result.agent_name,
            payload={
                **result.payload,
                "outputs": [str(structured_path)],
            },
            metadata=result.metadata,
        ),
        source_message_id="wamid.lae-corrected",
        sender_phone="67570000002",
    )

    assert routed["response_type"] == "accepted_ack"
    assert structured_path.exists()


def _attendance_work_item(text: str, *, sender: str) -> WorkItem:
    return WorkItem(
        kind="raw_message",
        payload={
            "source": "whatsapp",
            "raw_message": {"text": text},
            "metadata": {
                "received_at": "2026-05-01T09:00:00Z",
                "sender": sender,
            },
        },
    )


def _sample_text(filename: str) -> str:
    return (
        Path(__file__).resolve().parents[1]
        / "records"
        / "raw"
        / "whatsapp"
        / "unknown"
        / filename
    ).read_text(encoding="utf-8")


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
