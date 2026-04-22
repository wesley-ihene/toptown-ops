"""Tests for deterministic autonomous action storage."""

from __future__ import annotations

import json
from pathlib import Path

from packages.action_store import write_action_record


def test_action_json_is_written_to_deterministic_path(tmp_path: Path) -> None:
    result = write_action_record(_action_payload(), output_root=tmp_path)

    action_path = Path(result["action_path"])
    assert action_path == tmp_path / "records" / "actions" / "2026-04-07" / "waigani" / "low_conversion_rate" / "abc123def4567890.json"
    assert json.loads(action_path.read_text(encoding="utf-8"))["rule_code"] == "low_conversion_rate"


def test_whatsapp_preview_is_written(tmp_path: Path) -> None:
    result = write_action_record(_action_payload(), output_root=tmp_path)

    preview_path = Path(result["preview_path"])
    preview_text = preview_path.read_text(encoding="utf-8")
    assert preview_path == tmp_path / "records" / "actions" / "2026-04-07" / "waigani" / "low_conversion_rate" / "abc123def4567890.whatsapp.txt"
    assert "TOPTOWN ACTION HIGH" in preview_text
    assert "Action ID: abc123def4567890" in preview_text


def test_dedupe_idempotent_update_reuses_existing_action_path(tmp_path: Path) -> None:
    first = write_action_record(_action_payload(), output_root=tmp_path)
    updated_payload = _action_payload(priority="medium")

    second = write_action_record(updated_payload, output_root=tmp_path)

    assert first["action_path"] == second["action_path"]
    assert json.loads(Path(second["action_path"]).read_text(encoding="utf-8"))["priority"] == "medium"


def test_evidence_and_source_paths_are_preserved(tmp_path: Path) -> None:
    result = write_action_record(_action_payload(), output_root=tmp_path)

    payload = json.loads(Path(result["action_path"]).read_text(encoding="utf-8"))
    assert payload["evidence"] == {
        "conversion_rate": 0.2,
        "served": 2,
        "threshold": 0.35,
        "traffic": 10,
    }
    assert payload["source_paths"] == [
        "records/structured/sales_income/waigani/2026-04-07.json",
        "records/structured/sales_income/waigani/2026-04-07.governance.json",
    ]


def test_priority_assignment_and_expiry_fields_are_written(tmp_path: Path) -> None:
    result = write_action_record(_action_payload(), output_root=tmp_path)

    payload = json.loads(Path(result["action_path"]).read_text(encoding="utf-8"))
    assert payload["priority"] == "high"
    assert payload["assigned_to"] == "branch_supervisor"
    assert payload["expires_at"] == "2026-04-08T23:59:59Z"
    assert payload["delivery_status"] == "pending_manual_dispatch"


def _action_payload(*, priority: str = "high") -> dict[str, object]:
    return {
        "action_id": "abc123def4567890",
        "action_type": "low_conversion_rate",
        "rule_code": "low_conversion_rate",
        "branch": "waigani",
        "report_date": "2026-04-07",
        "signal_type": "sales_income",
        "severity": "warning",
        "priority": priority,
        "assigned_to": "branch_supervisor",
        "requires_ack": True,
        "status": "pending",
        "expires_at": "2026-04-08T23:59:59Z",
        "dedupe_key": "waigani:2026-04-07:low_conversion_rate:branch_conversion",
        "scope_key": "branch_conversion",
        "summary": "Review floor engagement and stock availability due to low conversion.",
        "evidence": {
            "traffic": 10,
            "served": 2,
            "conversion_rate": 0.2,
            "threshold": 0.35,
        },
        "source_paths": [
            "records/structured/sales_income/waigani/2026-04-07.json",
            "records/structured/sales_income/waigani/2026-04-07.governance.json",
        ],
    }
