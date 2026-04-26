"""Integration tests for deterministic WhatsApp command handling."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from packages.response_store import write_response_artifacts
from scripts import whatsapp_webhook_bridge as bridge


def test_help_command_bypasses_validator_and_orchestrator(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def should_not_run(*args, **kwargs):
        raise AssertionError("report pipeline should not run for commands")

    monkeypatch.setattr(bridge, "validate_inbound_text", should_not_run)
    monkeypatch.setattr(bridge.orchestrator_worker, "process_work_item", should_not_run)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.cmd.help", text="help")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = _read_json(Path(body["conversation_response"]["json_path"]))

    assert response.status_code == 200
    assert body["command"] is True
    assert body["command_name"] == "help"
    assert body["agent"] == "command_handler"
    assert body["orchestrator_status"] == "skipped"
    assert artifact_payload["response_type"] == "command_reply"
    assert artifact_payload["response_text"].startswith("Supported commands:")


def test_format_command_generates_template_reply_artifact(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(
            _meta_payload(message_id="wamid.cmd.format", text="format supervisor control")
        ).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = _read_json(Path(body["conversation_response"]["json_path"]))

    assert response.status_code == 200
    assert body["command_name"] == "format"
    assert artifact_payload["response_text"].startswith("SUPERVISOR CONTROL REPORT")


def test_status_command_reads_latest_response_record(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    _write_response(
        tmp_path,
        source_message_id="wamid.seed.status",
        sender_phone="67570000000",
        response_type="accepted_ack",
        governance_status="accepted",
        report_type="sales_income",
        branch="waigani",
        reason=None,
        generated_at="2026-04-07T12:00:00Z",
        response_text="accepted",
    )

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.cmd.status", text="status")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = _read_json(Path(body["conversation_response"]["json_path"]))

    assert response.status_code == 200
    assert artifact_payload["response_text"] == "Latest status: DAY-END SALES REPORT for Waigani is accepted."


def test_why_rejected_command_reads_last_rejection_reason(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    _write_response(
        tmp_path,
        source_message_id="wamid.seed.reject",
        sender_phone="67570000000",
        response_type="rejected_fix_request",
        governance_status="rejected",
        report_type="staff_attendance",
        branch="waigani",
        reason="validation_failed",
        generated_at="2026-04-07T12:00:00Z",
        response_text="rejected",
    )

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.cmd.why", text="why rejected")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))
    artifact_payload = _read_json(Path(body["conversation_response"]["json_path"]))

    assert response.status_code == 200
    assert artifact_payload["response_text"] == (
        "Last rejection: ATTENDANCE REPORT for Waigani. "
        "Reason: required report fields were missing or invalid."
    )


def test_replay_command_reply_is_suppressed_by_default(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)
    replay_payload = {
        "payload": {
            "text": "help",
            "message_id": "wamid.cmd.replay",
            "sender_name": "Replay User",
            "sender_phone": "67570000001",
            "group_name": "Waigani",
        },
        "metadata": {
            "received_at": "2026-04-07T12:00:00Z",
            "chat_id": "67570000001",
        },
        "replay": {
            "is_replay": True,
            "source": "raw",
            "original_path": "records/raw/whatsapp/unknown/original.txt",
            "replayed_at": "2026-04-07T12:30:00Z",
        },
    }

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(replay_payload).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert body["replay"] is True
    assert body["conversation_response"]["dispatch_status"] == "suppressed"


def test_command_response_failure_does_not_break_ingress(tmp_path: Path, monkeypatch) -> None:
    _patch_environment(monkeypatch, tmp_path)

    def explode(command, *, output_root=None):
        raise RuntimeError("command handler exploded")

    monkeypatch.setattr(bridge, "handle_whatsapp_command", explode)

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=json.dumps(_meta_payload(message_id="wamid.cmd.fail", text="help")).encode("utf-8"),
    )

    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert body["ok"] is True
    assert body["command"] is True
    assert body["conversation_response"] is None


def _meta_payload(*, message_id: str, text: str) -> dict[str, object]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "entry-1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {
                                "display_phone_number": "15551230000",
                                "phone_number_id": "pnid-1",
                            },
                            "contacts": [
                                {
                                    "profile": {"name": "Alice"},
                                    "wa_id": "67570000000",
                                }
                            ],
                            "messages": [
                                {
                                    "from": "67570000000",
                                    "id": message_id,
                                    "timestamp": "1775563200",
                                    "text": {"body": text},
                                    "type": "text",
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def _patch_environment(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setattr(bridge, "REPO_ROOT", tmp_path)
    monkeypatch.delenv("TOPTOWN_WHATSAPP_RESPONSE_MODE", raising=False)
    monkeypatch.delenv("TOPTOWN_ENABLE_REPLAY_RESPONSES", raising=False)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_response(
    root: Path,
    *,
    source_message_id: str,
    sender_phone: str,
    response_type: str,
    governance_status: str | None,
    report_type: str | None,
    branch: str | None,
    reason: str | None,
    generated_at: str,
    response_text: str,
) -> None:
    write_response_artifacts(
        {
            "source_message_id": source_message_id,
            "sender_phone": sender_phone,
            "response_type": response_type,
            "governance_status": governance_status,
            "report_type": report_type,
            "branch": branch,
            "reason": reason,
            "generated_at": generated_at,
            "response_text": response_text,
            "dispatch_status": "generated",
        },
        output_root=root,
    )
