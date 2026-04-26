"""Tests for proposal review, simulation, apply, and audit flow."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from apps.proposals_store.store import write_optimization_proposal
from apps.supervisor_commands.worker import handle_supervisor_command
from apps.command_router.worker import route_whatsapp_command


def test_list_proposals_is_limited_to_supervisor_branches(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)
    _write_proposal(tmp_path, proposal_id="waigani-proposal", branch="waigani")
    _write_proposal(tmp_path, proposal_id="lae-proposal", branch="lae_malaita")

    command = route_whatsapp_command("list proposals", sender_phone="67570000000")
    response = handle_supervisor_command(command, output_root=tmp_path)

    assert command["command_name"] == "list_proposals"
    assert "waigani-proposal" in response["response_text"]
    assert "lae-proposal" not in response["response_text"]


def test_approve_simulate_and_apply_proposal_write_audit_and_action(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)
    _write_proposal(tmp_path, proposal_id="apply-me", branch="waigani")

    approve = handle_supervisor_command(
        route_whatsapp_command("approve proposal apply-me", sender_phone="67570000000"),
        output_root=tmp_path,
    )
    simulate = handle_supervisor_command(
        route_whatsapp_command("simulate proposal apply-me", sender_phone="67570000000"),
        output_root=tmp_path,
    )
    apply = handle_supervisor_command(
        route_whatsapp_command("apply proposal apply-me", sender_phone="67570000000"),
        output_root=tmp_path,
    )

    proposal = _read_json(next((tmp_path / "records" / "proposals").rglob("apply-me.json")))
    action_paths = sorted((tmp_path / "records" / "actions").rglob("*.json"))
    audit_paths = sorted((tmp_path / "records" / "audit" / "proposal_actions").rglob("*.json"))

    assert approve["response_text"] == "Proposal apply-me approved."
    assert "Simulation ready for proposal apply-me." in simulate["response_text"]
    assert apply["response_text"] == "Proposal apply-me applied."
    assert proposal["approval_status"] == "approved"
    assert proposal["apply_status"] == "applied"
    assert len(action_paths) == 1
    assert len(audit_paths) == 3


def test_apply_requires_approval_and_simulate_has_no_side_effect(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)
    _write_proposal(tmp_path, proposal_id="pending-proposal", branch="waigani")

    simulate = handle_supervisor_command(
        route_whatsapp_command("simulate proposal pending-proposal", sender_phone="67570000000"),
        output_root=tmp_path,
    )
    apply = handle_supervisor_command(
        route_whatsapp_command("apply proposal pending-proposal", sender_phone="67570000000"),
        output_root=tmp_path,
    )

    assert "Simulation ready for proposal pending-proposal." in simulate["response_text"]
    assert apply["response_text"] == "Proposal pending-proposal must be approved before apply."
    assert not (tmp_path / "records" / "actions").exists()


def test_replay_safe_proposal_action_is_rejected(tmp_path: Path, monkeypatch) -> None:
    _patch_record_paths(monkeypatch, tmp_path)
    _patch_supervisors(monkeypatch, tmp_path)
    _write_proposal(tmp_path, proposal_id="replay-proposal", branch="waigani")

    response = handle_supervisor_command(
        route_whatsapp_command("apply proposal replay-proposal", sender_phone="67570000000", is_replay=True),
        output_root=tmp_path,
    )

    assert response["response_text"] == "Proposal action failed for replay-proposal: replay must not execute proposal actions."
    assert not (tmp_path / "records" / "actions").exists()


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "PROPOSALS_DIR", records_dir / "proposals")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")


def _patch_supervisors(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config" / "supervisors.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        '{"supervisors":[{"name":"Alice","sender_phone":"67570000000","branches":["waigani"]}]}',
        encoding="utf-8",
    )
    import apps.supervisor_auth.worker as supervisor_auth_worker

    monkeypatch.setattr(supervisor_auth_worker, "_CONFIG_PATH", config_path)
    supervisor_auth_worker._load_supervisors_cached.cache_clear()


def _write_proposal(root: Path, *, proposal_id: str, branch: str) -> str:
    return write_optimization_proposal(
        {
            "proposal_id": proposal_id,
            "generated_date": "2026-04-23",
            "proposal_type": "branch_optimization_action",
            "status": "pending_review",
            "approval_status": "pending",
            "apply_status": "not_applied",
            "branch": branch,
            "report_type": "sales",
            "summary": "Create branch follow-up action.",
            "priority": "high",
            "requires_human_approval": True,
            "evidence": {
                "review_count": 3,
                "leading_reason": "conflicting_record_same_scope",
            },
            "proposed_action": {
                "action_id": f"{proposal_id}-action",
                "action_type": "report_quality_coaching",
                "rule_code": "report_quality_coaching",
                "branch": branch,
                "report_date": "2026-04-23",
                "signal_type": "optimization_proposal",
                "severity": "warning",
                "priority": "high",
                "assigned_to": "branch_supervisor",
                "requires_ack": True,
                "status": "pending",
                "expires_at": "2026-04-24T23:59:59Z",
                "dedupe_key": f"{branch}:2026-04-23:report_quality_coaching:conflicting_record_same_scope",
                "scope_key": "conflicting_record_same_scope",
                "summary": "Review branch report quality controls.",
                "evidence": {
                    "review_count": 3,
                },
                "source_paths": ["records/learning/review_summary/2026-04-23.json"],
            },
            "source_paths": [
                "records/learning/review_summary/2026-04-23.json",
                "analytics/branch_comparison/2026-04-23.json",
            ],
        },
        output_root=root,
    )


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
