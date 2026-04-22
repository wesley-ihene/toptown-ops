"""Tests for automatic Phase 6 rebuild and export after structured writes."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from packages.feedback_store import record_action_feedback
from packages.observability import load_daily_artifact
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from scripts import replay_records
from packages.record_store.writer import write_structured


def test_structured_write_automatically_rebuilds_analytics_and_exports_signals(tmp_path: Path, caplog) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()
    caplog.set_level(logging.INFO)

    written_path = write_structured(
        "hr_performance",
        "waigani",
        "2026-04-07",
        _hr_performance_payload(),
        root=tmp_path,
        colony_root=colony_root,
    )

    assert written_path == tmp_path / "records" / "structured" / "hr_performance" / "waigani" / "2026-04-07.json"
    assert _read_json(tmp_path / "analytics" / "staff_daily" / "waigani" / "2026-04-07.json")["summary_counts"][
        "active_staff_count"
    ] == 1
    assert _read_json(tmp_path / "analytics" / "section_daily" / "waigani" / "2026-04-07.json")["sections"][0][
        "section"
    ] == "mens_tshirt"

    branch_daily = _read_json(tmp_path / "analytics" / "branch_daily" / "waigani" / "2026-04-07.json")
    assert branch_daily["sources"] == {
        "sales_income": False,
        "hr_performance": True,
    }
    assert any(warning["code"] == "missing_sales_record" for warning in branch_daily["warnings"])

    comparison = _read_json(tmp_path / "analytics" / "branch_comparison" / "2026-04-07.json")
    assert comparison["ranked_branches_by_sales"][0]["branch"] == "waigani"
    executive_alerts = _read_json(tmp_path / "alerts" / "executive" / "2026-04-07" / "summary.json")
    executive_alerts_whatsapp = (
        tmp_path / "alerts" / "executive" / "2026-04-07" / "summary.whatsapp.txt"
    ).read_text(encoding="utf-8")
    assert executive_alerts["report_date"] == "2026-04-07"
    assert "TOPTOWN EXECUTIVE ALERTS 2026-04-07" in executive_alerts_whatsapp

    event = _read_json(
        colony_root
        / "SIGNALS"
        / "normalized"
        / "waigani"
        / "2026-04-07"
        / "staff_performance_report__waigani__2026-04-07.json"
    )
    assert event["signal_type"] == "staff_performance_report"
    assert event["payload"]["staff_records"][0]["staff_name"] == "Alice Demo"

    manifest = _read_json(
        colony_root / "SIGNALS" / "normalized" / "waigani" / "2026-04-07" / "_export_manifest.json"
    )
    assert manifest["summary"] == {
        "scanned": 5,
        "written": 1,
        "missing": 4,
        "skipped": 0,
        "failed": 0,
    }
    assert '"event": "analytics_rebuild_started"' in caplog.text
    assert '"event": "analytics_rebuild_completed"' in caplog.text
    assert '"event": "branch_comparison_rebuild_completed"' in caplog.text
    assert '"event": "colony_export_started"' in caplog.text
    assert '"event": "colony_export_completed"' in caplog.text
    assert caplog.text.index('"event": "colony_export_completed"') < caplog.text.index('"event": "executive_alerts_started"')
    assert '"event": "executive_alerts_started"' in caplog.text
    assert '"event": "executive_alerts_completed"' in caplog.text


def test_repeated_structured_write_refreshes_outputs_idempotently(tmp_path: Path) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    write_structured(
        "hr_performance",
        "waigani",
        "2026-04-07",
        _hr_performance_payload(),
        root=tmp_path,
        colony_root=colony_root,
    )

    write_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_income_payload(gross_sales=500.0),
        root=tmp_path,
        colony_root=colony_root,
    )

    first_branch_daily = _read_json(tmp_path / "analytics" / "branch_daily" / "waigani" / "2026-04-07.json")
    first_sales_event = _read_json(
        colony_root
        / "SIGNALS"
        / "normalized"
        / "waigani"
        / "2026-04-07"
        / "daily_sales_report__waigani__2026-04-07.json"
    )
    assert first_branch_daily["gross_sales"] == 500.0
    assert first_sales_event["payload"]["totals"]["gross_sales"] == 500.0

    write_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_income_payload(gross_sales=900.0),
        root=tmp_path,
        colony_root=colony_root,
    )

    second_branch_daily = _read_json(tmp_path / "analytics" / "branch_daily" / "waigani" / "2026-04-07.json")
    second_comparison = _read_json(tmp_path / "analytics" / "branch_comparison" / "2026-04-07.json")
    second_sales_event = _read_json(
        colony_root
        / "SIGNALS"
        / "normalized"
        / "waigani"
        / "2026-04-07"
        / "daily_sales_report__waigani__2026-04-07.json"
    )
    manifest = _read_json(
        colony_root / "SIGNALS" / "normalized" / "waigani" / "2026-04-07" / "_export_manifest.json"
    )

    assert second_branch_daily["gross_sales"] == 900.0
    assert second_comparison["ranked_branches_by_sales"][0]["gross_sales"] == 900.0
    assert second_sales_event["payload"]["totals"]["gross_sales"] == 900.0
    assert second_sales_event["source_record_sha256"] != first_sales_event["source_record_sha256"]
    assert manifest["summary"] == {
        "scanned": 5,
        "written": 2,
        "missing": 3,
        "skipped": 0,
        "failed": 0,
    }


def test_downstream_failure_does_not_erase_structured_write(tmp_path: Path, monkeypatch, caplog) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()
    caplog.set_level(logging.INFO)

    def _boom(*args, **kwargs):
        raise RuntimeError("export bridge unavailable")

    monkeypatch.setattr(record_automation, "export_all_record_types", _boom)

    written_path = write_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_income_payload(gross_sales=500.0),
        root=tmp_path,
        colony_root=colony_root,
    )

    assert written_path.exists()
    assert _read_json(written_path)["metrics"]["gross_sales"] == 500.0
    assert _read_json(tmp_path / "analytics" / "branch_daily" / "waigani" / "2026-04-07.json")["gross_sales"] == 500.0
    assert not (
        colony_root
        / "SIGNALS"
        / "normalized"
        / "waigani"
        / "2026-04-07"
        / "_export_manifest.json"
    ).exists()
    assert '"event": "downstream_automation_failure"' in caplog.text
    assert '"affected_record_types": ["sales_income"]' in caplog.text
    assert '"report_date": "2026-04-07"' in caplog.text


def test_action_engine_runs_from_automation_layer_and_writes_previews(tmp_path: Path) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    write_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_income_payload(gross_sales=500.0, traffic=10, served=2, conversion_rate=0.2),
        root=tmp_path,
        colony_root=colony_root,
    )

    action_path = tmp_path / "records" / "actions" / "2026-04-07" / "waigani" / "low_conversion_rate"
    artifact = sorted(action_path.glob("*.json"))
    previews = sorted(action_path.glob("*.whatsapp.txt"))
    observability = load_daily_artifact("autonomous_actions", "2026-04-07", output_root=tmp_path)
    linked_review = tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales_income" / f"{artifact[0].stem}.json"
    feedback_summary = load_daily_artifact("feedback_summary", "2026-04-07", output_root=tmp_path)

    assert len(artifact) == 1
    assert len(previews) == 1
    assert _read_json(artifact[0])["rule_code"] == "low_conversion_rate"
    assert "TOPTOWN ACTION HIGH" in previews[0].read_text(encoding="utf-8")
    assert linked_review.exists()
    assert _read_json(linked_review)["linked_action_id"] == artifact[0].stem
    assert observability is not None
    assert observability["summary"]["actions_generated"] == 1
    assert feedback_summary is not None
    assert feedback_summary["summary"]["review_linked_actions"] == 1


def test_feedback_artifacts_do_not_block_follow_on_automation_or_export(tmp_path: Path) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    write_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_income_payload(gross_sales=500.0, traffic=10, served=2, conversion_rate=0.2),
        root=tmp_path,
        colony_root=colony_root,
    )

    action_path = next((tmp_path / "records" / "actions" / "2026-04-07" / "waigani" / "low_conversion_rate").glob("*.json"))
    record_action_feedback(
        action_id=action_path.stem,
        branch="waigani",
        report_date="2026-04-07",
        status="acknowledged",
        acknowledged_by="Ops One",
        acknowledged_at="2026-04-07T10:00:00Z",
        source_action_path=str(action_path),
        linked_review_queue_path=str(tmp_path / "records" / "review" / "2026_04_07" / "waigani" / "sales_income" / f"{action_path.stem}.json"),
        output_root=tmp_path,
    )

    write_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_income_payload(gross_sales=900.0, traffic=10, served=2, conversion_rate=0.2),
        root=tmp_path,
        colony_root=colony_root,
    )

    manifest = _read_json(
        colony_root / "SIGNALS" / "normalized" / "waigani" / "2026-04-07" / "_export_manifest.json"
    )
    feedback_path = tmp_path / "records" / "feedback" / "2026-04-07" / "waigani" / f"{action_path.stem}.json"

    assert feedback_path.exists()
    assert manifest["summary"]["written"] >= 1
    assert (
        colony_root
        / "SIGNALS"
        / "normalized"
        / "waigani"
        / "2026-04-07"
        / "daily_sales_report__waigani__2026-04-07.json"
    ).exists()


def test_action_generation_failure_is_non_blocking_and_export_still_proceeds(tmp_path: Path, monkeypatch) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    def _boom(*args, **kwargs):
        raise RuntimeError("autonomous control unavailable")

    monkeypatch.setattr(record_automation, "generate_control_actions", _boom)

    written_path = write_structured(
        "sales_income",
        "waigani",
        "2026-04-07",
        _sales_income_payload(gross_sales=500.0, traffic=10, served=2, conversion_rate=0.2),
        root=tmp_path,
        colony_root=colony_root,
    )

    assert written_path.exists()
    assert (
        colony_root
        / "SIGNALS"
        / "normalized"
        / "waigani"
        / "2026-04-07"
        / "daily_sales_report__waigani__2026-04-07.json"
    ).exists()
    assert not (tmp_path / "records" / "actions").exists()


def test_replay_path_triggers_same_postprocess_flow(tmp_path: Path, monkeypatch) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))
    _patch_replay_environment(monkeypatch, tmp_path)

    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-07__waigani__sample.txt",
        "\n".join(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 600",
                "Till Total: 600",
                "Deposit Total: 600",
                "Traffic: 12",
                "Served: 9",
                "Labor Hours: 4",
            ]
        ),
    )

    exit_code = replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "orchestrator",
            "--path",
            str(raw_path),
        ]
    )

    assert exit_code == 0
    assert (tmp_path / "analytics" / "branch_daily" / "waigani" / "2026-04-07.json").exists()
    assert (tmp_path / "analytics" / "branch_comparison" / "2026-04-07.json").exists()
    assert (
        colony_root
        / "SIGNALS"
        / "normalized"
        / "waigani"
        / "2026-04-07"
        / "daily_sales_report__waigani__2026-04-07.json"
    ).exists()
    assert (
        colony_root / "SIGNALS" / "normalized" / "waigani" / "2026-04-07" / "_export_manifest.json"
    ).exists()
    assert (tmp_path / "alerts" / "executive" / "2026-04-07" / "summary.json").exists()
    assert not (tmp_path / "records" / "actions").exists()
    observability = load_daily_artifact("autonomous_actions", "2026-04-07", output_root=tmp_path)
    assert observability is not None
    assert observability["summary"]["actions_suppressed_replay"] == 1


def test_replay_suppresses_actions_by_default(tmp_path: Path, monkeypatch) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))
    _patch_replay_environment(monkeypatch, tmp_path)

    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-07__waigani__actioncheck.txt",
        "\n".join(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 600",
                "Till Total: 600",
                "Deposit Total: 600",
                "Traffic: 10",
                "Served: 2",
                "Labor Hours: 4",
            ]
        ),
    )

    assert replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "orchestrator",
            "--path",
            str(raw_path),
        ]
    ) == 0

    assert not (tmp_path / "records" / "actions").exists()



def test_replay_overwrite_refreshes_executive_alert_artifacts_idempotently(tmp_path: Path, monkeypatch) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))
    _patch_replay_environment(monkeypatch, tmp_path)

    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-07__waigani__sample.txt",
        "\n".join(
            [
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 600",
                "Till Total: 600",
                "Deposit Total: 600",
                "Traffic: 12",
                "Served: 9",
                "Labor Hours: 4",
            ]
        ),
    )

    assert replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "orchestrator",
            "--path",
            str(raw_path),
        ]
    ) == 0

    summary_path = tmp_path / "alerts" / "executive" / "2026-04-07" / "summary.json"
    branch_path = tmp_path / "alerts" / "executive" / "2026-04-07" / "waigani.json"
    first_summary = _read_json(summary_path)
    first_branch = _read_json(branch_path)

    assert replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "orchestrator",
            "--path",
            str(raw_path),
            "--overwrite",
        ]
    ) == 0

    second_summary = _read_json(summary_path)
    second_branch = _read_json(branch_path)
    assert first_summary["report_date"] == second_summary["report_date"] == "2026-04-07"
    assert first_branch["branch"] == second_branch["branch"] == "waigani"


def _hr_performance_payload() -> dict[str, object]:
    return {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "signal_type": "hr",
        "signal_subtype": "staff_performance",
        "status": "accepted",
        "warnings": [],
        "items": [
            {
                "record_number": 1,
                "staff_name": "Alice Demo",
                "section": "mens_tshirt",
                "raw_section": "Men's Tshirt",
                "role": "Cashier",
                "duty_status": "on_duty",
                "items_moved": 10,
                "assisting_count": 2,
                "activity_score": 11.0,
                "notes": [],
            }
        ],
        "metrics": {
            "total_staff_records": 1,
            "total_items_moved": 10,
            "total_assisting_count": 2,
            "unresolved_section_count": 0,
        },
        "diagnostics": {
            "section_resolution_stats": {
                "resolved_count": 1,
                "unresolved_count": 0,
                "unresolved_examples": [],
            }
        },
        "provenance": {
            "notes": [],
        },
    }


def _sales_income_payload(
    *,
    gross_sales: float,
    traffic: int = 10,
    served: int = 5,
    conversion_rate: float | None = None,
) -> dict[str, object]:
    return {
        "branch": "waigani",
        "report_date": "2026-04-07",
        "signal_type": "sales_income",
        "status": "accepted",
        "warnings": [],
        "metrics": {
            "gross_sales": gross_sales,
            "cash_sales": gross_sales,
            "traffic": traffic,
            "served": served,
            "conversion_rate": conversion_rate if conversion_rate is not None else round(served / traffic, 4),
            "sales_per_labor_hour": gross_sales / 2,
        },
        "provenance": {
            "notes": [],
        },
    }


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _patch_replay_environment(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "ACTIONS_DIR", records_dir / "actions")
    monkeypatch.setattr(record_paths, "FEEDBACK_DIR", records_dir / "feedback")
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setattr(replay_records, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(replay_records, "LOGS_REPLAY_DIR", tmp_path / "logs" / "replay")


def _write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path
