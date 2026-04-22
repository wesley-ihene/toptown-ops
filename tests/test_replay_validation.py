"""Tests for replay observability and validation artifacts."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.paths as record_paths
from scripts import replay_records


def test_replay_run_records_daily_audit_and_latency(tmp_path: Path, monkeypatch) -> None:
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
            "--dry-run",
        ]
    )

    assert exit_code == 0

    audit = _read_json(
        tmp_path / "records" / "observability" / "daily" / "2026_04_07" / "replay_audit.json"
    )
    assert audit["summary"] == {
        "total": 1,
        "structured_written": 0,
        "rejected": 0,
        "skipped": 1,
        "failed": 0,
    }
    assert audit["results"][0]["validation_mode"] == "dry_run"
    assert audit["results"][0]["branch"] == "waigani"

    latency = _read_json(
        tmp_path / "records" / "observability" / "daily" / "2026_04_07" / "pipeline_latency.json"
    )
    assert latency["summary"]["total_events"] == 2
    assert latency["summary"]["event_types"]["processing"]["count"] == 1
    assert latency["summary"]["event_types"]["replay"]["count"] == 1
    assert not (tmp_path / "records" / "actions").exists()


def test_validation_mode_writes_audit_and_compares_against_structured_baseline(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-07__waigani__sample.txt",
        _sales_report_text(),
    )
    _write_json(
        tmp_path
        / "tests"
        / "fixtures"
        / "replay_validation"
        / "raw"
        / "sales"
        / "2026-04-07__waigani__sample.expected.json",
        {
            "status": "structured_written",
            "report_family": "sales_income",
            "structured_outputs": [
                {
                    "signal_type": "sales_income",
                    "branch": "waigani",
                    "report_date": "2026-04-07",
                    "payload": {
                        "branch": "waigani",
                        "report_date": "2026-04-07",
                        "signal_type": "sales_income",
                        "source_agent": "sales_income_agent",
                        "status": "accepted_with_warning",
                    },
                }
            ],
        },
    )

    exit_code = replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "validation",
            "--path",
            str(raw_path),
            "--write-audit",
        ]
    )

    assert exit_code == 0
    assert not (tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json").exists()
    assert not (tmp_path / "records" / "observability").exists()

    audit = _read_json(tmp_path / "analytics" / "replay_audit" / "latest.json")
    assert audit["summary"] == {
        "total": 1,
        "stable": 1,
        "drift_detected": 0,
        "missing_expected": 0,
        "unexpected_acceptance": 0,
        "unexpected_rejection": 0,
        "error": 0,
    }
    assert audit["results"][0]["source_file"] == "records/raw/whatsapp/sales/2026-04-07__waigani__sample.txt"
    assert audit["results"][0]["status"] == "stable"
    assert audit["results"][0]["report_family"] == "sales_income"
    assert audit["results"][0]["actual_outcome"]["status"] == "structured_written"


def test_validation_mode_supports_rejected_inputs_with_controlled_expected_outcomes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    rejected_path = _write_text(
        tmp_path / "records" / "rejected" / "whatsapp" / "unknown" / "20260407T004814027636__unknown__unknown_report_type.txt",
        "Shift note only. Please call me when you arrive.",
    )
    _write_json(
        rejected_path.with_suffix(".meta.json"),
        {
            "attempted_report_type": "unknown",
            "received_at": "2026-04-07T11:05:00Z",
            "sender": "rejected-smoke",
            "source": "whatsapp",
        },
    )
    _write_json(
        tmp_path
        / "tests"
        / "fixtures"
        / "replay_validation"
        / "rejected"
        / "unknown"
        / "20260407T004814027636__unknown__unknown_report_type.expected.json",
        {
            "status": "rejected",
            "report_family": "unknown",
            "rejected_outputs": [
                {
                    "report_type": "unknown",
                    "rejection_reason": "unknown_report_type",
                    "attempted_report_type": "unknown",
                }
            ],
        },
    )

    exit_code = replay_records.main(
        [
            "--source",
            "rejected",
            "--mode",
            "validation",
            "--path",
            str(rejected_path),
            "--write-audit",
        ]
    )

    assert exit_code == 0
    rejected_files = sorted((tmp_path / "records" / "rejected" / "whatsapp" / "unknown").glob("*.txt"))
    assert rejected_files == [rejected_path]

    audit = _read_json(tmp_path / "analytics" / "replay_audit" / "latest.json")
    assert audit["results"][0]["status"] == "stable"
    assert audit["results"][0]["actual_outcome"]["status"] == "rejected"
    assert audit["results"][0]["report_family"] == "unknown"


def test_validation_mode_can_fail_on_drift_without_polluting_runtime_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-07__waigani__drift.txt",
        _sales_report_text(),
    )
    _write_json(
        tmp_path
        / "tests"
        / "fixtures"
        / "replay_validation"
        / "raw"
        / "sales"
        / "2026-04-07__waigani__drift.expected.json",
        {
            "status": "rejected",
            "report_family": "sales_income",
        },
    )

    exit_code = replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "validation",
            "--path",
            str(raw_path),
            "--write-audit",
            "--fail-on-drift",
        ]
    )

    assert exit_code == 1
    assert not (tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json").exists()

    audit = _read_json(tmp_path / "analytics" / "replay_audit" / "latest.json")
    assert audit["summary"]["unexpected_acceptance"] == 1
    assert audit["results"][0]["status"] == "unexpected_acceptance"
    assert "expected_rejection" in audit["results"][0]["reason"]


def _patch_replay_environment(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "ACTIONS_DIR", records_dir / "actions")
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setattr(replay_records, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(replay_records, "LOGS_REPLAY_DIR", tmp_path / "logs" / "replay")


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _sales_report_text() -> str:
    return "\n".join(
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
    )
