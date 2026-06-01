"""Focused tests for adaptive SOP learning and safe application."""

from __future__ import annotations

import json
from pathlib import Path

import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from apps.adaptive_sop_engine.storage import variation_registry_path
from apps.sales_income_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem
from scripts import sop_learning_admin


def test_observes_new_variation_without_auto_applying(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            date="07/04/2026",
            lines=[
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 600",
                "Walk In: 12",
                "Served: 10",
            ],
        )
    )

    registry = _read_json(variation_registry_path(output_root=tmp_path))

    assert result.payload["metrics"]["traffic"] is None
    assert result.payload["adaptive_sop"]["corrections_applied"] == []
    assert result.payload["adaptive_sop"]["observed_variations"] == [
        {
            "confidence": 0.55,
            "count": 1,
            "first_seen": "2026-04-07",
            "last_branch": "waigani",
            "last_report_date": "2026-04-07",
            "last_seen": "2026-04-07",
            "mapped_to": "Traffic",
            "report_type": "sales_income",
            "source_field": "customer_count",
            "source_label": "Walk In",
            "status": "observed",
        }
    ]
    assert registry["sales_income"]["customer_count"]["Walk In"]["status"] == "observed"
    assert registry["sales_income"]["customer_count"]["Walk In"]["count"] == 1


def test_repeated_variation_becomes_suggested(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    for report_date in ("07/04/2026", "08/04/2026", "09/04/2026"):
        process_work_item(
            _sales_work_item(
                date=report_date,
                lines=[
                    "DAY-END SALES REPORT",
                    "Branch: Waigani Branch",
                    f"Date: {report_date}",
                    "Gross Sales: 1200",
                    "Cash Sales: 600",
                    "Eftpos Sales: 600",
                    "Walk In: 12",
                    "Served: 10",
                ],
            )
        )

    registry = _read_json(variation_registry_path(output_root=tmp_path))
    review_queue = _read_json(tmp_path / "records" / "sop_learning" / "review_queue" / "2026-04-09.json")

    entry = registry["sales_income"]["customer_count"]["Walk In"]
    assert entry["status"] == "suggested"
    assert entry["count"] == 3
    assert review_queue["pending_suggestions"] == [
        {
            "confidence": 0.75,
            "count": 3,
            "first_seen": "2026-04-07",
            "last_branch": "waigani",
            "last_report_date": "2026-04-09",
            "last_seen": "2026-04-09",
            "mapped_to": "Traffic",
            "report_type": "sales_income",
            "review_message": "Observed repeated label 'Walk In' mapping to 'Traffic'. Review for approval.",
            "source_field": "customer_count",
            "source_label": "Walk In",
            "status": "suggested",
        }
    ]


def test_approved_variation_auto_applies(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)
    _write_registry(
        tmp_path,
        {
            "sales_income": {
                "customer_count": {
                    "Walk In": {
                        "mapped_to": "Traffic",
                        "count": 3,
                        "first_seen": "2026-04-07",
                        "last_seen": "2026-04-09",
                        "status": "approved",
                        "confidence": 0.94,
                        "report_type": "sales_income",
                        "source_field": "customer_count",
                        "last_branch": "waigani",
                        "last_report_date": "2026-04-09",
                    }
                }
            }
        },
    )

    result = process_work_item(
        _sales_work_item(
            date="10/04/2026",
            lines=[
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 10/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 600",
                "Walk In: 12",
                "Served: 10",
            ],
        )
    )

    structured_payload = _read_json(tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-10.json")

    assert result.payload["status"] == "accepted"
    assert result.payload["metrics"]["traffic"] == 12
    assert result.payload["adaptive_sop"]["corrections_applied"] == [
        {"source_label": "Walk In", "mapped_to": "Traffic"}
    ]
    assert "⚠️ ADAPTIVE SOP CORRECTIONS APPLIED" in result.payload["adaptive_sop"]["feedback_message"]
    assert structured_payload["metrics"]["traffic"] == 12
    assert structured_payload["adaptive_sop"]["corrections_applied"] == [
        {"source_label": "Walk In", "mapped_to": "Traffic"}
    ]


def test_rejected_variation_never_auto_applies(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)
    _write_registry(
        tmp_path,
        {
            "sales_income": {
                "customer_count": {
                    "Walk In": {
                        "mapped_to": "Traffic",
                        "count": 4,
                        "first_seen": "2026-04-07",
                        "last_seen": "2026-04-09",
                        "status": "rejected",
                        "confidence": 0.0,
                        "report_type": "sales_income",
                        "source_field": "customer_count",
                        "last_branch": "waigani",
                        "last_report_date": "2026-04-09",
                    }
                }
            }
        },
    )

    result = process_work_item(
        _sales_work_item(
            date="10/04/2026",
            lines=[
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 10/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 600",
                "Walk In: 12",
                "Served: 10",
            ],
        )
    )

    assert result.payload["metrics"]["traffic"] is None
    assert result.payload["adaptive_sop"]["corrections_applied"] == []


def test_financial_fields_are_never_adaptively_changed(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)
    _write_registry(
        tmp_path,
        {
            "sales_income": {
                "financial": {
                    "Takings": {
                        "mapped_to": "Total_Sales",
                        "count": 3,
                        "first_seen": "2026-04-07",
                        "last_seen": "2026-04-09",
                        "status": "approved",
                        "confidence": 0.99,
                        "report_type": "sales_income",
                        "source_field": "financial",
                        "last_branch": "waigani",
                        "last_report_date": "2026-04-09",
                    }
                }
            }
        },
    )

    result = process_work_item(
        _sales_work_item(
            date="10/04/2026",
            lines=[
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 10/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 600",
                "Traffic: 12",
                "Served: 10",
                "Takings: 9999",
            ],
        )
    )

    assert result.payload["metrics"]["gross_sales"] == 1200.0
    assert result.payload["metrics"]["total_sales"] == 1200.0
    assert result.payload["adaptive_sop"]["corrections_applied"] == []


def test_structured_output_includes_adaptive_sop_metadata(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _sales_work_item(
            date="07/04/2026",
            lines=[
                "DAY-END SALES REPORT",
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Gross Sales: 1200",
                "Cash Sales: 600",
                "Eftpos Sales: 600",
                "Traffic: 12",
                "Served: 10",
            ],
        )
    )

    structured_payload = _read_json(tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json")

    assert result.payload["adaptive_sop"] == {
        "corrections_applied": [],
        "observed_variations": [],
        "suggested_mappings": [],
        "registry_version": "v1",
    }
    assert structured_payload["adaptive_sop"] == {
        "corrections_applied": [],
        "observed_variations": [],
        "suggested_mappings": [],
        "registry_version": "v1",
    }


def test_cli_approve_reject_updates_registry_safely(tmp_path: Path, capsys) -> None:
    assert sop_learning_admin.main(
        [
            "approve",
            "--root",
            str(tmp_path),
            "--report-type",
            "sales_income",
            "--source",
            "Main Door",
            "--target",
            "Traffic",
        ]
    ) == 0

    approved_registry = _read_json(variation_registry_path(output_root=tmp_path))
    assert approved_registry["sales_income"]["customer_count"]["Main Door"]["status"] == "approved"
    assert approved_registry["sales_income"]["customer_count"]["Main Door"]["confidence"] == 0.94
    capsys.readouterr()

    assert sop_learning_admin.main(["list", "--root", str(tmp_path)]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert listed == [
        {
            "confidence": 0.94,
            "count": 0,
            "first_seen": approved_registry["sales_income"]["customer_count"]["Main Door"]["first_seen"],
            "last_branch": None,
            "last_report_date": approved_registry["sales_income"]["customer_count"]["Main Door"]["last_report_date"],
            "last_seen": approved_registry["sales_income"]["customer_count"]["Main Door"]["last_seen"],
            "mapped_to": "Traffic",
            "report_type": "sales_income",
            "source_field": "customer_count",
            "source_label": "Main Door",
            "status": "approved",
        }
    ]

    assert sop_learning_admin.main(
        [
            "reject",
            "--root",
            str(tmp_path),
            "--report-type",
            "sales_income",
            "--source",
            "Main Door",
        ]
    ) == 0

    rejected_registry = _read_json(variation_registry_path(output_root=tmp_path))
    assert rejected_registry["sales_income"]["customer_count"]["Main Door"]["status"] == "rejected"
    assert rejected_registry["sales_income"]["customer_count"]["Main Door"]["confidence"] == 0.0


def _patch_output_paths(tmp_path: Path, monkeypatch) -> None:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "INTELLIGENCE_DIR", records_dir / "intelligence")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(record_paths, "REVIEW_DIR", records_dir / "review")
    monkeypatch.setattr(record_paths, "PROVENANCE_DIR", records_dir / "provenance")
    monkeypatch.setattr(record_paths, "OBSERVABILITY_DIR", records_dir / "observability")
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))


def _sales_work_item(*, date: str, lines: list[str]) -> WorkItem:
    return WorkItem(
        kind="raw_message",
        payload={
            "classification": {"report_type": "sales"},
            "raw_message": {"text": "\n".join(lines)},
        },
    )


def _write_registry(root: Path, payload: dict) -> None:
    path = variation_registry_path(output_root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
