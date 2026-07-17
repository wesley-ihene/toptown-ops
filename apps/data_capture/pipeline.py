"""Transactional-outbox adapter into TopTown's structured record boundary."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from packages.record_store.paths import get_structured_path_for_root
from packages.record_store.writer import write_json_file

from .db import CaptureStore


class PipelineDispatcher:
    """Idempotently materialize committed outbox events as pipeline records."""

    def __init__(self, store: CaptureStore, *, repo_root: str | Path) -> None:
        self.store = store
        self.repo_root = Path(repo_root)

    def dispatch_pending(self, *, limit: int = 100) -> dict[str, int]:
        summary = {"scanned": 0, "dispatched": 0, "failed": 0}
        for item in self.store.pending_outbox(limit):
            summary["scanned"] += 1
            try:
                self._dispatch(item)
            except Exception as error:
                self.store.mark_outbox(item["outbox_id"], success=False, error=str(error))
                summary["failed"] += 1
            else:
                self.store.mark_outbox(item["outbox_id"], success=True)
                summary["dispatched"] += 1
        return summary

    def _dispatch(self, item: dict[str, Any]) -> None:
        event = json.loads(item["payload_json"])
        path = get_structured_path_for_root(
            self.repo_root / "records" / "structured",
            signal_type=event["report_type"],
            branch=event["branch"],
            date=event["report_date"],
        )
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            existing_version = int((existing.get("capture") or {}).get("version", 0))
            if existing_version > int(event["version"]):
                return
        record = build_structured_record(event)
        write_json_file(path, record)
        governance = {
            "version": "v1",
            "status": "accepted",
            "source_status": "accepted",
            "export_allowed": True,
            "signal_type": event["report_type"],
            "report_family": event["report_type"],
            "branch": event["branch"],
            "report_date": event["report_date"],
            "normalized_scope": f"{event['report_type']}:{event['branch']}:{event['report_date']}",
            "message_id": None,
            "raw_sha256": None,
            "semantic_sha256": hashlib.sha256(item["payload_json"].encode("utf-8")).hexdigest(),
            "duplicate_of": None,
            "reasons": [],
            "warnings": [],
        }
        write_json_file(path.with_suffix(".governance.json"), governance)
        provenance_path = (
            self.repo_root / "records" / "provenance" / "accepted" / event["report_date"]
            / event["branch"] / event["report_type"] / f"{event['report_id']}.json"
        )
        write_json_file(
            provenance_path,
            {
                "outcome": "accept",
                "report_type": event["report_type"],
                "branch": event["branch"],
                "date": event["report_date"],
                "raw_message_hash": event["report_id"],
                "parser_used": "data_capture_form",
                "parse_mode": "structured_form",
                "confidence": 1.0,
                "warnings": record["warnings"],
                "validation_outcome": {"valid": True, "errors": []},
                "acceptance_outcome": {"decision": "accept", "reason": "validated_structured_form"},
                "downstream_references": {"structured_records": [_relative(path, self.repo_root)]},
                "submitted_by": event["submitted_by"],
                "submitted_at": event["submitted_at"],
                "authoritative_report_id": event["report_id"],
                "authoritative_version": event["version"],
            },
        )


def build_structured_record(event: dict[str, Any]) -> dict[str, Any]:
    """Map an authoritative form report to existing specialist record shapes."""

    report_type = event["report_type"]
    metrics = dict(event["metrics"])
    details = dict(event["details"])
    lines = list(event["lines"])
    warnings = [{"code": "structured_form_warning", "message": warning} for warning in event.get("warnings", [])]
    base: dict[str, Any] = {
        "signal_type": report_type,
        "branch": event["branch"],
        "branch_slug": event["branch"],
        "date": event["report_date"],
        "report_date": event["report_date"],
        "source": "web_form",
        "parse_mode": "structured_form",
        "confidence": 1.0,
        "status": "accepted",
        "export_allowed": True,
        "warnings": warnings,
        "capture": {
            "report_id": event["report_id"],
            "version": event["version"],
            "submitted_by": event["submitted_by"],
            "submitted_at": event["submitted_at"],
            "change_reason": event.get("change_reason"),
        },
    }
    if report_type == "sales_income":
        base["items"] = lines
        base["metrics"] = {
            **metrics,
            "gross_sales": metrics["total_sales"],
            "cash_sales": metrics["total_cash"],
            "eftpos_sales": metrics["total_card"],
            "z_reading": metrics["total_z_reading"],
            "cash_variance": metrics["total_variance"],
            "traffic": metrics["main_door"],
        }
        base["provenance"] = {
            "balanced_by": details.get("balanced_by"),
            "supervisor": details.get("balanced_by"),
            "supervisor_confirmation": "YES",
            "notes": [details["notes"]] if details.get("notes") else [],
        }
    elif report_type == "hr_performance":
        base["items"] = [{**line, "activity_score": line["items_moved"], "duty_status": "on_duty", "raw_section": line["section"]} for line in lines]
        base["metrics"] = metrics
    elif report_type == "hr_attendance":
        score = {"present": 1, "off": 0, "leave": 0, "sick": 0, "absent": 0}
        base["items"] = [{**line, "raw_status": line["status"], "presence_score": score[line["status"]]} for line in lines]
        base["metrics"] = metrics
    elif report_type == "pricing_stock_release":
        base["items"] = [{**line, "bale_id": str(line["record_number"])} for line in lines]
        base["metrics"] = metrics
        base["provenance"] = {"prepared_by": event["submitted_by_name"], "role": "pricing_clerk"}
    elif report_type == "supervisor_control":
        staffing = details.get("staffing_issues") or {}
        stock = details.get("stock_issues_affecting_sales") or {}
        pricing = details.get("pricing_or_system_issues") or {}
        base.update(
            {
                "report_family": "intelligence",
                "report_type": "supervisor_control",
                "cash_variance": metrics.get("cash_variance"),
                "cash_variance_detail": details.get("cash_variance_explanation"),
                "staffing_issues": staffing.get("has_issue", False),
                "staffing_issues_detail": staffing.get("detail"),
                "stock_issues_affecting_sales": stock.get("has_issue", False),
                "stock_issues_affecting_sales_detail": stock.get("detail"),
                "pricing_or_system_issues": pricing.get("has_issue", False),
                "pricing_or_system_issues_detail": pricing.get("detail"),
                "exceptions_escalated": details.get("exceptions_escalated"),
                "supervisor_confirmation": "YES",
                "metrics": {"exception_count": sum(bool(x.get("has_issue")) for x in (staffing, stock, pricing))},
                "provenance": {"supervisor": event["submitted_by_name"], "supervisor_confirmation": "YES", "notes": []},
            }
        )
    else:
        raise ValueError(f"Unsupported form report type: {report_type}")
    return base


def _relative(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)
