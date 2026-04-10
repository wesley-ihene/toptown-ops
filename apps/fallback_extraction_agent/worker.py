"""Schema-bound fallback extraction after strict specialist parsing fails."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import re
from typing import Any

from packages.common.branch import canonical_branch_slug
from packages.common.date import normalize_report_date
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "fallback_extraction_agent"
SIGNAL_TYPE = "fallback_extraction"
_SUPPORTED_REPORT_TYPES = {
    "sales",
    "bale_summary",
    "staff_attendance",
    "staff_performance",
    "supervisor_control",
}
_STATUS_PATTERN = re.compile(r"\b(present|absent|off|leave)\b", flags=re.IGNORECASE)
_NUMERIC_PATTERN = re.compile(r"-?\d+(?:[.,]\d+)?")


@dataclass(slots=True)
class FallbackExtraction:
    """One schema-bound fallback extraction result."""

    report_type: str
    normalized_report: dict[str, Any]
    confidence: float
    warnings: list[dict[str, str]]
    provenance: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        """Return the JSON-safe fallback payload."""

        return {
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "report_type": self.report_type,
            "parse_mode": "fallback",
            "confidence": self.confidence,
            "warnings": self.warnings,
            "provenance": self.provenance,
            "normalized_report": self.normalized_report,
            "status": "extracted",
        }


def process_work_item(work_item: WorkItem) -> AgentResult:
    """Extract one normalized report payload after strict parsing failure."""

    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    report_type = _report_type(payload)
    text = _raw_text(payload)
    if report_type not in _SUPPORTED_REPORT_TYPES or not text:
        return AgentResult(
            agent_name=AGENT_NAME,
            payload={
                "signal_type": SIGNAL_TYPE,
                "source_agent": AGENT_NAME,
                "report_type": report_type,
                "parse_mode": "fallback",
                "confidence": 0.0,
                "warnings": [
                    _warning(
                        code="fallback_unavailable",
                        severity="error",
                        message="Fallback extraction requires a supported report type and non-empty raw text.",
                    )
                ],
                "provenance": _base_provenance(payload, branch_raw=None, report_date_raw=None),
                "normalized_report": {},
                "status": "invalid_input",
            },
        )

    extraction = _extract_report(report_type=report_type, text=text, payload=payload)
    return AgentResult(agent_name=AGENT_NAME, payload=extraction.to_payload())


def _extract_report(*, report_type: str, text: str, payload: dict[str, Any]) -> FallbackExtraction:
    """Return a schema-bound extraction for one supported report type."""

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    branch_raw = _extract_labeled_value(lines, "branch") or _branch_hint(payload)
    report_date_raw = _extract_labeled_value(lines, "date") or _routing_value(payload, "raw_report_date") or _routing_value(payload, "report_date")
    branch = canonical_branch_slug(branch_raw) if isinstance(branch_raw, str) and branch_raw.strip() else "unknown"
    report_date = normalize_report_date(report_date_raw) if isinstance(report_date_raw, str) and report_date_raw.strip() else "unknown"

    warnings: list[dict[str, str]] = []
    if branch == "unknown":
        warnings.append(_warning(code="missing_branch", severity="warning", message="Fallback extraction could not resolve a canonical branch."))
    if report_date == "unknown":
        warnings.append(_warning(code="missing_report_date", severity="warning", message="Fallback extraction could not resolve a report date."))

    if report_type == "sales":
        normalized_report = _extract_sales(lines, branch=branch, report_date=report_date, warnings=warnings)
    elif report_type == "bale_summary":
        normalized_report = _extract_bale_summary(lines, branch=branch, report_date=report_date, warnings=warnings)
    elif report_type == "staff_attendance":
        normalized_report = _extract_attendance(lines, branch=branch, report_date=report_date, warnings=warnings)
    elif report_type == "staff_performance":
        normalized_report = _extract_staff_performance(lines, branch=branch, report_date=report_date, warnings=warnings)
    else:
        normalized_report = _extract_supervisor_control(lines, branch=branch, report_date=report_date, warnings=warnings)

    confidence = _fallback_confidence(normalized_report=normalized_report, warnings=warnings)
    provenance = _base_provenance(payload, branch_raw=branch_raw, report_date_raw=report_date_raw)
    return FallbackExtraction(
        report_type=report_type,
        normalized_report=normalized_report,
        confidence=confidence,
        warnings=warnings,
        provenance=provenance,
    )


def _extract_sales(lines: list[str], *, branch: str, report_date: str, warnings: list[dict[str, str]]) -> dict[str, Any]:
    """Return a fallback sales payload."""

    metrics = {
        "gross_sales": _number_from_labels(lines, "gross sales", "total sales", "z reading") or 0.0,
        "cash_sales": _number_from_labels(lines, "cash sales") or 0.0,
        "eftpos_sales": _number_from_labels(lines, "eftpos sales", "card sales") or 0.0,
        "mobile_money_sales": _number_from_labels(lines, "mobile money sales") or 0.0,
        "traffic": int(_number_from_labels(lines, "traffic") or 0),
        "served": int(_number_from_labels(lines, "served") or 0),
    }
    if not any(metrics.values()):
        warnings.append(_warning(code="fallback_sparse", severity="warning", message="Fallback sales extraction found only partial metrics."))
    return {
        "branch": branch,
        "report_date": report_date,
        "metrics": metrics,
        "items": [],
    }


def _extract_bale_summary(lines: list[str], *, branch: str, report_date: str, warnings: list[dict[str, str]]) -> dict[str, Any]:
    """Return a fallback bale-summary payload."""

    items: list[dict[str, Any]] = []
    current_name: str | None = None
    current_qty: float | None = None
    current_amount: float | None = None
    item_index = 0
    for line in lines:
        normalized = line.casefold()
        if normalized.startswith("#"):
            if current_name is not None:
                items.append(_bale_item(item_index, current_name, current_qty, current_amount))
                item_index += 1
            current_name = line.split(".", 1)[-1].strip() if "." in line else line.lstrip("# ").strip()
            current_qty = None
            current_amount = None
            continue
        if "qty" in normalized:
            current_qty = _number_from_line(line)
            continue
        if "amt" in normalized or "amount" in normalized:
            current_amount = _number_from_line(line)
            continue
    if current_name is not None:
        items.append(_bale_item(item_index, current_name, current_qty, current_amount))

    if not items:
        warnings.append(_warning(code="fallback_sparse", severity="warning", message="Fallback bale extraction found no explicit item rows."))

    total_qty = int(sum(item["qty"] for item in items))
    total_amount = round(sum(item["amount"] for item in items), 2)
    metrics = {
        "bales_processed": int(_number_from_labels(lines, "bales processed") or 0),
        "bales_released": int(_number_from_labels(lines, "bales released") or 0),
        "total_qty": int(_number_from_labels(lines, "total qty") or total_qty),
        "total_amount": _number_from_labels(lines, "total amount") or total_amount,
    }
    return {
        "branch": branch,
        "report_date": report_date,
        "metrics": metrics,
        "items": items,
    }


def _extract_attendance(lines: list[str], *, branch: str, report_date: str, warnings: list[dict[str, str]]) -> dict[str, Any]:
    """Return a fallback attendance payload."""

    items: list[dict[str, Any]] = []
    counts = {"present": 0, "absent": 0, "off": 0, "leave": 0}
    for line in lines:
        match = re.match(r"^(?:\d+[.)-]?\s*)?(?P<name>.+?)\s*[-:]\s*(?P<status>present|absent|off|leave)\b", line, flags=re.IGNORECASE)
        if not match:
            continue
        status = match.group("status").lower()
        items.append({"staff_name": match.group("name").strip(), "status": status})
        counts[status] += 1
    if not items:
        warnings.append(_warning(code="fallback_sparse", severity="warning", message="Fallback attendance extraction found no attendance rows."))
    return {
        "branch": branch,
        "report_date": report_date,
        "metrics": {
            "total_staff_listed": len(items),
            "present_count": counts["present"],
            "absent_count": counts["absent"],
            "off_count": counts["off"],
            "leave_count": counts["leave"],
        },
        "items": items,
    }


def _extract_staff_performance(lines: list[str], *, branch: str, report_date: str, warnings: list[dict[str, str]]) -> dict[str, Any]:
    """Return a fallback staff-performance payload."""

    items: list[dict[str, Any]] = []
    current_name: str | None = None
    current_status: str | None = None
    for line in lines:
        header_match = re.match(r"^(?:\d+[.)]?\s*)?(?P<name>[A-Za-z][A-Za-z .'-]+?)\s*-\s*(?P<status>[^-]+)$", line)
        if header_match and "section" not in line.casefold():
            current_name = header_match.group("name").strip()
            current_status = header_match.group("status").strip()
            continue
        if current_name is None:
            continue
        if "items" in line.casefold() or "total items moved" in line.casefold():
            items_moved = int(_number_from_line(line) or 0)
            items.append(
                {
                    "staff_name": current_name,
                    "duty_status": current_status or "",
                    "items_moved": items_moved,
                }
            )
            current_name = None
            current_status = None
    if not items:
        warnings.append(_warning(code="fallback_sparse", severity="warning", message="Fallback staff-performance extraction found no staff rows."))
    return {
        "branch": branch,
        "report_date": report_date,
        "metrics": {
            "total_staff_records": len(items),
            "total_items_moved": int(sum(item["items_moved"] for item in items)),
        },
        "items": items,
    }


def _extract_supervisor_control(lines: list[str], *, branch: str, report_date: str, warnings: list[dict[str, str]]) -> dict[str, Any]:
    """Return a fallback supervisor-control payload."""

    exception_type = _extract_labeled_value(lines, "exception type")
    action_taken = _extract_labeled_value(lines, "action taken")
    supervisor_confirmed = _extract_labeled_value(lines, "supervisor confirmed")
    items: list[dict[str, Any]] = []
    if exception_type or action_taken or supervisor_confirmed:
        items.append(
            {
                "exception_type": (exception_type or "GENERAL_CONTROL").strip(),
                "action_taken": (action_taken or "pending").strip(),
                "supervisor_confirmed": (supervisor_confirmed or "NO").strip().upper(),
            }
        )
    else:
        for line in lines:
            if ":" not in line:
                continue
            key, value = [part.strip() for part in line.split(":", 1)]
            normalized_key = key.casefold()
            if normalized_key in {"branch", "date"}:
                continue
            items.append(
                {
                    "exception_type": normalized_key.replace(" ", "_").upper(),
                    "action_taken": value or "pending",
                    "supervisor_confirmed": "YES" if value.casefold() in {"yes", "passed", "resolved"} else "NO",
                }
            )
    if not items:
        warnings.append(_warning(code="fallback_sparse", severity="warning", message="Fallback supervisor-control extraction found no exception rows."))
    return {
        "branch": branch,
        "report_date": report_date,
        "metrics": {
            "exception_count": len(items),
        },
        "items": items,
    }


def _fallback_confidence(*, normalized_report: dict[str, Any], warnings: list[dict[str, str]]) -> float:
    """Return a conservative fallback confidence capped below auto-acceptance."""

    confidence = 0.74
    if normalized_report.get("branch") == "unknown":
        confidence -= 0.12
    if normalized_report.get("report_date") == "unknown":
        confidence -= 0.12
    items = normalized_report.get("items")
    if isinstance(items, list) and not items:
        confidence -= 0.18
    metrics = normalized_report.get("metrics")
    if isinstance(metrics, Mapping) and not any(float(value or 0) for value in metrics.values() if isinstance(value, (int, float))):
        confidence -= 0.12
    confidence -= 0.05 * len(warnings)
    return round(max(confidence, 0.0), 2)


def _report_type(payload: dict[str, Any]) -> str:
    """Return the routed specialist report type when present."""

    classification = payload.get("classification")
    if isinstance(classification, Mapping):
        report_type = classification.get("report_type")
        if isinstance(report_type, str):
            return report_type
    return "unknown"


def _raw_text(payload: dict[str, Any]) -> str:
    """Return the inbound raw text when present."""

    raw_message = payload.get("raw_message")
    if isinstance(raw_message, Mapping):
        text = raw_message.get("text")
        if isinstance(text, str):
            return text
    return ""


def _branch_hint(payload: dict[str, Any]) -> str | None:
    """Return the best branch hint from payload metadata or routing."""

    metadata = payload.get("metadata")
    if isinstance(metadata, Mapping):
        branch_hint = metadata.get("branch_hint")
        if isinstance(branch_hint, str) and branch_hint.strip():
            return branch_hint.strip()
    return _routing_value(payload, "branch_hint")


def _routing_value(payload: dict[str, Any], field_name: str) -> str | None:
    """Return one routing field string when present."""

    routing = payload.get("routing")
    if not isinstance(routing, Mapping):
        return None
    value = routing.get(field_name)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _extract_labeled_value(lines: list[str], label: str) -> str | None:
    """Return the first `label: value` match from lines."""

    prefix = f"{label.casefold()}:"
    for line in lines:
        if line.casefold().startswith(prefix):
            value = line.split(":", 1)[1].strip()
            return value or None
    return None


def _number_from_labels(lines: list[str], *labels: str) -> float | None:
    """Return the first numeric value found on a labeled line."""

    label_set = {label.casefold() for label in labels}
    for line in lines:
        normalized = line.casefold()
        if any(normalized.startswith(f"{label}:") for label in label_set):
            return _number_from_line(line)
    return None


def _number_from_line(line: str) -> float | None:
    """Return the first numeric token from a line."""

    matches = _NUMERIC_PATTERN.findall(line.replace(",", ""))
    if not matches:
        return None
    candidate = matches[-1].replace(",", ".")
    try:
        return float(candidate)
    except ValueError:
        return None


def _bale_item(index: int, item_name: str, qty: float | None, amount: float | None) -> dict[str, Any]:
    """Return one normalized bale item row."""

    return {
        "bale_id": f"fallback_{index + 1}",
        "item_name": item_name,
        "qty": int(qty or 0),
        "amount": float(amount or 0.0),
    }


def _base_provenance(payload: dict[str, Any], *, branch_raw: str | None, report_date_raw: str | None) -> dict[str, Any]:
    """Return stable fallback provenance fields."""

    text = _raw_text(payload)
    return {
        "raw_text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest() if text else None,
        "branch_raw": branch_raw,
        "report_date_raw": report_date_raw,
        "message_hash": payload.get("message_hash") if isinstance(payload.get("message_hash"), str) else None,
        "routing": dict(payload.get("routing", {})) if isinstance(payload.get("routing"), Mapping) else {},
        "classification": dict(payload.get("classification", {})) if isinstance(payload.get("classification"), Mapping) else {},
    }


def _warning(*, code: str, severity: str, message: str) -> dict[str, str]:
    """Return one JSON-safe warning."""

    return {
        "code": code,
        "severity": severity,
        "message": message,
    }
