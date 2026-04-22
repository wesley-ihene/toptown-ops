"""Daily pipeline health metrics helpers."""

from __future__ import annotations

from typing import Any

from .types import utc_now_iso


def build_pipeline_health(report_date: str, summary: dict[str, Any]) -> dict[str, Any]:
    """Build a compact daily pipeline health payload."""

    summary_block = summary.get("summary", {}) if isinstance(summary, dict) else {}
    exports = summary.get("exports", {}) if isinstance(summary, dict) else {}
    intake_volume = _int(summary_block.get("intake_volume"))
    accept_count = _int(summary_block.get("accept_count"))
    review_count = _int(summary_block.get("review_count"))
    reject_count = _int(summary_block.get("reject_count"))
    export_failures = _int(exports.get("failure_count"))
    status = "healthy"
    if reject_count > 0 or export_failures > 0:
        status = "warning"
    if intake_volume > 0 and reject_count >= accept_count and reject_count > 0:
        status = "critical"

    return {
        "report_date": report_date,
        "generated_at_utc": utc_now_iso(),
        "status": status,
        "summary": {
            "intake_volume": intake_volume,
            "accept_count": accept_count,
            "review_count": review_count,
            "reject_count": reject_count,
            "export_success_count": _int(exports.get("success_count")),
            "export_failure_count": export_failures,
            "fallback_activation_count": _int(summary_block.get("fallback_activation_count")),
        },
        "rates": {
            "accept_rate": _ratio(accept_count, intake_volume),
            "review_rate": _ratio(review_count, intake_volume),
            "reject_rate": _ratio(reject_count, intake_volume),
            "export_failure_rate": _ratio(export_failures, _int(exports.get("success_count")) + export_failures),
            "fallback_activation_rate": _float(summary_block.get("fallback_activation_rate")),
        },
        "checks": [
            {
                "name": "rejections_present",
                "status": "pass" if reject_count == 0 else "warn",
                "value": reject_count,
            },
            {
                "name": "export_failures_present",
                "status": "pass" if export_failures == 0 else "warn",
                "value": export_failures,
            },
        ],
    }


def _int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: Any) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return round(float(value), 4)
    return 0.0


def _ratio(left: int, right: int) -> float:
    if right <= 0:
        return 0.0
    return round(left / right, 4)
