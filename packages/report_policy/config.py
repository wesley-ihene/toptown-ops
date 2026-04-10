"""Load file-backed report policy configuration."""

from __future__ import annotations

from dataclasses import dataclass
import json
from functools import lru_cache
from typing import Any

from packages.common.paths import REPO_ROOT

_CONFIG_PATH = REPO_ROOT / "config" / "report_policy.json"
_REPORT_TYPE_ALIASES = {
    "pricing_stock_release": "bale_summary",
    "bale_release": "bale_summary",
    "staff_attendance": "attendance",
    "hr_attendance": "attendance",
}


@dataclass(slots=True, frozen=True)
class ConfidenceThresholds:
    """Confidence thresholds for one report type."""

    auto_accept_min: float
    review_min: float
    reject_max: float

    def to_payload(self) -> dict[str, float]:
        """Return a JSON-safe thresholds payload."""

        return {
            "auto_accept_min": self.auto_accept_min,
            "review_min": self.review_min,
            "reject_max": self.reject_max,
        }


@dataclass(slots=True, frozen=True)
class ReportPolicy:
    """Loaded policy for one canonical report type."""

    report_type: str
    fallback_enabled: bool
    confidence_thresholds: ConfidenceThresholds


def get_report_policy(report_type: str) -> ReportPolicy:
    """Return one canonical policy for a report type or alias."""

    canonical_type = _REPORT_TYPE_ALIASES.get(report_type, report_type)
    config = _load_config()["report_types"]
    raw_policy = config.get(canonical_type)
    if not isinstance(raw_policy, dict):
        raise ValueError(f"Missing report policy config for report type {report_type!r}.")
    thresholds = raw_policy.get("confidence_thresholds")
    if not isinstance(thresholds, dict):
        raise ValueError(f"Missing confidence thresholds for report type {canonical_type!r}.")
    policy = ReportPolicy(
        report_type=canonical_type,
        fallback_enabled=bool(raw_policy.get("fallback_enabled")),
        confidence_thresholds=ConfidenceThresholds(
            auto_accept_min=float(thresholds["auto_accept_min"]),
            review_min=float(thresholds["review_min"]),
            reject_max=float(thresholds["reject_max"]),
        ),
    )
    _validate_thresholds(policy)
    return policy


@lru_cache(maxsize=1)
def _load_config() -> dict[str, Any]:
    """Load the JSON config once per process."""

    payload = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Report policy config must contain a JSON object.")
    return payload


def _validate_thresholds(policy: ReportPolicy) -> None:
    """Validate threshold ordering for one report type."""

    thresholds = policy.confidence_thresholds
    if not (0.0 <= thresholds.reject_max <= thresholds.review_min <= thresholds.auto_accept_min <= 1.0):
        raise ValueError(
            f"Invalid threshold ordering for report type {policy.report_type!r}: "
            f"{thresholds.to_payload()!r}"
        )
