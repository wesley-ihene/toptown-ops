"""Structured warning helpers for the sales income agent."""

from __future__ import annotations

from dataclasses import dataclass

LOW_CONVERSION_ALERT_TYPE = "low_conversion_rate"
MISSING_CUSTOMER_DATA_WARNING_CODE = "missing_customer_data"
PERFORMANCE_ALERT_WARNING_CODES = frozenset({"low_conversion", LOW_CONVERSION_ALERT_TYPE})


@dataclass(slots=True)
class WarningEntry:
    """Structured warning object used across the sales income agent."""

    code: str
    severity: str
    message: str

    def to_payload(self) -> dict[str, str]:
        """Return a JSON-safe warning payload."""

        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
        }


@dataclass(slots=True)
class AlertEntry:
    """Structured performance alert used when business signals should not block acceptance."""

    alert_type: str
    alert_level: str
    alert_message: str
    alert_category: str = "performance_alert"

    def to_payload(self) -> dict[str, str]:
        """Return a JSON-safe alert payload."""

        return {
            "alert_type": self.alert_type,
            "alert_level": self.alert_level,
            "alert_message": self.alert_message,
            "alert_category": self.alert_category,
        }


def make_warning(*, code: str, severity: str, message: str) -> WarningEntry:
    """Create a structured warning entry."""

    return WarningEntry(code=code, severity=severity, message=message)


def make_alert(
    *,
    alert_type: str,
    alert_level: str,
    alert_message: str,
    alert_category: str = "performance_alert",
) -> AlertEntry:
    """Create a structured alert entry."""

    return AlertEntry(
        alert_type=alert_type,
        alert_level=alert_level,
        alert_message=alert_message,
        alert_category=alert_category,
    )


def dedupe_warnings(warnings: list[WarningEntry]) -> list[WarningEntry]:
    """Return de-duplicated warnings keyed by warning code."""

    unique: dict[str, WarningEntry] = {}
    for warning in warnings:
        if warning.code not in unique:
            unique[warning.code] = warning
    return list(unique.values())


def dedupe_alerts(alerts: list[AlertEntry]) -> list[AlertEntry]:
    """Return de-duplicated alerts keyed by alert type."""

    unique: dict[str, AlertEntry] = {}
    for alert in alerts:
        if alert.alert_type not in unique:
            unique[alert.alert_type] = alert
    return list(unique.values())


def warning_impacts_status(warning: WarningEntry) -> bool:
    """Return whether one warning should affect acceptance status."""

    return warning.code not in PERFORMANCE_ALERT_WARNING_CODES


def warning_impacts_confidence(warning: WarningEntry) -> bool:
    """Return whether one warning should reduce confidence."""

    return warning.code not in PERFORMANCE_ALERT_WARNING_CODES
