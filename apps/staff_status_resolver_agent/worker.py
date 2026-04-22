"""Resolve staff duty and grade annotations from free-form text."""

from __future__ import annotations

from dataclasses import dataclass, field
import re

_GRADE_PATTERN = re.compile(r"\b([1-5])\b")
_ROLE_PATTERN = re.compile(r"\(([^)]+)\)")
_SHORT_ROLE_PATTERN = re.compile(
    r"\b(cashier|pricing room|price room|pricing clerk|pricing cluck|supervisor|door man|doorman)\b",
    flags=re.IGNORECASE,
)


@dataclass(slots=True)
class StaffStatusResolution:
    """Resolved status tokens from a performance row."""

    duty_status: str = "on_duty"
    performance_grade: int | None = None
    role_annotation: str | None = None
    vacancy_marker: bool = False
    raw_tokens: list[str] = field(default_factory=list)


def resolve_staff_status(*values: str | None) -> StaffStatusResolution:
    """Resolve duty, grade, role, and vacancy markers from text fragments."""

    resolution = StaffStatusResolution()
    for value in values:
        if not value:
            continue
        resolution.raw_tokens.append(value)
        lowered = value.casefold()
        if "vacant" in lowered:
            resolution.vacancy_marker = True
        if "off" in lowered:
            resolution.duty_status = "off_duty"
        elif "leave" in lowered:
            resolution.duty_status = "leave"
        elif "sent home" in lowered:
            resolution.duty_status = "sent_home"
        elif "absent" in lowered or "awn" in lowered or "without notice" in lowered:
            resolution.duty_status = "absent"
        elif "sick" in lowered:
            resolution.duty_status = "sick"
        elif "transfer" in lowered or "trenfer" in lowered:
            resolution.duty_status = "transferred"
        grade_match = _GRADE_PATTERN.search(value)
        if grade_match is not None:
            resolution.performance_grade = int(grade_match.group(1))
        for role_match in _ROLE_PATTERN.findall(value):
            cleaned_role = role_match.strip(" -")
            if cleaned_role and cleaned_role.casefold() != "vacant":
                resolution.role_annotation = cleaned_role
        short_role_match = _SHORT_ROLE_PATTERN.search(value)
        if short_role_match is not None and resolution.role_annotation is None:
            resolution.role_annotation = short_role_match.group(1).strip()
    return resolution
