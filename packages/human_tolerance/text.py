"""Human-tolerant normalization entry points for WhatsApp intake."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import re
import unicodedata
from typing import Any

from packages.normalization.branches import CANONICAL_BRANCHES

from .branches import BranchDetection, detect_branch
from .dates import DateDetection, detect_report_date
from .report_titles import TitleDetection, detect_attendance_title
from .statuses import StatusNormalization, normalize_attendance_line

_MULTISPACE_PATTERN = re.compile(r"[ \t]+")
_LEADING_ZERO_WIDTH_PATTERN = re.compile(r"[\u200b\u200c\u200d\ufeff]")
_DASH_TRANSLATION = str.maketrans(
    {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
    }
)


@dataclass(slots=True)
class HumanToleranceResult:
    """Normalized intake text plus stable provenance metadata."""

    normalized_text: str
    applied: bool
    original_text_hash: str
    normalized_text_hash: str
    normalized_fields: dict[str, Any] = field(default_factory=dict)
    corrections: list[dict[str, Any]] = field(default_factory=list)
    report_type_hint: str | None = None

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe metadata payload without embedding full text."""

        return {
            "human_tolerance_applied": self.applied,
            "applied": self.applied,
            "original_text_hash": self.original_text_hash,
            "normalized_text_hash": self.normalized_text_hash,
            "normalized_fields": dict(self.normalized_fields),
            "corrections": [dict(item) for item in self.corrections],
            "report_type_hint": self.report_type_hint,
        }


def normalize_human_whatsapp_text(text: str) -> str:
    """Return parser-ready text for common human WhatsApp variations."""

    return analyze_human_whatsapp_text(text).normalized_text


def analyze_human_whatsapp_text(text: str) -> HumanToleranceResult:
    """Return human-tolerant normalization plus explicit provenance."""

    original_text = text if isinstance(text, str) else ""
    basic_lines = [_normalize_basic_line(line) for line in _normalize_line_endings(original_text).split("\n")]
    lines = _collapse_blank_lines(basic_lines)
    initial_title = detect_attendance_title(lines)

    status_corrections: list[StatusNormalization] = []
    if initial_title is not None:
        normalized_lines: list[str] = []
        for index, line in enumerate(lines):
            normalized_line, changes = normalize_attendance_line(line, line_index=index)
            normalized_lines.append(normalized_line)
            status_corrections.extend(changes)
        lines = normalized_lines

    title = detect_attendance_title(lines)
    attendance_like = title is not None

    corrections: list[dict[str, Any]] = []
    normalized_fields: dict[str, Any] = {}
    if status_corrections:
        normalized_fields["statuses"] = sorted(
            {
                correction.normalized_value.split()[-1]
                for correction in status_corrections
                if correction.normalized_value.split()
            }
        )
        corrections.extend(
            {
                "field": "statuses",
                "raw_value": correction.raw_value,
                "normalized_value": correction.normalized_value,
                "line_index": correction.line_index,
            }
            for correction in status_corrections[:10]
        )

    if attendance_like:
        lines, title_payload = _apply_title_normalization(lines, title)
        if title_payload is not None:
            normalized_fields["report_title"] = "ATTENDANCE REPORT"
            corrections.append(title_payload)
        elif title is not None:
            normalized_fields["report_title"] = "ATTENDANCE REPORT"

        branch = detect_branch(lines)
        lines, branch_payload = _apply_branch_normalization(lines, branch)
        if branch_payload is not None:
            normalized_fields["branch"] = branch_payload["normalized_value"]
            corrections.append(branch_payload)
        elif branch is not None:
            normalized_fields["branch"] = branch.slug

        report_date = detect_report_date(lines)
        lines, date_payload = _apply_date_normalization(lines, report_date)
        if date_payload is not None:
            normalized_fields["date"] = report_date.iso_date if report_date is not None else None
            corrections.append(date_payload)
        elif report_date is not None:
            normalized_fields["date"] = report_date.iso_date

    normalized_text = "\n".join(_trim_edge_blank_lines(lines))
    normalized_text = normalized_text.strip()
    if not normalized_text:
        normalized_text = original_text.strip()

    original_hash = hashlib.sha256(original_text.encode("utf-8")).hexdigest()
    normalized_hash = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()
    applied = normalized_text != original_text.strip()

    return HumanToleranceResult(
        normalized_text=normalized_text,
        applied=applied,
        original_text_hash=original_hash,
        normalized_text_hash=normalized_hash,
        normalized_fields={key: value for key, value in normalized_fields.items() if value},
        corrections=corrections,
        report_type_hint="staff_attendance" if attendance_like else None,
    )


def _apply_branch_normalization(
    lines: list[str],
    branch: BranchDetection | None,
) -> tuple[list[str], dict[str, Any] | None]:
    if branch is None:
        return lines, None

    canonical_line = f"Branch: {branch.slug}"
    display_name = CANONICAL_BRANCHES.get(branch.slug, branch.display_name)
    payload = {
        "field": "branch",
        "raw_value": branch.raw_value,
        "normalized_value": branch.slug,
        "display_value": display_name,
        "source": "explicit_branch_line" if branch.explicit_line else "header",
        "message": f"Branch detected from header: {display_name}",
    }
    updated = list(lines)
    if branch.explicit_line:
        if updated[branch.line_index].strip() == canonical_line:
            return updated, None
        updated[branch.line_index] = canonical_line
        return updated, payload

    if any(line.strip().casefold().startswith("branch:") for line in updated):
        return updated, None
    insertion_index = _insertion_index_after_headers(updated)
    updated.insert(insertion_index, canonical_line)
    return updated, payload


def _apply_date_normalization(
    lines: list[str],
    report_date: DateDetection | None,
) -> tuple[list[str], dict[str, Any] | None]:
    if report_date is None:
        return lines, None

    canonical_line = f"Date: {report_date.canonical_date}"
    payload = {
        "field": "date",
        "raw_value": report_date.raw_value,
        "normalized_value": report_date.iso_date,
        "display_value": report_date.canonical_date,
        "source": "explicit_date_line" if report_date.explicit_line else "header",
        "message": f'Date detected from "{report_date.raw_value}"',
    }
    updated = list(lines)
    if report_date.explicit_line:
        if report_date.already_canonical:
            return updated, None
        updated[report_date.line_index] = canonical_line
        return updated, payload

    if any(line.strip().casefold().startswith("date:") for line in updated):
        return updated, None
    insertion_index = _insertion_index_after_headers(updated)
    updated.insert(insertion_index, canonical_line)
    return updated, payload


def _apply_title_normalization(
    lines: list[str],
    title: TitleDetection | None,
) -> tuple[list[str], dict[str, Any] | None]:
    if title is None:
        return lines, None

    payload = {
        "field": "report_title",
        "raw_value": title.raw_value,
        "normalized_value": "ATTENDANCE REPORT",
        "display_value": "ATTENDANCE REPORT",
        "source": "explicit_title_line" if title.explicit_line else "attendance_structure",
        "message": (
            f"Report title normalized: {title.raw_value} -> ATTENDANCE REPORT"
            if title.explicit_line
            else "Report title normalized from attendance structure: ATTENDANCE REPORT"
        ),
    }
    updated = list(lines)
    if title.explicit_line and title.line_index is not None:
        if updated[title.line_index].strip() == "ATTENDANCE REPORT":
            return updated, None
        updated[title.line_index] = "ATTENDANCE REPORT"
        return updated, payload

    if any(line.strip().casefold() == "attendance report" for line in updated):
        return updated, None
    insertion_index = _insertion_index_after_headers(updated)
    updated.insert(insertion_index, "ATTENDANCE REPORT")
    return updated, payload


def _normalize_line_endings(value: str) -> str:
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _normalize_basic_line(line: str) -> str:
    normalized = unicodedata.normalize("NFKC", line)
    normalized = _LEADING_ZERO_WIDTH_PATTERN.sub("", normalized)
    normalized = normalized.translate(_DASH_TRANSLATION)
    normalized = normalized.replace("\t", " ")
    normalized = _MULTISPACE_PATTERN.sub(" ", normalized)
    return normalized.strip()


def _collapse_blank_lines(lines: list[str]) -> list[str]:
    collapsed: list[str] = []
    previous_blank = False
    for line in lines:
        is_blank = not line.strip()
        if is_blank and previous_blank:
            continue
        collapsed.append(line)
        previous_blank = is_blank
    return collapsed


def _trim_edge_blank_lines(lines: list[str]) -> list[str]:
    trimmed = list(lines)
    while trimmed and not trimmed[0].strip():
        trimmed.pop(0)
    while trimmed and not trimmed[-1].strip():
        trimmed.pop()
    return trimmed


def _insertion_index_after_headers(lines: list[str]) -> int:
    for index, line in enumerate(lines):
        if line.strip():
            continue
        return index
    return 0
