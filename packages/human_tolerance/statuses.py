"""Attendance-status tolerance helpers for noisy WhatsApp input."""

from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata

_CHECKMARK_PATTERN = re.compile(r"[✔✅☑]")
_KEY_VALUE_PATTERN = re.compile(r"^\s*(?P<left>[^:=]+?)\s*(?P<sep>[:=])\s*(?P<right>.+?)\s*$")
_RECORD_PATTERN = re.compile(r"^\s*(?P<prefix>\d+\s*[.)-]?\s*.+?)\s*(?P<sep>=|--+|-|:|\||/)\s*(?P<status>.+?)\s*$")
_STATUS_ALIASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("AWON", ("absent without notice", "absent without", "awon")),
    ("AWN", ("absent with notice", "awn")),
    ("LAY_OFF", ("lay off", "layoff")),
    ("SUSPEND", ("suspended", "suspend")),
    ("TRANSFER", ("transfer",)),
    ("LEAVE", ("leave break", "on leave", "annual leave", "leavebreak", "leave")),
    ("OFF", ("day off", "off duty", "off")),
    ("SICK", ("sick",)),
    ("LATE", ("late",)),
    ("ABSENT", ("absent",)),
    ("P", ("press", "press/", "present", "p")),
    ("NIL", ("nill", "nil")),
)
_SUMMARY_LABEL_REPLACEMENTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bpress/?\b", flags=re.IGNORECASE), "Present"),
    (re.compile(r"\bon\s+leavebreak\b", flags=re.IGNORECASE), "On Leave"),
    (re.compile(r"\bleavebreak\b", flags=re.IGNORECASE), "Leave"),
    (re.compile(r"\babsent\s+without\b", flags=re.IGNORECASE), "Absent Without Notice"),
    (re.compile(r"\babsent\s+with\s+notice\b", flags=re.IGNORECASE), "Absent With Notice"),
    (re.compile(r"\blay\s*off\b", flags=re.IGNORECASE), "Lay Off"),
)


@dataclass(slots=True, frozen=True)
class StatusNormalization:
    """One applied status normalization for attendance input."""

    raw_value: str
    normalized_value: str
    line_index: int


def normalize_status_token(raw_value: str) -> str | None:
    """Return one canonical attendance token when recognized."""

    if _CHECKMARK_PATTERN.search(raw_value):
        return "P"

    normalized = _normalize_text(raw_value)
    if not normalized:
        return None

    for canonical, aliases in _STATUS_ALIASES:
        for alias in aliases:
            alias_text = _normalize_text(alias)
            if normalized == alias_text:
                return canonical
    return None


def looks_like_attendance_row(line: str) -> bool:
    """Return whether one line looks like a numbered attendance row."""

    if _CHECKMARK_PATTERN.search(line):
        return True
    match = _RECORD_PATTERN.match(line.strip())
    if match is None:
        return False
    return normalize_status_token(match.group("status")) is not None


def normalize_attendance_line(line: str, *, line_index: int) -> tuple[str, list[StatusNormalization]]:
    """Return one attendance-aware normalized line plus applied status corrections."""

    normalized_line = unicodedata.normalize("NFKC", line)
    changes: list[StatusNormalization] = []

    if _CHECKMARK_PATTERN.search(normalized_line):
        replaced = _CHECKMARK_PATTERN.sub("P", normalized_line)
        if replaced != normalized_line:
            changes.append(StatusNormalization(raw_value=line.strip(), normalized_value=replaced.strip(), line_index=line_index))
            normalized_line = replaced

    kv_match = _KEY_VALUE_PATTERN.match(normalized_line)
    if kv_match is not None:
        left = _normalize_summary_label(kv_match.group("left").strip())
        right = kv_match.group("right").strip()
        normalized_right = normalize_status_token(right)
        left_changed = left != kv_match.group("left").strip()
        if normalized_right is not None:
            normalized_candidate = f"{left} {kv_match.group('sep')} {normalized_right}"
            if normalized_candidate != normalized_line:
                changes.append(
                    StatusNormalization(
                        raw_value=normalized_line.strip(),
                        normalized_value=normalized_candidate.strip(),
                        line_index=line_index,
                    )
                )
                return normalized_candidate, changes
        if left_changed:
            normalized_candidate = f"{left}{kv_match.group('sep')} {right}"
            changes.append(
                StatusNormalization(
                    raw_value=normalized_line.strip(),
                    normalized_value=normalized_candidate.strip(),
                    line_index=line_index,
                )
            )
            normalized_line = normalized_candidate

    record_match = _RECORD_PATTERN.match(normalized_line.strip())
    if record_match is None:
        return normalized_line, changes

    normalized_status = normalize_status_token(record_match.group("status"))
    if normalized_status is None:
        return normalized_line, changes

    prefix = record_match.group("prefix").strip()
    separator = record_match.group("sep")
    normalized_candidate = f"{prefix} {separator} {normalized_status}"
    if normalized_candidate != normalized_line.strip():
        changes.append(
            StatusNormalization(
                raw_value=normalized_line.strip(),
                normalized_value=normalized_candidate,
                line_index=line_index,
            )
        )
    return normalized_candidate, changes


def _normalize_summary_label(value: str) -> str:
    normalized = value
    for pattern, replacement in _SUMMARY_LABEL_REPLACEMENTS:
        normalized = pattern.sub(replacement, normalized)
    return " ".join(normalized.split())


def _normalize_text(value: str) -> str:
    folded = unicodedata.normalize("NFKC", value).casefold()
    folded = folded.replace("_", " ").replace("/", " ")
    folded = re.sub(r"[^a-z0-9]+", " ", folded)
    return " ".join(folded.split())
