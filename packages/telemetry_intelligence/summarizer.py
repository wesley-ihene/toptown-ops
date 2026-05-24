"""Summarize noisy runtime logs into compact daily telemetry intelligence."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence

from packages.common.paths import REPO_ROOT
from packages.record_store.writer import ensure_directory, write_json_file

from .schemas import HighRiskTarget, TelemetryEvent, VectorizeCandidate

SCHEMA_VERSION = "telemetry_intelligence.v1"
_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
_SENSITIVE_VALUE_RE = re.compile(
    r"(?i)\b(password|passwd|token|secret|credential|credentials|authorization|api[_-]?key)\b\s*[:=]\s*([^\s,;]+)"
)
_SENSITIVE_JSON_RE = re.compile(
    r'(?i)("(?:password|passwd|token|secret|credential|credentials|authorization|api[_-]?key)"\s*:\s*)"[^"]*"'
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[a-z0-9._\-]+=*")
_REASON_RE = re.compile(
    r"(?i)\b(?:reason|reason_code|duplicate_reason|validation_error_code|code)\b\s*[:=]\s*([a-z0-9_:-]+)"
)
_BRANCH_RE = re.compile(r"(?i)\bbranch(?:_hint)?\b\s*[:=]\s*([a-z0-9_-]+)")
_ITEM_RE = re.compile(r"(?i)\b(?:item|item_id|report_type|signal_type)\b\s*[:=]\s*([a-z0-9_-]+)")
_PRIORITY_RE = re.compile(r"(?i)\b(?:priority|risk)\b\s*[:=]\s*(high|critical)")
_IDENTIFIER_RE = re.compile(r"(?i)\b(message_id|raw_sha256|provider_message_id|sender_phone)\b\s*[:=]\s*([^\s,;]+)")
_WARNING_PREFIX_RE = re.compile(
    r"^\s*(?:\d{4}-\d{2}-\d{2}(?:[ t]\d{2}:\d{2}:\d{2}(?:,\d+|(?:\.\d+)?)?(?:z|[+-]\d{2}:\d{2})?)?\s+)?"
    r"(?:(?:debug|info|warning|error|critical)\s+)?"
)
_HEX_RE = re.compile(r"\b[a-f0-9]{12,}\b")
_NUMBER_RE = re.compile(r"\b\d+\b")
_ALLOWED_SINCE_RE = re.compile(r"^(\d+)d$")


@dataclass(slots=True)
class _Observation:
    event_type: str
    severity: str
    key: tuple[str, ...]
    branches: set[str] = field(default_factory=set)
    items: set[str] = field(default_factory=set)
    reason_codes: set[str] = field(default_factory=set)
    pattern: str | None = None
    sample: str | None = None


@dataclass(slots=True)
class _AggregateEvent:
    event_type: str
    severity: str
    key: tuple[str, ...]
    count: int = 0
    sources: set[str] = field(default_factory=set)
    branches: set[str] = field(default_factory=set)
    items: set[str] = field(default_factory=set)
    reason_codes: set[str] = field(default_factory=set)
    pattern: str | None = None
    sample: str | None = None

    def add(self, observation: _Observation, *, source_name: str) -> None:
        self.count += 1
        self.sources.add(source_name)
        self.branches.update(observation.branches)
        self.items.update(observation.items)
        self.reason_codes.update(observation.reason_codes)
        if self.pattern is None and observation.pattern is not None:
            self.pattern = observation.pattern
        if observation.sample is not None and (self.sample is None or observation.sample < self.sample):
            self.sample = observation.sample


def summarize_telemetry_for_date(
    report_date: str,
    *,
    repo_root: str | Path | None = None,
    log_paths: Sequence[Path] | None = None,
) -> dict[str, Any]:
    """Summarize one day of eligible logs and persist deterministic artifacts."""

    normalized_date = _normalized_report_date(report_date)
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    candidate_paths = sorted(log_paths or discover_log_paths(root))
    event_map: dict[tuple[str, ...], _AggregateEvent] = {}
    lines_scanned = 0
    lines_matched = 0

    for path in candidate_paths:
        source_name = _display_path(path, repo_root=root)
        for line in _iter_log_lines(path):
            line_date = _extract_line_date(line)
            if line_date != normalized_date:
                continue
            lines_scanned += 1
            observation = _classify_line(line)
            if observation is None:
                continue
            lines_matched += 1
            aggregate = event_map.get(observation.key)
            if aggregate is None:
                aggregate = _AggregateEvent(
                    event_type=observation.event_type,
                    severity=observation.severity,
                    key=observation.key,
                )
                event_map[observation.key] = aggregate
            aggregate.add(observation, source_name=source_name)

    events = _build_events(event_map)
    summary_payload = _build_summary_payload(
        report_date=normalized_date,
        source_files=[_display_path(path, repo_root=root) for path in candidate_paths],
        lines_scanned=lines_scanned,
        lines_matched=lines_matched,
        events=events,
    )
    events_payload = {
        "schema_version": SCHEMA_VERSION,
        "report_date": normalized_date,
        "source_files": summary_payload["source_files"],
        "lines_scanned": lines_scanned,
        "lines_matched": lines_matched,
        "events": [event.to_payload() for event in events],
    }

    output_dir = telemetry_output_dir(normalized_date, repo_root=root)
    ensure_directory(output_dir)
    events_path = write_json_file(output_dir / "telemetry_events.json", events_payload)
    summary_path = write_json_file(output_dir / "telemetry_summary.json", summary_payload)
    return {
        "report_date": normalized_date,
        "events_path": str(events_path),
        "summary_path": str(summary_path),
        "events_payload": events_payload,
        "summary_payload": summary_payload,
    }


def summarize_telemetry_for_dates(
    report_dates: Iterable[str],
    *,
    repo_root: str | Path | None = None,
    log_paths: Sequence[Path] | None = None,
) -> list[dict[str, Any]]:
    """Summarize multiple dates in stable ascending order."""

    results: list[dict[str, Any]] = []
    for report_date in sorted({_normalized_report_date(value) for value in report_dates}):
        results.append(
            summarize_telemetry_for_date(
                report_date,
                repo_root=repo_root,
                log_paths=log_paths,
            )
        )
    return results


def resolve_report_dates(
    *,
    report_date: str | None = None,
    since: str | None = None,
    today: date | None = None,
) -> list[str]:
    """Resolve either one explicit date or one inclusive trailing day window."""

    if bool(report_date) == bool(since):
        raise ValueError("Provide exactly one of --date or --since.")
    if report_date is not None:
        return [_normalized_report_date(report_date)]

    match = _ALLOWED_SINCE_RE.fullmatch(str(since or "").strip().lower())
    if match is None:
        raise ValueError("`--since` must use the form <days>d, for example `7d`.")
    days = int(match.group(1))
    if days <= 0:
        raise ValueError("`--since` must be greater than zero.")
    anchor = today or datetime.now().date()
    return [
        (anchor - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in reversed(range(days))
    ]


def discover_log_paths(repo_root: str | Path | None = None) -> list[Path]:
    """Return the phase-1 eligible input logs for telemetry intelligence."""

    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    paths = sorted((root / "LOGS").glob("*.log"))
    worker_log = root / "worker_decision_v2.log"
    if worker_log.exists():
        paths.append(worker_log)
    return sorted({path.resolve() for path in paths})


def telemetry_output_dir(report_date: str, *, repo_root: str | Path | None = None) -> Path:
    """Return the canonical daily telemetry output directory."""

    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    normalized_date = _normalized_report_date(report_date)
    return root / "records" / "telemetry" / "daily" / normalized_date


def _build_events(event_map: Mapping[tuple[str, ...], _AggregateEvent]) -> list[TelemetryEvent]:
    events: list[TelemetryEvent] = []
    for aggregate in sorted(event_map.values(), key=lambda value: value.key):
        if aggregate.event_type == "repeated_warning" and aggregate.count < 2:
            continue
        event_id = "|".join(aggregate.key)
        events.append(
            TelemetryEvent(
                event_id=event_id,
                event_type=aggregate.event_type,
                severity=aggregate.severity,
                count=aggregate.count,
                sources=sorted(aggregate.sources),
                branches=sorted(aggregate.branches),
                items=sorted(aggregate.items),
                reason_codes=sorted(aggregate.reason_codes),
                pattern=aggregate.pattern,
                sample=aggregate.sample,
            )
        )
    return events


def _build_summary_payload(
    *,
    report_date: str,
    source_files: list[str],
    lines_scanned: int,
    lines_matched: int,
    events: Sequence[TelemetryEvent],
) -> dict[str, Any]:
    counts_by_type = Counter()
    reason_code_counts = Counter()
    for event in events:
        counts_by_type[event.event_type] += event.count
        for reason_code in event.reason_codes:
            reason_code_counts[reason_code] += event.count

    high_risk_targets = _build_high_risk_targets(events)
    vectorize_candidates = _build_vectorize_candidates(events)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_date": report_date,
        "source_files": sorted(source_files),
        "source_file_count": len(source_files),
        "lines_scanned": lines_scanned,
        "lines_matched": lines_matched,
        "lines_ignored": max(lines_scanned - lines_matched, 0),
        "event_count": len(events),
        "occurrence_count": sum(event.count for event in events),
        "counts_by_type": dict(sorted(counts_by_type.items())),
        "reason_code_counts": dict(sorted(reason_code_counts.items())),
        "high_risk_targets": [target.to_payload() for target in high_risk_targets],
        "vectorize_candidates": [candidate.to_payload() for candidate in vectorize_candidates],
    }


def _build_high_risk_targets(events: Sequence[TelemetryEvent]) -> list[HighRiskTarget]:
    targets: dict[tuple[str, str | None], HighRiskTarget] = {}
    for event in events:
        if event.event_type != "high_risk_signal":
            continue
        branches = event.branches or ["unknown"]
        items = event.items or [None]
        for branch in branches:
            for item in items:
                key = (branch, item)
                current = targets.get(key)
                occurrences = (current.occurrences if current is not None else 0) + event.count
                event_ids = sorted(set((current.event_ids if current is not None else []) + [event.event_id]))
                targets[key] = HighRiskTarget(
                    branch=branch,
                    item=item,
                    occurrences=occurrences,
                    event_ids=event_ids,
                )
    return [targets[key] for key in sorted(targets)]


def _build_vectorize_candidates(events: Sequence[TelemetryEvent]) -> list[VectorizeCandidate]:
    candidates: list[VectorizeCandidate] = []
    for event in events:
        lesson = _lesson_for_event(event)
        if lesson is None:
            continue
        priority = "high" if event.severity in {"error", "critical"} or event.event_type in {
            "system_failure",
            "unresolved_report_conflict",
            "high_risk_signal",
        } else "medium"
        candidates.append(
            VectorizeCandidate(
                candidate_id=event.event_id,
                event_type=event.event_type,
                priority=priority,
                lesson=lesson,
            )
        )
    return sorted(candidates, key=lambda candidate: (candidate.priority, candidate.event_type, candidate.candidate_id))


def _lesson_for_event(event: TelemetryEvent) -> str | None:
    scope = _scope_text(event)
    if event.event_type == "duplicate_replay":
        reason = _joined(event.reason_codes) or "duplicate replay"
        return f"Duplicate replay pressure persisted for {scope} ({reason}, {event.count} occurrences). Tighten upstream dedupe before replay."
    if event.event_type == "guardrail_exclusion":
        reason = _joined(event.reason_codes) or "guardrail exclusion"
        return f"Guardrail exclusions recurred for {scope} ({reason}, {event.count} occurrences). Review suppression rules before downstream export."
    if event.event_type == "failed_validation":
        reason = _joined(event.reason_codes) or "uncoded validation failures"
        return f"Validation failures repeated for {scope} ({reason}, {event.count} occurrences). Promote the recurring reason codes into explicit operator diagnostics."
    if event.event_type == "reinforcement_decision":
        return f"Reinforcement decisions accumulated for {scope} ({event.count} occurrences). Verify the decision thresholds remain stable before promoting lessons."
    if event.event_type == "anomaly_event":
        reason = _joined(event.reason_codes) or "anomaly pattern"
        return f"Anomaly signals surfaced for {scope} ({reason}, {event.count} occurrences). Inspect drift before it becomes a workflow regression."
    if event.event_type == "system_failure":
        return f"System failures were logged for {scope} ({event.count} occurrences). Resolve the runtime fault before expanding automation coverage."
    if event.event_type == "high_risk_signal":
        return f"High-risk operational signals concentrated in {scope} ({event.count} occurrences). Keep human review ahead of any downstream automation."
    if event.event_type == "crash_restart_indicator":
        return f"Crash or restart indicators appeared for {scope} ({event.count} occurrences). Check process stability and startup loops."
    if event.event_type == "unresolved_report_conflict":
        return f"Unresolved report conflicts persisted for {scope} ({event.count} occurrences). Clear the scope collision before replay or export."
    if event.event_type == "repeated_warning":
        pattern = event.pattern or "warning pattern"
        return f"Repeated warning pattern detected for {scope} ({pattern}, {event.count} occurrences). Convert the noise into an explicit rule or remove it."
    return None


def _scope_text(event: TelemetryEvent) -> str:
    branch = _joined(event.branches)
    item = _joined(event.items)
    if branch and item:
        return f"{branch}/{item}"
    if branch:
        return branch
    if item:
        return item
    return "runtime telemetry"


def _joined(values: Sequence[str]) -> str | None:
    cleaned = [value for value in values if value]
    if not cleaned:
        return None
    return ",".join(cleaned[:3])


def _iter_log_lines(path: Path) -> Iterable[str]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            yield line.rstrip("\n")


def _classify_line(line: str) -> _Observation | None:
    payload = _extract_json_payload(line)
    lowered = line.casefold()
    sample = _sanitize_sample(line)
    branches = _extract_branches(line, payload)
    items = _extract_items(line, payload)
    reason_codes = _extract_reason_codes(line, payload)

    if _is_duplicate_replay(lowered, payload):
        reason = _first_or_unknown(reason_codes, default="duplicate")
        return _Observation(
            event_type="duplicate_replay",
            severity="warning",
            key=("duplicate_replay", reason, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes=reason_codes or {reason},
            sample=sample,
        )
    if _is_guardrail_exclusion(lowered, payload):
        reason = _first_or_unknown(reason_codes, default="guardrail_exclusion")
        return _Observation(
            event_type="guardrail_exclusion",
            severity="warning",
            key=("guardrail_exclusion", reason, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes=reason_codes or {reason},
            sample=sample,
        )
    if _is_failed_validation(lowered, payload, reason_codes):
        reason = _first_or_unknown(reason_codes, default="validation_failed")
        return _Observation(
            event_type="failed_validation",
            severity="warning",
            key=("failed_validation", reason, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes=reason_codes or {reason},
            sample=sample,
        )
    if _is_reinforcement_decision(lowered, payload):
        reason = _first_or_unknown(reason_codes, default="reinforcement")
        return _Observation(
            event_type="reinforcement_decision",
            severity="info",
            key=("reinforcement_decision", reason, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes=reason_codes,
            sample=sample,
        )
    if _is_anomaly_event(lowered, payload):
        reason = _first_or_unknown(reason_codes, default=_normalized_pattern(sample))
        return _Observation(
            event_type="anomaly_event",
            severity="warning",
            key=("anomaly_event", reason, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes=reason_codes or {reason},
            sample=sample,
        )
    if _is_unresolved_conflict(lowered, payload):
        reason = _first_or_unknown(reason_codes, default="conflict_blocked")
        return _Observation(
            event_type="unresolved_report_conflict",
            severity="error",
            key=("unresolved_report_conflict", reason, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes=reason_codes or {reason},
            sample=sample,
        )
    if _is_high_risk_signal(lowered, payload):
        reason = _first_or_unknown(reason_codes, default="high_risk")
        return _Observation(
            event_type="high_risk_signal",
            severity="error",
            key=("high_risk_signal", reason, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes=reason_codes or {reason},
            sample=sample,
        )
    if _is_crash_restart_indicator(lowered, payload):
        pattern = _crash_pattern(lowered)
        return _Observation(
            event_type="crash_restart_indicator",
            severity="warning",
            key=("crash_restart_indicator", pattern, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes={pattern},
            sample=sample,
        )
    if _is_system_failure(lowered, payload):
        reason = _first_or_unknown(reason_codes, default="system_failure")
        return _Observation(
            event_type="system_failure",
            severity="error",
            key=("system_failure", reason, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes=reason_codes or {reason},
            sample=sample,
        )
    if _is_warning_line(lowered, payload):
        pattern = _normalized_pattern(sample)
        return _Observation(
            event_type="repeated_warning",
            severity="warning",
            key=("repeated_warning", pattern, _first_or_unknown(branches), _first_or_unknown(items)),
            branches=branches,
            items=items,
            reason_codes=reason_codes,
            pattern=pattern,
            sample=sample,
        )
    return None


def _extract_json_payload(line: str) -> Mapping[str, Any]:
    stripped = line.strip()
    candidate = stripped
    if not candidate.startswith("{"):
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start < 0 or end <= start:
            return {}
        candidate = candidate[start : end + 1]
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_line_date(line: str) -> str | None:
    match = _DATE_RE.search(line)
    if match is not None:
        return match.group(1)
    return None


def _extract_reason_codes(line: str, payload: Mapping[str, Any]) -> set[str]:
    codes: set[str] = set()
    for key in ("reason", "reason_code", "duplicate_reason", "validation_error_code", "code"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            codes.add(_reason_token(value))
    for key in ("reasons", "warning_codes", "reason_codes"):
        value = payload.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    codes.add(_reason_token(item))
                elif isinstance(item, Mapping):
                    nested = item.get("reason_code") or item.get("code")
                    if isinstance(nested, str) and nested.strip():
                        codes.add(_reason_token(nested))
    for match in _REASON_RE.finditer(line):
        codes.add(_reason_token(match.group(1)))
    return {code for code in codes if code}


def _extract_branches(line: str, payload: Mapping[str, Any]) -> set[str]:
    branches: set[str] = set()
    for key in ("branch", "branch_hint", "group_name"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            branches.add(_slug_token(value))
    match = _BRANCH_RE.search(line)
    if match is not None:
        branches.add(_slug_token(match.group(1)))
    return {branch for branch in branches if branch}


def _extract_items(line: str, payload: Mapping[str, Any]) -> set[str]:
    items: set[str] = set()
    for key in ("item", "item_id", "report_type", "signal_type"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            items.add(_slug_token(value))
    match = _ITEM_RE.search(line)
    if match is not None:
        items.add(_slug_token(match.group(1)))
    return {item for item in items if item}


def _reason_token(value: str) -> str:
    return _slug_token(value)


def _slug_token(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_-]+", "_", value.strip().casefold()).strip("_")
    return cleaned or "unknown"


def _sanitize_sample(line: str) -> str:
    sample = line.strip()
    sample = _SENSITIVE_VALUE_RE.sub(lambda match: f"{match.group(1)}=[REDACTED]", sample)
    sample = _SENSITIVE_JSON_RE.sub(lambda match: f'{match.group(1)}"[REDACTED]"', sample)
    sample = _BEARER_RE.sub("Bearer [REDACTED]", sample)
    sample = _IDENTIFIER_RE.sub(lambda match: f"{match.group(1)}=[REDACTED]", sample)
    sample = re.sub(r"\s+", " ", sample)
    sample = _WARNING_PREFIX_RE.sub("", sample)
    if len(sample) > 250:
        return f"{sample[:247].rstrip()}..."
    return sample


def _normalized_pattern(sample: str) -> str:
    pattern = _WARNING_PREFIX_RE.sub("", sample.casefold())
    pattern = _DATE_RE.sub("<date>", pattern)
    pattern = _HEX_RE.sub("<hex>", pattern)
    pattern = re.sub(r"\bmessage_id\b\s*[:=]\s*[^ ,;]+", "message_id=<id>", pattern)
    pattern = re.sub(r"\braw_sha256\b\s*[:=]\s*[^ ,;]+", "raw_sha256=<hash>", pattern)
    pattern = _NUMBER_RE.sub("<n>", pattern)
    pattern = re.sub(r"\s+", " ", pattern).strip()
    return pattern[:120] if len(pattern) > 120 else pattern


def _is_duplicate_replay(lowered: str, payload: Mapping[str, Any]) -> bool:
    event = str(payload.get("event") or "").casefold()
    return (
        "duplicate live whatsapp message skipped" in lowered
        or ("duplicate" in lowered and ("replay" in lowered or "skipped" in lowered))
        or event.startswith("duplicate")
        or "duplicate_reason" in lowered
    )


def _is_guardrail_exclusion(lowered: str, payload: Mapping[str, Any]) -> bool:
    event = str(payload.get("event") or "").casefold()
    if "guardrail" in event and any(token in event for token in ("exclude", "excluded", "suppress", "skip", "blocked")):
        return True
    return "guardrail" in lowered and any(
        token in lowered for token in ("exclude", "excluded", "suppress", "skipped", "blocked")
    )


def _is_failed_validation(lowered: str, payload: Mapping[str, Any], reason_codes: set[str]) -> bool:
    event = str(payload.get("event") or "").casefold()
    if "validation" in event and any(token in event for token in ("failed", "rejected", "error")):
        return True
    if "validation_error_code" in lowered or "fallback_validation_failed" in lowered:
        return True
    if "validation failed" in lowered:
        return True
    return any(code.endswith("mismatch") or code.endswith("failed") or code.endswith("review") for code in reason_codes)


def _is_reinforcement_decision(lowered: str, payload: Mapping[str, Any]) -> bool:
    event = str(payload.get("event") or "").casefold()
    return "reinforcement" in lowered or "decay" in lowered or "reinforcement" in event or "decay" in event


def _is_anomaly_event(lowered: str, payload: Mapping[str, Any]) -> bool:
    event = str(payload.get("event") or "").casefold()
    anomaly_tokens = ("anomaly", "anomalous", "outlier", "drift_detected", "unexpected_acceptance", "unexpected_rejection")
    return any(token in lowered for token in anomaly_tokens) or any(token in event for token in anomaly_tokens)


def _is_system_failure(lowered: str, payload: Mapping[str, Any]) -> bool:
    event = str(payload.get("event") or "").casefold()
    return (
        "traceback" in lowered
        or "exception" in lowered
        or "critical" in lowered
        or " adapter failure" in lowered
        or lowered.startswith("error ")
        or '"level":"error"' in lowered
        or event.endswith("failed")
    )


def _is_high_risk_signal(lowered: str, payload: Mapping[str, Any]) -> bool:
    priority = str(payload.get("priority") or "").casefold()
    if priority in {"high", "critical"}:
        return True
    return "high-risk" in lowered or "high risk" in lowered or _PRIORITY_RE.search(lowered) is not None


def _is_crash_restart_indicator(lowered: str, payload: Mapping[str, Any]) -> bool:
    event = str(payload.get("event") or "").casefold()
    tokens = ("shutting down", "listening on", "restart", "restarting", "crash", "crashed")
    return any(token in lowered for token in tokens) or any(token in event for token in tokens)


def _crash_pattern(lowered: str) -> str:
    if "listening on" in lowered:
        return "startup"
    if "shutting down" in lowered:
        return "shutdown"
    if "restart" in lowered or "restarting" in lowered:
        return "restart"
    return "crash"


def _is_unresolved_conflict(lowered: str, payload: Mapping[str, Any]) -> bool:
    event = str(payload.get("event") or "").casefold()
    tokens = ("conflict_blocked", "conflicting_record_same_scope", "normalized_conflict", "unresolved report conflict")
    return any(token in lowered for token in tokens) or any(token in event for token in tokens)


def _is_warning_line(lowered: str, payload: Mapping[str, Any]) -> bool:
    level = str(payload.get("level") or "").casefold()
    if level == "warning":
        return True
    return " warning " in f" {lowered} " or lowered.startswith("warning ")


def _first_or_unknown(values: set[str], *, default: str = "unknown") -> str:
    if not values:
        return default
    return sorted(values)[0]


def _display_path(path: Path, *, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return str(path.resolve())


def _normalized_report_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
