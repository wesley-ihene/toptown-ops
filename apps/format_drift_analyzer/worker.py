"""Deterministic format-drift summaries for raw WhatsApp inputs."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import re
from pathlib import Path
from typing import Any

from packages.learning_store import write_format_drift
import packages.record_store.paths as record_paths
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem

AGENT_NAME = "format_drift_analyzer"
SIGNAL_TYPE = "format_drift"

_LINE_WITH_COLON_RE = re.compile(r"^\s*([^:\n]{2,80}):\s*(.+?)\s*$")
_LINE_WITH_EQUALS_RE = re.compile(r"^\s*([^=\n]{2,80})=\s*(.+?)\s*$")
_LINE_WITH_DASH_RE = re.compile(r"^\s*([A-Za-z][A-Za-z /_-]{1,80})\s*-\s*([0-9Kk].*?)\s*$")
_LABEL_CANONICAL_RE = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class FormatDriftAnalyzerWorker:
    """Generate one daily format-drift summary artifact."""

    agent_name: str = AGENT_NAME

    def process(self, work_item: WorkItem) -> AgentResult:
        return process_work_item(work_item)


def process_work_item(work_item: WorkItem) -> AgentResult:
    payload = work_item.payload if isinstance(work_item.payload, dict) else {}
    report_date = payload.get("report_date") or payload.get("date")
    output_root = payload.get("root") or payload.get("output_root")
    generated_at = payload.get("generated_at")
    window_days = payload.get("window_days", 7)

    if not isinstance(report_date, str) or not report_date.strip():
        return _failure_result("report_date is required for format drift analysis.")
    if not isinstance(window_days, int) or isinstance(window_days, bool) or window_days <= 0:
        return _failure_result("window_days must be a positive integer.")

    result = analyze_format_drift(
        report_date.strip(),
        window_days=window_days,
        output_root=_path_or_none(output_root),
        generated_at=generated_at if isinstance(generated_at, str) else None,
    )
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "report_date": report_date.strip(),
            "status": "written",
            "output_path": result["output_path"],
            "format_drift": result["summary"],
        },
    )


def analyze_format_drift(
    report_date: str,
    *,
    window_days: int = 7,
    output_root: str | Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Analyze raw WhatsApp formatting drift without changing parser behavior."""

    normalized_report_date = _normalize_date(report_date)
    normalized_window_days = _normalized_window_days(window_days)
    start_date = _window_start_date(normalized_report_date, normalized_window_days)

    review_links = _load_review_links(start_date=start_date, end_date=normalized_report_date, output_root=output_root)
    rejected_links = _load_rejected_links(start_date=start_date, end_date=normalized_report_date, output_root=output_root)
    raw_records = _load_raw_records(start_date=start_date, end_date=normalized_report_date, output_root=output_root)
    summary_payload = _build_summary_payload(
        report_date=normalized_report_date,
        start_date=start_date,
        window_days=normalized_window_days,
        raw_records=raw_records,
        review_links=review_links,
        rejected_links=rejected_links,
    )
    output_path = write_format_drift(
        normalized_report_date,
        summary_payload,
        generated_at=generated_at or f"{normalized_report_date}T00:00:00Z",
        source_paths=_source_paths(raw_records, review_links, rejected_links),
        analysis_window={
            "start_date": start_date,
            "end_date": normalized_report_date,
            "window_days": normalized_window_days,
        },
        output_root=output_root,
    )
    return {
        "status": "written",
        "output_path": output_path,
        "summary": json.loads(Path(output_path).read_text(encoding="utf-8")),
    }


def _build_summary_payload(
    *,
    report_date: str,
    start_date: str,
    window_days: int,
    raw_records: list[dict[str, Any]],
    review_links: dict[str, dict[str, Any]],
    rejected_links: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    issue_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    report_type_issue_counts: dict[str, Counter[str]] = defaultdict(Counter)
    branch_issue_counts: dict[str, Counter[str]] = defaultdict(Counter)
    report_type_raw_counts = Counter(record["report_type"] for record in raw_records)
    branch_raw_counts = Counter(record["branch"] for record in raw_records)
    report_type_branch_counts: dict[str, Counter[str]] = defaultdict(Counter)
    branch_report_type_counts: dict[str, Counter[str]] = defaultdict(Counter)
    label_observations: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for record in raw_records:
        report_type_branch_counts[record["report_type"]][record["branch"]] += 1
        branch_report_type_counts[record["branch"]][record["report_type"]] += 1

        linked_review = review_links.get(record["path"], {})
        linked_rejected = rejected_links.get(record["path"], {})

        for issue in _line_level_issues(record, linked_review, linked_rejected):
            issue_buckets[issue["issue_code"]].append(issue)
            report_type_issue_counts[issue["report_type"]][issue["issue_code"]] += 1
            branch_issue_counts[issue["branch"]][issue["issue_code"]] += 1

        for label in record["labels"]:
            label_observations[label["canonical"]].append(
                {
                    "record": record,
                    "variant": label["variant"],
                    "case_style": label["case_style"],
                    "review_reasons": list(linked_review.get("review_reasons", [])),
                    "rejection_reasons": list(linked_rejected.get("rejection_reasons", [])),
                }
            )

    for canonical_label, observations in sorted(label_observations.items()):
        variants = {item["variant"] for item in observations}
        case_styles = {item["case_style"] for item in observations}
        if len(variants) > 1:
            aggregate = _aggregate_label_issue(
                issue_code="label_spelling_variant",
                observations=observations,
                canonical_label=canonical_label,
            )
            issue_buckets[aggregate["issue_code"]].append(aggregate)
            for report_type in aggregate["report_types"]:
                report_type_issue_counts[report_type][aggregate["issue_code"]] += aggregate["count"]
            for branch in aggregate["branches"]:
                branch_issue_counts[branch][aggregate["issue_code"]] += aggregate["count"]
        if len(case_styles) > 1:
            aggregate = _aggregate_label_issue(
                issue_code="casing_drift",
                observations=observations,
                canonical_label=canonical_label,
            )
            issue_buckets[aggregate["issue_code"]].append(aggregate)
            for report_type in aggregate["report_types"]:
                report_type_issue_counts[report_type][aggregate["issue_code"]] += aggregate["count"]
            for branch in aggregate["branches"]:
                branch_issue_counts[branch][aggregate["issue_code"]] += aggregate["count"]

    frequent_format_issues = _frequent_issue_rows(issue_buckets)
    template_improvement_candidates = _template_candidates(frequent_format_issues)
    branch_training_notes = _branch_training_notes(branch_issue_counts, branch_report_type_counts, review_links, rejected_links)

    return {
        "artifact_type": SIGNAL_TYPE,
        "report_date": report_date,
        "total_raw_records": len(raw_records),
        "total_review_records": len(review_links),
        "total_rejection_records": len(rejected_links),
        "frequent_format_issues": frequent_format_issues,
        "template_improvement_candidates": template_improvement_candidates,
        "branch_training_notes": branch_training_notes,
        "report_family_summaries": [
            {
                "report_type": report_type,
                "raw_records": report_type_raw_counts[report_type],
                "issue_counts": _counter_rows("issue_code", report_type_issue_counts[report_type]),
                "branch_counts": _counter_rows("branch", report_type_branch_counts[report_type]),
            }
            for report_type in sorted(report_type_raw_counts)
        ],
        "branch_summaries": [
            {
                "branch": branch,
                "raw_records": branch_raw_counts[branch],
                "issue_counts": _counter_rows("issue_code", branch_issue_counts[branch]),
                "report_type_counts": _counter_rows("report_type", branch_report_type_counts[branch]),
            }
            for branch in sorted(branch_raw_counts)
        ],
        "analysis_scope": {
            "start_date": start_date,
            "end_date": report_date,
            "window_days": window_days,
        },
    }


def _line_level_issues(
    record: Mapping[str, Any],
    linked_review: Mapping[str, Any],
    linked_rejected: Mapping[str, Any],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    total_styles: set[str] = set()
    for line in record["lines"]:
        stripped = line.strip()
        if not stripped:
            continue
        match_equals = _LINE_WITH_EQUALS_RE.match(stripped)
        if match_equals:
            issues.append(_issue_observation("punctuation_equals_separator", record, stripped, linked_review, linked_rejected))
        match_dash = _LINE_WITH_DASH_RE.match(stripped)
        if match_dash:
            issues.append(_issue_observation("punctuation_dash_separator", record, stripped, linked_review, linked_rejected))
        if "total" in stripped.lower():
            total_styles.add(_totals_style(stripped))
    if any(style != "colon" for style in total_styles):
        issues.append(
            _issue_observation(
                "inconsistent_totals_syntax",
                record,
                next((line.strip() for line in record["lines"] if "total" in line.lower()), ""),
                linked_review,
                linked_rejected,
            )
        )
    return issues


def _aggregate_label_issue(
    *,
    issue_code: str,
    observations: list[dict[str, Any]],
    canonical_label: str,
) -> dict[str, Any]:
    report_types = sorted({item["record"]["report_type"] for item in observations})
    branches = sorted({item["record"]["branch"] for item in observations})
    review_reasons = Counter(
        reason
        for item in observations
        for reason in item.get("review_reasons", [])
    )
    rejection_reasons = Counter(
        reason
        for item in observations
        for reason in item.get("rejection_reasons", [])
    )
    variants = sorted({item["variant"] for item in observations})
    paths = sorted({item["record"]["path"] for item in observations})
    return {
        "issue_code": issue_code,
        "count": len(paths),
        "report_types": report_types,
        "branches": branches,
        "examples": variants[:3],
        "related_review_reasons": _counter_rows("reason", review_reasons),
        "related_rejection_reasons": _counter_rows("reason", rejection_reasons),
        "affected_record_paths": paths,
        "canonical_label": canonical_label,
    }


def _issue_observation(
    issue_code: str,
    record: Mapping[str, Any],
    example: str,
    linked_review: Mapping[str, Any],
    linked_rejected: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "issue_code": issue_code,
        "count": 1,
        "report_type": record["report_type"],
        "branch": record["branch"],
        "report_types": [record["report_type"]],
        "branches": [record["branch"]],
        "examples": [example],
        "related_review_reasons": _counter_rows("reason", Counter(linked_review.get("review_reasons", []))),
        "related_rejection_reasons": _counter_rows("reason", Counter(linked_rejected.get("rejection_reasons", []))),
        "affected_record_paths": [record["path"]],
    }


def _frequent_issue_rows(issue_buckets: Mapping[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for issue_code, entries in sorted(issue_buckets.items()):
        affected_paths = sorted({path for entry in entries for path in entry["affected_record_paths"]})
        if len(affected_paths) < 2:
            continue
        review_reason_counts = Counter(
            row["reason"]
            for entry in entries
            for row in entry["related_review_reasons"]
            for _ in range(int(row["count"]))
        )
        rejection_reason_counts = Counter(
            row["reason"]
            for entry in entries
            for row in entry["related_rejection_reasons"]
            for _ in range(int(row["count"]))
        )
        rows.append(
            {
                "issue_code": issue_code,
                "count": len(affected_paths),
                "report_types": sorted({report_type for entry in entries for report_type in entry["report_types"]}),
                "branches": sorted({branch for entry in entries for branch in entry["branches"]}),
                "examples": sorted({example for entry in entries for example in entry["examples"]})[:3],
                "related_review_reasons": _counter_rows("reason", review_reason_counts),
                "related_rejection_reasons": _counter_rows("reason", rejection_reason_counts),
            }
        )
    return sorted(rows, key=lambda row: (-row["count"], row["issue_code"]))


def _template_candidates(frequent_issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for issue in frequent_issues:
        if len(issue["report_types"]) != 1:
            continue
        report_type = issue["report_types"][0]
        candidates.append(
            {
                "candidate_id": f"{report_type}__{issue['issue_code']}",
                "report_type": report_type,
                "issue_code": issue["issue_code"],
                "reason": _candidate_reason(issue["issue_code"]),
                "evidence": {
                    "issue_count": issue["count"],
                    "branches": list(issue["branches"]),
                },
                "proposed_template_note": _template_note(issue["issue_code"]),
            }
        )
    return sorted(candidates, key=lambda row: (row["report_type"], row["issue_code"]))


def _branch_training_notes(
    branch_issue_counts: Mapping[str, Counter[str]],
    branch_report_type_counts: Mapping[str, Counter[str]],
    review_links: Mapping[str, dict[str, Any]],
    rejected_links: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    review_by_branch = Counter(
        item["branch"]
        for item in review_links.values()
        if isinstance(item.get("branch"), str)
    )
    rejected_by_branch = Counter(
        item["branch"]
        for item in rejected_links.values()
        if isinstance(item.get("branch"), str)
    )
    rows: list[dict[str, Any]] = []
    for branch in sorted(branch_issue_counts):
        total_issues = sum(branch_issue_counts[branch].values())
        if total_issues < 2:
            continue
        dominant_issues = [row["issue_code"] for row in _counter_rows("issue_code", branch_issue_counts[branch])[:2]]
        rows.append(
            {
                "branch": branch,
                "note": f"Reinforce consistent separators and label formatting for {branch}.",
                "issue_codes": dominant_issues,
                "affected_report_types": [row["report_type"] for row in _counter_rows("report_type", branch_report_type_counts[branch])],
                "supporting_reviews": review_by_branch[branch],
                "supporting_rejections": rejected_by_branch[branch],
            }
        )
    return rows


def _load_raw_records(
    *,
    start_date: str,
    end_date: str,
    output_root: str | Path | None,
) -> list[dict[str, Any]]:
    raw_root = _raw_root(output_root)
    if not raw_root.exists():
        return []

    rows: list[dict[str, Any]] = []
    for path in sorted(raw_root.rglob("*.txt")):
        parsed = _parse_raw_path(path)
        if parsed is None:
            continue
        if not (start_date <= parsed["date"] <= end_date):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        rows.append(
            {
                "path": str(path),
                "date": parsed["date"],
                "branch": parsed["branch"],
                "report_type": parsed["report_type"],
                "text": text,
                "lines": [line.rstrip() for line in text.splitlines()],
                "labels": _extract_label_observations(text),
            }
        )
    return rows


def _extract_label_observations(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line in text.splitlines():
        match = _LINE_WITH_COLON_RE.match(line.strip())
        if match is None:
            continue
        label = match.group(1).strip()
        canonical = _canonical_label(label)
        if canonical is None:
            continue
        rows.append(
            {
                "variant": label,
                "canonical": canonical,
                "case_style": _case_style(label),
            }
        )
    return rows


def _load_review_links(
    *,
    start_date: str,
    end_date: str,
    output_root: str | Path | None,
) -> dict[str, dict[str, Any]]:
    review_root = _review_root(output_root)
    if not review_root.exists():
        return {}

    links: dict[str, dict[str, Any]] = {}
    for path in sorted(review_root.rglob("*.json")):
        payload = _read_json(path)
        if payload is None:
            continue
        date = _string_or_none(payload.get("date"))
        if date is None:
            continue
        try:
            normalized_date = _normalize_date(date)
        except ValueError:
            continue
        if not (start_date <= normalized_date <= end_date):
            continue
        raw_path = _review_raw_path(payload)
        if raw_path is None:
            continue
        reason = _review_reason(payload)
        links[raw_path] = {
            "path": str(path),
            "branch": _string_or_none(payload.get("branch")) or "unknown",
            "report_type": _string_or_none(payload.get("report_type")) or "unknown",
            "review_reasons": [reason] if reason is not None else [],
        }
    return links


def _load_rejected_links(
    *,
    start_date: str,
    end_date: str,
    output_root: str | Path | None,
) -> dict[str, dict[str, Any]]:
    rejected_root = _rejected_root(output_root)
    if not rejected_root.exists():
        return {}

    links: dict[str, dict[str, Any]] = {}
    for meta_path in sorted(rejected_root.rglob("*.meta.json")):
        payload = _read_json(meta_path)
        if payload is None:
            continue
        received_at = _string_or_none(payload.get("received_at"))
        if received_at is None:
            continue
        received_date = received_at[:10]
        try:
            normalized_date = _normalize_date(received_date)
        except ValueError:
            continue
        if not (start_date <= normalized_date <= end_date):
            continue
        text_path_value = _string_or_none(payload.get("original_rejected_path"))
        if text_path_value is None:
            text_path = meta_path.with_suffix("").with_suffix(".txt")
            text_path_value = str(text_path)
        rejection_reason = _string_or_none(payload.get("rejection_reason"))
        links[text_path_value] = {
            "path": str(meta_path),
            "branch": _string_or_none(payload.get("branch_hint")) or "unknown",
            "report_type": _string_or_none(payload.get("attempted_report_type")) or "unknown",
            "rejection_reasons": [rejection_reason] if rejection_reason is not None else [],
        }
    return links


def _parse_raw_path(path: Path) -> dict[str, str] | None:
    parts = path.stem.split("__")
    if len(parts) != 3:
        return None
    try:
        normalized_date = _normalize_date(parts[0])
    except ValueError:
        return None
    return {
        "date": normalized_date,
        "branch": parts[1],
        "report_type": parts[2],
    }


def _review_raw_path(payload: Mapping[str, Any]) -> str | None:
    provenance = payload.get("provenance")
    if isinstance(provenance, Mapping):
        value = _string_or_none(provenance.get("source_record_path"))
        if value is not None:
            return value
    raw_paths = payload.get("raw_paths")
    if isinstance(raw_paths, Mapping):
        value = _string_or_none(raw_paths.get("raw_txt_path"))
        if value is not None:
            return value
    return None


def _review_reason(payload: Mapping[str, Any]) -> str | None:
    value = _string_or_none(payload.get("reason"))
    if value is not None:
        return value
    governance = payload.get("governance")
    if isinstance(governance, Mapping):
        reasons = governance.get("reasons")
        if isinstance(reasons, list):
            for item in reasons:
                if isinstance(item, str) and item.strip():
                    return item.strip()
    return None


def _candidate_reason(issue_code: str) -> str:
    if issue_code == "label_spelling_variant":
        return "Observed repeated label spelling variants for one report family."
    if issue_code == "casing_drift":
        return "Observed repeated casing drift across label lines."
    if issue_code == "inconsistent_totals_syntax":
        return "Observed repeated totals syntax drift."
    return "Observed repeated formatting drift in raw WhatsApp reports."


def _template_note(issue_code: str) -> str:
    if issue_code == "label_spelling_variant":
        return "Show one canonical label example in the reporting template."
    if issue_code == "casing_drift":
        return "Show one consistent label-casing example in the reporting template."
    if issue_code == "inconsistent_totals_syntax":
        return "Show totals using `Label: value` consistently."
    return "Use `Label: value` formatting consistently in sample templates."


def _totals_style(line: str) -> str:
    if ":" in line:
        return "colon"
    if "=" in line:
        return "equals"
    if "-" in line:
        return "dash"
    return "other"


def _case_style(label: str) -> str:
    if label.upper() == label and any(character.isalpha() for character in label):
        return "upper"
    if label.lower() == label and any(character.isalpha() for character in label):
        return "lower"
    words = [word for word in re.split(r"\s+", label.strip()) if word]
    if words and all(word[:1].isupper() and word[1:].islower() for word in words if word.isalpha()):
        return "title"
    return "mixed"


def _canonical_label(label: str) -> str | None:
    cleaned = _LABEL_CANONICAL_RE.sub(" ", label.strip().lower()).strip()
    return cleaned or None


def _source_paths(
    raw_records: list[dict[str, Any]],
    review_links: Mapping[str, dict[str, Any]],
    rejected_links: Mapping[str, dict[str, Any]],
) -> list[str]:
    paths = {record["path"] for record in raw_records}
    paths.update(item["path"] for item in review_links.values() if isinstance(item.get("path"), str))
    paths.update(item["path"] for item in rejected_links.values() if isinstance(item.get("path"), str))
    return sorted(paths)


def _counter_rows(field_name: str, counts: Counter[str]) -> list[dict[str, Any]]:
    return [
        {
            field_name: key,
            "count": count,
        }
        for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _raw_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return record_paths.RAW_WHATSAPP_DIR
    return Path(output_root) / "records" / "raw" / "whatsapp"


def _review_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return record_paths.REVIEW_DIR
    return Path(output_root) / "records" / "review"


def _rejected_root(output_root: str | Path | None) -> Path:
    if output_root is None:
        return record_paths.REJECTED_DIR
    return Path(output_root) / "records" / "rejected" / "whatsapp"


def _normalize_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")


def _window_start_date(report_date: str, window_days: int) -> str:
    anchor = datetime.strptime(report_date, "%Y-%m-%d").date()
    return (anchor - timedelta(days=window_days - 1)).strftime("%Y-%m-%d")


def _normalized_window_days(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError("window_days must be a positive integer")
    return value


def _string_or_none(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _path_or_none(value: object) -> Path | None:
    if isinstance(value, (str, Path)):
        return Path(value)
    return None


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _failure_result(message: str) -> AgentResult:
    return AgentResult(
        agent_name=AGENT_NAME,
        payload={
            "signal_type": SIGNAL_TYPE,
            "source_agent": AGENT_NAME,
            "report_date": None,
            "output_path": None,
            "status": "invalid_input",
            "warnings": [
                {
                    "code": "missing_fields",
                    "severity": "error",
                    "message": message,
                }
            ],
        },
    )
