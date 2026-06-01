#!/usr/bin/env python3

from __future__ import annotations

import argparse
import glob
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path.cwd()

STATUS_ACCEPTED = "ACCEPTED"
STATUS_REVIEW = "REVIEW_REQUIRED"
STATUS_DUPLICATE = "DUPLICATE"
STATUS_RAW_ONLY = "RAW_ONLY"
STATUS_NOT_FOUND = "NOT_FOUND"

REPORT_TYPE_ALIASES = {
    "sales_income": {"sales_income", "sales", "day_end_sales", "day-end sales report"},
    "pricing_stock_release": {"pricing_stock_release", "pricing", "bale_summary", "bale_release"},
    "staff_attendance": {"staff_attendance", "attendance", "hr_attendance", "staff_attendance_report"},
    "staff_performance": {"staff_performance", "hr_performance", "staff_performance_report"},
    "supervisor_control": {"supervisor_control", "supervisor_control_summary"},
}


def load_json(path: str | Path) -> dict[str, Any] | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except Exception:
        return None
    return loaded if isinstance(loaded, dict) else None


def _normalize_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip().casefold()
    if not cleaned:
        return None
    return cleaned.replace("-", "_").replace(" ", "_")


def _report_type_aliases(report_type: str) -> set[str]:
    normalized = _normalize_text(report_type) or report_type.casefold()
    return REPORT_TYPE_ALIASES.get(normalized, {normalized})


def _date_variants(date_text: str) -> set[str]:
    variants = {date_text}
    parts = date_text.split("-")
    if len(parts) == 3:
        year, month, day = parts
        variants.add(f"{day}/{month}/{year}")
        variants.add(f"{day}/{month}/{year[2:]}")
        variants.add(f"{year}_{month}_{day}")
    return {value.casefold() for value in variants}


def _json_text(data: dict[str, Any]) -> str:
    try:
        return json.dumps(data, sort_keys=True).casefold()
    except Exception:
        return ""


def _response_feedback(data: dict[str, Any]) -> dict[str, Any]:
    feedback = data.get("feedback")
    return feedback if isinstance(feedback, dict) else {}


def _matches_branch_date_type(
    *,
    branch: str,
    date_text: str,
    report_type: str,
    category: str,
    path: Path,
    json_data: dict[str, Any] | None,
) -> bool:
    aliases = _report_type_aliases(report_type)
    date_values = _date_variants(date_text)
    path_text = str(path).casefold()

    if json_data is None:
        if branch not in path_text:
            return False
        if not any(value in path_text for value in date_values):
            return False
        return any(alias in path_text for alias in aliases)

    feedback = _response_feedback(json_data)
    candidate_branches = {
        value
        for value in (
            _normalize_text(json_data.get("branch")),
            _normalize_text(feedback.get("branch")),
        )
        if value is not None
    }
    candidate_dates = {
        value.casefold()
        for value in (
            json_data.get("report_date"),
            feedback.get("report_date"),
            ((feedback.get("diagnostics") or {}).get("date") if isinstance(feedback.get("diagnostics"), dict) else None),
        )
        if isinstance(value, str) and value.strip()
    }
    candidate_types = {
        value
        for value in (
            _normalize_text(json_data.get("report_type")),
            _normalize_text(json_data.get("reason")),
            _normalize_text(feedback.get("report_type")),
            _normalize_text(feedback.get("route")),
        )
        if value is not None
    }

    if category == "duplicates":
        duplicate_type = _normalize_text(json_data.get("report_type"))
        if duplicate_type is not None:
            candidate_types.add(duplicate_type)
    if category == "structured":
        signal_type = _normalize_text(json_data.get("signal_type"))
        signal_subtype = _normalize_text(json_data.get("signal_subtype"))
        if signal_type is not None:
            candidate_types.add(signal_type)
        if signal_subtype is not None:
            candidate_types.add(signal_subtype)

    if branch in candidate_branches and candidate_dates & date_values and candidate_types & aliases:
        return True

    text = _json_text(json_data)
    return branch in text and any(value in text for value in date_values) and any(alias in text for alias in aliases)


def search_files(branch: str, date_text: str, report_type: str) -> dict[str, list[str]]:
    results = {
        "structured": [],
        "responses": [],
        "duplicates": [],
        "raw": [],
        "review": [],
    }

    search_roots = [
        ("structured", ROOT / "records" / "structured"),
        ("responses", ROOT / "records" / "responses"),
        ("duplicates", ROOT / "records" / "duplicates"),
        ("raw", ROOT / "records" / "raw"),
        ("review", ROOT / "records" / "review"),
    ]

    for category, root in search_roots:
        for raw_path in glob.glob(f"{root}/**/*", recursive=True):
            path = Path(raw_path)
            if not path.is_file():
                continue

            json_data = load_json(path) if path.suffix == ".json" else None
            if _matches_branch_date_type(
                branch=branch,
                date_text=date_text,
                report_type=report_type,
                category=category,
                path=path,
                json_data=json_data,
            ):
                results[category].append(str(path))

    return results


def _first_mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _extract_failure_details(paths: list[str]) -> dict[str, str] | None:
    for raw_path in paths:
        path = Path(raw_path)
        if path.suffix != ".json":
            continue
        data = load_json(path)
        if not data:
            continue
        feedback = _response_feedback(data)
        diagnostics = _first_mapping(feedback.get("diagnostics"))
        failed_rules = diagnostics.get("failed_rules")
        if not isinstance(failed_rules, list):
            continue
        for rule in failed_rules:
            if not isinstance(rule, dict):
                continue
            code = rule.get("code")
            expected = rule.get("expected")
            received = rule.get("received")
            if not isinstance(code, str) or not code.strip():
                continue
            return {
                "code": code.strip(),
                "expected": _summarize_value(expected),
                "received": _summarize_value(received),
            }
    return None


def _summarize_value(value: object) -> str:
    if not isinstance(value, str):
        return "unknown"
    sales_match = re.search(r"Sales\s+(K[\d,]+(?:\.\d{2})?)", value)
    if sales_match is not None:
        return sales_match.group(1)
    stripped = value.strip()
    return stripped or "unknown"


def _extract_duplicate_reason(duplicate_paths: list[str], response_paths: list[str]) -> str | None:
    for raw_path in duplicate_paths:
        data = load_json(raw_path)
        if not data:
            continue
        reason = data.get("duplicate_reason")
        if isinstance(reason, str) and reason.strip():
            return reason.strip()

    for raw_path in response_paths:
        data = load_json(raw_path)
        if not data:
            continue
        if data.get("governance_status") != "duplicate":
            continue
        reason = data.get("reason")
        if isinstance(reason, str) and reason.strip():
            return reason.strip()
        feedback = _response_feedback(data)
        reason_codes = feedback.get("reason_codes")
        if isinstance(reason_codes, list):
            for item in reason_codes:
                if isinstance(item, str) and item.strip():
                    return item.strip()
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--branch", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--type", required=True)

    args = parser.parse_args()

    branch = args.branch.lower()
    report_date = args.date
    report_type = args.type.lower()

    print("=" * 70)
    print("TAOP REPORT TRACE")
    print("=" * 70)
    print(f"Branch      : {branch}")
    print(f"Date        : {report_date}")
    print(f"Report Type : {report_type}")
    print("-" * 70)

    results = search_files(branch, report_date, report_type)

    structured_exists = len(results["structured"]) > 0
    duplicate_exists = len(results["duplicates"]) > 0
    response_exists = len(results["responses"]) > 0
    raw_exists = len(results["raw"]) > 0
    review_exists = len(results["review"]) > 0

    if structured_exists:
        status = STATUS_ACCEPTED
    elif duplicate_exists:
        status = STATUS_DUPLICATE
    elif response_exists or review_exists:
        status = STATUS_REVIEW
    elif raw_exists:
        status = STATUS_RAW_ONLY
    else:
        status = STATUS_NOT_FOUND

    failure = _extract_failure_details(results["responses"])
    duplicate_reason = _extract_duplicate_reason(results["duplicates"], results["responses"])

    print(f"STATUS: {status}")
    print("-" * 70)

    print(f"RAW RECEIVED        : {'YES' if raw_exists else 'NO'}")
    print(f"RESPONSE GENERATED  : {'YES' if response_exists else 'NO'}")
    print(f"REVIEW RECORD       : {'YES' if review_exists else 'NO'}")
    print(f"DUPLICATE DETECTED  : {'YES' if duplicate_exists else 'NO'}")
    print(f"STRUCTURED ACCEPTED : {'YES' if structured_exists else 'NO'}")

    if failure is not None:
        print()
        print(f"FAILURE CODE : {failure['code']}")
        print(f"EXPECTED     : {failure['expected']}")
        print(f"RECEIVED     : {failure['received']}")

    if duplicate_reason is not None:
        print()
        print(f"DUPLICATE REASON : {duplicate_reason}")

    print("-" * 70)

    for category, files in results.items():
        if not files:
            continue

        print(f"{category.upper()} FILES:")
        for file_path in files[:10]:
            print(f"  - {file_path}")
        print()

    print("=" * 70)


if __name__ == "__main__":
    main()
