"""Export canonical structured records into downstream IOI Colony signal events."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from apps.hr_agent.field_mapper import canonical_branch_slug
from apps.hr_agent.staff_identity import normalize_staff_name
from packages.common.paths import REPO_ROOT
from packages.record_store.paths import (
    RECORDS_DIR,
    get_structured_path_for_root,
)
from packages.record_store.reader import read_structured
from packages.record_store.writer import ensure_directory, write_json_file

CONTRACT_VERSION = "1.0"
SIGNALS_DIRNAME = "SIGNALS"
NORMALIZED_DIRNAME = "normalized"

RECORD_TYPE_TO_SIGNAL_TYPE: dict[str, str] = {
    "sales_income": "daily_sales_report",
    "hr_performance": "staff_performance_report",
    "hr_attendance": "staff_attendance_report",
    "pricing_stock_release": "daily_bale_summary_report",
    "supervisor_control": "supervisor_control_report",
}

SIGNAL_TYPE_TO_REPORT_TYPE: dict[str, str] = {
    "daily_sales_report": "sales_report",
    "staff_performance_report": "staff_report",
    "staff_attendance_report": "staff_attendance_report",
    "daily_bale_summary_report": "bale_report",
    "supervisor_control_report": "supervisor_control_report",
}

COMPATIBILITY_WARNING = (
    "Compatibility mode requested, but no additional compatibility outputs are "
    "defined for this bridge. Canonical JSON event only."
)


def validate_iso_date(value: str) -> str:
    """Return one validated ISO date string."""

    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError as error:
        raise ValueError(f"Invalid ISO date {value!r}; expected YYYY-MM-DD.") from error


def canonicalize_branch(value: str) -> str:
    """Return one canonical branch slug."""

    canonical = canonical_branch_slug(value).strip()
    if not canonical:
        raise ValueError("Branch is required.")
    return canonical


def sha256_file(path: Path) -> str:
    """Return the stable SHA-256 of one file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_signal_id(
    source_record_type: str,
    branch: str,
    report_date: str,
    source_record_sha256: str,
) -> str:
    """Return a deterministic signal id for one exported event."""

    canonical_branch = canonicalize_branch(branch)
    iso_date = validate_iso_date(report_date)
    short_hash = source_record_sha256[:12]
    return f"{canonical_branch}__{source_record_type}__{iso_date}__{short_hash}"


def build_signal_envelope(
    *,
    source_record_type: str,
    signal_type: str,
    branch: str,
    report_date: str,
    source_path: Path,
    source_root: Path,
    payload: dict[str, Any],
    warnings: list[Any],
    extra_fields: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the canonical normalized event envelope."""

    canonical_branch = canonicalize_branch(branch)
    iso_date = validate_iso_date(report_date)
    source_sha = sha256_file(source_path)
    relative_source_path = _relative_to_root(source_path, source_root)
    event: dict[str, Any] = {
        "signal_id": build_signal_id(source_record_type, canonical_branch, iso_date, source_sha),
        "signal_type": signal_type,
        "event_kind": signal_type,
        "branch": canonical_branch,
        "branch_slug": canonical_branch,
        "report_date": iso_date,
        "date": iso_date,
        "source_system": "toptown_ops",
        "source_record_type": source_record_type,
        "source_record_path": relative_source_path,
        "source_record_sha256": source_sha,
        "contract_version": CONTRACT_VERSION,
        "payload": payload,
        "warnings": list(warnings),
    }
    report_type = SIGNAL_TYPE_TO_REPORT_TYPE.get(signal_type)
    if report_type is not None:
        event["report_type"] = report_type
    for key, value in (extra_fields or {}).items():
        if value is None:
            continue
        event[key] = value
    return event


def write_signal_event(
    event: Mapping[str, Any],
    *,
    output_root: str | Path,
    overwrite: bool = False,
) -> Path:
    """Write one normalized event into the canonical IOI Colony path."""

    branch = canonicalize_branch(str(event["branch_slug"]))
    report_date = validate_iso_date(str(event["report_date"]))
    signal_type = str(event["signal_type"]).strip()
    if not signal_type:
        raise ValueError("signal_type is required for downstream export.")

    output_path = (
        Path(output_root)
        / SIGNALS_DIRNAME
        / NORMALIZED_DIRNAME
        / branch
        / report_date
        / f"{signal_type}__{branch}__{report_date}.json"
    )
    ensure_directory(output_path.parent)
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"{output_path} already exists; pass overwrite=True to replace it.")
    return write_json_file(output_path, dict(event))


def export_one_record_type(
    record_type: str,
    branch: str,
    report_date: str,
    *,
    source_root: str | Path | None = None,
    colony_root: str | Path | None = None,
    overwrite: bool = False,
    write_compat: bool = False,
) -> dict[str, Any]:
    """Export one structured record type and write one manifest."""

    if colony_root is None:
        raise ValueError("colony_root is required.")
    started_at = _utc_now_iso()
    canonical_branch = canonicalize_branch(branch)
    iso_date = validate_iso_date(report_date)
    result = _export_record_type_result(
        record_type,
        canonical_branch,
        iso_date,
        source_root=source_root,
        colony_root=colony_root,
        overwrite=overwrite,
        write_compat=write_compat,
    )
    manifest = generate_export_manifest(
        [result],
        branch=canonical_branch,
        report_date=iso_date,
        source_root=_source_repo_root(source_root),
        colony_root=Path(colony_root),
        started_at=started_at,
        finished_at=_utc_now_iso(),
    )
    _write_manifest(manifest, colony_root=Path(colony_root), branch=canonical_branch, report_date=iso_date)
    return manifest


def export_all_record_types(
    branch: str,
    report_date: str,
    *,
    source_root: str | Path | None = None,
    colony_root: str | Path | None = None,
    overwrite: bool = False,
    write_compat: bool = False,
) -> dict[str, Any]:
    """Export all supported structured record types and write one manifest."""

    if colony_root is None:
        raise ValueError("colony_root is required.")
    started_at = _utc_now_iso()
    canonical_branch = canonicalize_branch(branch)
    iso_date = validate_iso_date(report_date)
    results = [
        _export_record_type_result(
            record_type,
            canonical_branch,
            iso_date,
            source_root=source_root,
            colony_root=colony_root,
            overwrite=overwrite,
            write_compat=write_compat,
        )
        for record_type in RECORD_TYPE_TO_SIGNAL_TYPE
    ]
    manifest = generate_export_manifest(
        results,
        branch=canonical_branch,
        report_date=iso_date,
        source_root=_source_repo_root(source_root),
        colony_root=Path(colony_root),
        started_at=started_at,
        finished_at=_utc_now_iso(),
    )
    _write_manifest(manifest, colony_root=Path(colony_root), branch=canonical_branch, report_date=iso_date)
    return manifest


def generate_export_manifest(
    results: Sequence[Mapping[str, Any]],
    *,
    branch: str,
    report_date: str,
    source_root: Path,
    colony_root: Path,
    started_at: str,
    finished_at: str,
) -> dict[str, Any]:
    """Build one manifest payload for one export run."""

    normalized_results = [dict(result) for result in results]
    summary = {
        "scanned": len(normalized_results),
        "written": sum(1 for result in normalized_results if result.get("status") == "written"),
        "missing": sum(1 for result in normalized_results if result.get("status") == "missing"),
        "skipped": sum(1 for result in normalized_results if result.get("status") == "skipped"),
        "failed": sum(1 for result in normalized_results if result.get("status") == "failed"),
    }
    run_id = f"export-{canonicalize_branch(branch)}-{validate_iso_date(report_date)}-{started_at.replace(':', '').replace('-', '')}"
    return {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "branch": canonicalize_branch(branch),
        "report_date": validate_iso_date(report_date),
        "contract_version": CONTRACT_VERSION,
        "source_root": str(source_root),
        "colony_root": str(colony_root),
        "results": normalized_results,
        "summary": summary,
    }


def map_sales_income_record(record: Mapping[str, Any], *, source_path: Path) -> dict[str, Any]:
    """Map one sales income structured record into a downstream event."""

    metrics = _mapping(record.get("metrics"))
    provenance = _mapping(record.get("provenance"))

    totals = _filtered_dict(
        {
            "gross_sales": _number_or_value(metrics.get("gross_sales")),
            "total_sales": _number_or_value(metrics.get("gross_sales")),
            "cash_sales": _number_or_value(metrics.get("cash_sales")),
            "eftpos_sales": _number_or_value(metrics.get("eftpos_sales")),
            "mobile_money_sales": _number_or_value(metrics.get("mobile_money_sales")),
            "deposit_total": _number_or_value(metrics.get("deposit_total")),
            "till_total": _number_or_value(metrics.get("till_total")),
            "z_reading": _number_or_value(metrics.get("z_reading")),
            "cash_variance": _number_or_value(metrics.get("cash_variance")),
        }
    )
    traffic = _filtered_dict(
        {
            "total_customers": _number_or_value(metrics.get("traffic")),
            "customers_served": _number_or_value(metrics.get("served")),
            "conversion_rate": _number_or_value(metrics.get("conversion_rate")),
        }
    )
    staffing = _filtered_dict(
        {
            "staff_on_duty": _number_or_value(metrics.get("staff_on_duty")),
        }
    )
    control = _filtered_dict(
        {
            "cashier": provenance.get("cashier"),
            "assistant": provenance.get("assistant"),
            "balanced_by": provenance.get("balanced_by"),
            "supervisor": provenance.get("supervisor"),
            "supervisor_confirmation": provenance.get("supervisor_confirmation"),
        }
    )
    notes = provenance.get("notes")

    payload = _filtered_dict(
        {
            "totals": totals if totals else None,
            "traffic": traffic if traffic else None,
            "staffing": staffing if staffing else None,
            "control": control if control else None,
            "notes": notes,
        }
    )
    return build_signal_envelope(
        source_record_type="sales_income",
        signal_type=RECORD_TYPE_TO_SIGNAL_TYPE["sales_income"],
        branch=_record_branch(record),
        report_date=_record_date(record),
        source_path=source_path,
        source_root=_source_root_from_source_path(source_path),
        payload=payload,
        warnings=_event_warnings(record),
        extra_fields=_filtered_dict(
            {
                "totals": totals if totals else None,
                "traffic": traffic if traffic else None,
                "staffing": staffing if staffing else None,
                "control": control if control else None,
                "notes": notes,
            }
        ),
    )


def map_hr_performance_record(record: Mapping[str, Any], *, source_path: Path) -> dict[str, Any]:
    """Map one HR performance structured record into a downstream event."""

    staff_records: list[dict[str, Any]] = []
    for item in _items_list(record.get("items")):
        staff_name = item.get("staff_name")
        staff_records.append(
            _filtered_dict(
                {
                    "staff_name": staff_name,
                    "staff_name_normalized": normalize_staff_name(str(staff_name or "")) or None,
                    "arrangement": _number_or_value(item.get("arrangement")),
                    "display": _number_or_value(item.get("display")),
                    "performance": _number_or_value(item.get("performance")),
                    "activity_score": _number_or_value(item.get("activity_score")),
                    "customers_assisted": _number_or_value(item.get("customers_assisted")),
                    "assisting_count": _number_or_value(item.get("assisting_count")),
                    "items_moved": _number_or_value(item.get("items_moved")),
                    "section": item.get("section"),
                    "raw_section": item.get("raw_section"),
                    "duty_status": item.get("duty_status"),
                    "role": item.get("role"),
                    "notes": item.get("notes"),
                }
            )
        )

    payload = {
        "staff_records": staff_records,
    }
    return build_signal_envelope(
        source_record_type="hr_performance",
        signal_type=RECORD_TYPE_TO_SIGNAL_TYPE["hr_performance"],
        branch=_record_branch(record),
        report_date=_record_date(record),
        source_path=source_path,
        source_root=_source_root_from_source_path(source_path),
        payload=payload,
        warnings=_event_warnings(record),
        extra_fields={
            "staff_records": staff_records,
        },
    )


def map_hr_attendance_record(record: Mapping[str, Any], *, source_path: Path) -> dict[str, Any]:
    """Map one HR attendance structured record into a downstream event."""

    metrics = _mapping(record.get("metrics"))
    attendance_records: list[dict[str, Any]] = []
    for item in _items_list(record.get("items")):
        staff_name = item.get("staff_name")
        attendance_records.append(
            _filtered_dict(
                {
                    "staff_name": staff_name,
                    "staff_name_normalized": normalize_staff_name(str(staff_name or "")) or None,
                    "status": item.get("status"),
                    "raw_status": item.get("raw_status"),
                    "presence_score": _number_or_value(item.get("presence_score")),
                    "section": item.get("section"),
                    "raw_section": item.get("raw_section"),
                }
            )
        )

    attendance_totals = _filtered_dict(
        {
            "present": _number_or_value(metrics.get("present_count")),
            "absent": _number_or_value(metrics.get("absent_count")),
            "off_duty": _number_or_value(metrics.get("off_count")),
            "annual_leave": _number_or_value(metrics.get("annual_leave_count")),
            "sick": _number_or_value(metrics.get("sick_count")),
            "suspended": _number_or_value(metrics.get("suspended_count")),
            "total_staff": _number_or_value(metrics.get("total_staff_records")),
        }
    )
    declared_totals = metrics.get("declared_status_totals")
    payload = _filtered_dict(
        {
            "attendance_records": attendance_records,
            "attendance_totals": attendance_totals if attendance_totals else None,
            "declared_totals": declared_totals if isinstance(declared_totals, Mapping) else None,
        }
    )
    return build_signal_envelope(
        source_record_type="hr_attendance",
        signal_type=RECORD_TYPE_TO_SIGNAL_TYPE["hr_attendance"],
        branch=_record_branch(record),
        report_date=_record_date(record),
        source_path=source_path,
        source_root=_source_root_from_source_path(source_path),
        payload=payload,
        warnings=_event_warnings(record),
        extra_fields=_filtered_dict(
            {
                "attendance_records": attendance_records,
                "attendance_totals": attendance_totals if attendance_totals else None,
                "declared_totals": declared_totals if isinstance(declared_totals, Mapping) else None,
            }
        ),
    )


def map_pricing_stock_release_record(record: Mapping[str, Any], *, source_path: Path) -> dict[str, Any]:
    """Map one pricing stock release structured record into a downstream event."""

    metrics = _mapping(record.get("metrics"))
    provenance = _mapping(record.get("provenance"))

    bales: list[dict[str, Any]] = []
    for item in _items_list(record.get("items")):
        bales.append(
            _filtered_dict(
                {
                    "bale_id": item.get("bale_id"),
                    "item_name": item.get("item_name"),
                    "qty": _number_or_value(item.get("qty")),
                    "quantity": _number_or_value(item.get("quantity")),
                    "amount": _number_or_value(item.get("amount")),
                    "value": _number_or_value(item.get("value")),
                    "price_per_piece": _number_or_value(item.get("price_per_piece")),
                }
            )
        )

    totals = _filtered_dict(
        {
            "total_qty": _number_or_value(metrics.get("total_qty")),
            "total_amount": _number_or_value(metrics.get("total_amount")),
            "bales_processed": _number_or_value(metrics.get("bales_processed")),
            "bales_released": _number_or_value(metrics.get("bales_released")),
            "bales_pending_approval": _number_or_value(metrics.get("bales_pending_approval")),
            "release_ratio": _number_or_value(metrics.get("release_ratio")),
        }
    )
    operator = _filtered_dict(
        {
            "name": provenance.get("prepared_by"),
            "role": provenance.get("role"),
        }
    )
    payload = _filtered_dict(
        {
            "bales": bales,
            "totals": totals if totals else None,
            "operator": operator if operator else None,
        }
    )
    return build_signal_envelope(
        source_record_type="pricing_stock_release",
        signal_type=RECORD_TYPE_TO_SIGNAL_TYPE["pricing_stock_release"],
        branch=_record_branch(record),
        report_date=_record_date(record),
        source_path=source_path,
        source_root=_source_root_from_source_path(source_path),
        payload=payload,
        warnings=_event_warnings(record),
        extra_fields=_filtered_dict(
            {
                "bales": bales,
                "totals": totals if totals else None,
                "released_by": provenance.get("prepared_by"),
                "operator": operator if operator else None,
            }
        ),
    )


def map_supervisor_control_record(record: Mapping[str, Any], *, source_path: Path) -> dict[str, Any]:
    """Map one supervisor control structured record into a downstream event."""

    metrics = _mapping(record.get("metrics"))
    provenance = _mapping(record.get("provenance"))
    key_values = _mapping(record.get("key_values"))
    checklist = _string_list(record.get("checklist"))
    notes = _string_list(record.get("notes"))

    payload = _filtered_dict(
        {
            "checklist": checklist or None,
            "key_values": dict(key_values) if key_values else None,
            "notes": notes or None,
            "metrics": _filtered_dict(
                {
                    "checklist_count": _number_or_value(metrics.get("checklist_count")),
                    "key_value_count": _number_or_value(metrics.get("key_value_count")),
                    "note_count": _number_or_value(metrics.get("note_count")),
                }
            )
            or None,
            "provenance": _filtered_dict(
                {
                    "raw_branch": provenance.get("raw_branch"),
                    "raw_date": provenance.get("raw_date"),
                    "detected_subtype": provenance.get("detected_subtype"),
                    "notes": _string_list(provenance.get("notes")) or None,
                }
            )
            or None,
        }
    )
    return build_signal_envelope(
        source_record_type="supervisor_control",
        signal_type=RECORD_TYPE_TO_SIGNAL_TYPE["supervisor_control"],
        branch=_record_branch(record),
        report_date=_record_date(record),
        source_path=source_path,
        source_root=_source_root_from_source_path(source_path),
        payload=payload,
        warnings=_event_warnings(record),
        extra_fields=_filtered_dict(
            {
                "checklist": checklist or None,
                "key_values": dict(key_values) if key_values else None,
                "notes": notes or None,
            }
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI bridge entrypoint."""

    parser = argparse.ArgumentParser(description="Export structured TopTown records into IOI Colony signals.")
    parser.add_argument("--branch", required=True, help="Canonical branch slug or recognized branch label.")
    parser.add_argument("--date", required=True, help="ISO report date in YYYY-MM-DD format.")
    parser.add_argument("--colony-root", required=True, help="Path to the ioi-colony repo root.")
    parser.add_argument("--source-root", help="Optional source repo root override for tests.")
    parser.add_argument(
        "--record-type",
        choices=sorted(RECORD_TYPE_TO_SIGNAL_TYPE),
        help="Export one structured record type.",
    )
    parser.add_argument("--all", action="store_true", help="Export all supported structured record types.")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting existing output files.")
    parser.add_argument("--write-compat", action="store_true", help="Attempt compatibility writes when supported.")
    parser.add_argument("--print-json", action="store_true", help="Print the manifest JSON to stdout.")
    args = parser.parse_args(argv)

    if args.all == bool(args.record_type):
        parser.error("Choose exactly one of --record-type or --all.")

    try:
        if args.all:
            manifest = export_all_record_types(
                args.branch,
                args.date,
                source_root=args.source_root,
                colony_root=args.colony_root,
                overwrite=args.overwrite,
                write_compat=args.write_compat,
            )
        else:
            manifest = export_one_record_type(
                args.record_type,
                args.branch,
                args.date,
                source_root=args.source_root,
                colony_root=args.colony_root,
                overwrite=args.overwrite,
                write_compat=args.write_compat,
            )
    except ValueError as error:
        parser.error(str(error))
        return 2

    if args.write_compat:
        print(COMPATIBILITY_WARNING)
    if args.print_json:
        print(json.dumps(manifest, indent=2, sort_keys=True))
    else:
        print(
            f"Exported {manifest['summary']['written']} signal(s) "
            f"for {manifest['branch']} on {manifest['report_date']}."
        )
    return 0 if manifest["summary"]["failed"] == 0 else 1


def _export_record_type_result(
    record_type: str,
    branch: str,
    report_date: str,
    *,
    source_root: str | Path | None,
    colony_root: str | Path,
    overwrite: bool,
    write_compat: bool,
) -> dict[str, Any]:
    if record_type not in RECORD_TYPE_TO_SIGNAL_TYPE:
        raise ValueError(f"Unsupported record type: {record_type}")

    source_repo_root = _source_repo_root(source_root)
    canonical_branch = canonicalize_branch(branch)
    iso_date = validate_iso_date(report_date)
    record, source_path = load_structured_input(
        record_type,
        canonical_branch,
        iso_date,
        source_root=source_repo_root,
    )
    result: dict[str, Any] = {
        "record_type": record_type,
        "status": "missing",
        "source_path": _relative_to_root(source_path, source_repo_root),
        "output_path": None,
        "signal_type": RECORD_TYPE_TO_SIGNAL_TYPE[record_type],
        "reason": "source_record_missing",
        "warnings": [],
    }
    if record is None:
        return result

    mapper = _mapper_for_record_type(record_type)
    try:
        event = mapper(record, source_path=source_path)
    except Exception as error:
        result["status"] = "failed"
        result["reason"] = str(error)
        return result
    output_path = _signal_output_path(
        Path(colony_root),
        canonical_branch,
        iso_date,
        str(event["signal_type"]),
    )
    result["output_path"] = _relative_to_root(output_path, Path(colony_root))

    try:
        written_path = write_signal_event(event, output_root=colony_root, overwrite=overwrite)
    except FileExistsError:
        result["status"] = "skipped"
        result["reason"] = "output_exists_use_overwrite"
        return result
    except Exception as error:
        result["status"] = "failed"
        result["reason"] = str(error)
        return result

    result["status"] = "written"
    result["reason"] = "written"
    result["output_path"] = _relative_to_root(written_path, Path(colony_root))
    if write_compat:
        result["warnings"].append(COMPATIBILITY_WARNING)
    return result


def load_structured_input(
    record_type: str,
    branch: str,
    report_date: str,
    *,
    source_root: Path,
) -> tuple[dict[str, Any] | None, Path]:
    canonical_branch = canonicalize_branch(branch)
    iso_date = validate_iso_date(report_date)
    source_path = get_structured_path_for_root(
        source_root / RECORDS_DIR.name / "structured",
        signal_type=record_type,
        branch=canonical_branch,
        date=iso_date,
    )
    record = read_structured(record_type, canonical_branch, iso_date, root=source_root)
    return record, source_path


def _mapper_for_record_type(record_type: str):
    return {
        "sales_income": map_sales_income_record,
        "hr_performance": map_hr_performance_record,
        "hr_attendance": map_hr_attendance_record,
        "pricing_stock_release": map_pricing_stock_release_record,
        "supervisor_control": map_supervisor_control_record,
    }[record_type]


def _record_branch(record: Mapping[str, Any]) -> str:
    branch = record.get("branch_slug") or record.get("branch")
    if not isinstance(branch, str) or not branch.strip():
        raise ValueError("Structured record is missing branch information.")
    return canonicalize_branch(branch)


def _record_date(record: Mapping[str, Any]) -> str:
    report_date = record.get("report_date") or record.get("date")
    if not isinstance(report_date, str) or not report_date.strip():
        raise ValueError("Structured record is missing report_date.")
    return validate_iso_date(report_date)


def _event_warnings(record: Mapping[str, Any]) -> list[Any]:
    warnings = record.get("warnings")
    if isinstance(warnings, list):
        return list(warnings)
    return []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _items_list(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, str) and item.strip()]


def _filtered_dict(values: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in values.items()
        if value is not None
    }


def _number_or_value(value: Any) -> int | float | str | bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        return value if value.strip() else None
    return None


def _source_repo_root(source_root: str | Path | None) -> Path:
    return Path(source_root) if source_root is not None else REPO_ROOT


def _source_root_from_source_path(source_path: Path) -> Path:
    return source_path.parents[4]


def _signal_output_path(colony_root: Path, branch: str, report_date: str, signal_type: str) -> Path:
    return colony_root / SIGNALS_DIRNAME / NORMALIZED_DIRNAME / branch / report_date / (
        f"{signal_type}__{branch}__{report_date}.json"
    )


def _write_manifest(
    manifest: Mapping[str, Any],
    *,
    colony_root: Path,
    branch: str,
    report_date: str,
) -> Path:
    manifest_path = (
        colony_root
        / SIGNALS_DIRNAME
        / NORMALIZED_DIRNAME
        / branch
        / report_date
        / "_export_manifest.json"
    )
    ensure_directory(manifest_path.parent)
    return write_json_file(manifest_path, dict(manifest))


def _relative_to_root(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(path)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
