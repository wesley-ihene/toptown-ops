"""Read-only TAOP operations API routes."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any

from packages.common.branch import canonical_branch_slug_or_none
from packages.taop_ops.exports import (
    default_export_directory,
    display_export_path,
    export_attendance_csv,
    export_bale_release_csv,
    export_daily_operations_json,
    export_sales_csv,
)
from packages.taop_ops.loader import (
    load_attendance_records,
    load_bale_release_records,
    load_sales_records,
)
from packages.taop_ops.summaries import (
    build_daily_operations_summary,
    summarize_attendance,
    summarize_bale_release,
    summarize_sales,
)

SERVICE_NAME = "taop_ops_api"


def route_request(path: str, params: Mapping[str, list[str]], *, root: str | None = None) -> tuple[int, dict[str, Any]] | None:
    """Dispatch one read-only TAOP ops API route."""

    if path in {"/taop/ops/daily", "/api/taop/ops/daily"}:
        date, branch, error = _filters_from_params(params)
        if error is not None:
            return error
        return HTTPStatus.OK, {
            "ok": True,
            "service": SERVICE_NAME,
            "product": "daily_operations",
            "report_date": date,
            "branch": branch,
            "payload": build_daily_operations_summary(date, branch=branch, root=root),
        }

    if path in {"/taop/ops/sales", "/api/taop/ops/sales"}:
        date, branch, error = _filters_from_params(params)
        if error is not None:
            return error
        payload = summarize_sales(load_sales_records(date=date, branch=branch, root=root))
        return HTTPStatus.OK, _payload_response("sales", date, branch, payload)

    if path in {"/taop/ops/bale-release", "/api/taop/ops/bale-release"}:
        date, branch, error = _filters_from_params(params)
        if error is not None:
            return error
        payload = summarize_bale_release(load_bale_release_records(date=date, branch=branch, root=root))
        return HTTPStatus.OK, _payload_response("bale_release", date, branch, payload)

    if path in {"/taop/ops/attendance", "/api/taop/ops/attendance"}:
        date, branch, error = _filters_from_params(params)
        if error is not None:
            return error
        payload = summarize_attendance(load_attendance_records(date=date, branch=branch, root=root))
        return HTTPStatus.OK, _payload_response("attendance", date, branch, payload)

    if path in {"/taop/ops/export", "/api/taop/ops/export"}:
        date, branch, error = _filters_from_params(params)
        if error is not None:
            return error
        export_type = _normalize_export_type(_query_value(params, "type"))
        if export_type is None:
            return HTTPStatus.BAD_REQUEST, {
                "ok": False,
                "service": SERVICE_NAME,
                "error": "invalid_export_type",
                "message": "`type` must be one of sales, bale_release, attendance, or daily.",
            }
        export_dir = default_export_directory(date, branch=branch, root=root)
        output_path = _export_path_for_type(export_type, date=date, branch=branch, export_dir=export_dir, root=root)
        return HTTPStatus.OK, {
            "ok": True,
            "service": SERVICE_NAME,
            "product": "export",
            "report_date": date,
            "branch": branch,
            "export_type": export_type,
            "output_path": display_export_path(output_path, root=root),
        }

    return None


def _payload_response(product: str, report_date: str, branch: str | None, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "service": SERVICE_NAME,
        "product": product,
        "report_date": report_date,
        "branch": branch,
        "payload": payload,
    }


def _filters_from_params(
    params: Mapping[str, list[str]],
) -> tuple[str, str | None, tuple[int, dict[str, Any]] | None]:
    report_date = _query_value(params, "date")
    if report_date is None:
        return "", None, (
            HTTPStatus.BAD_REQUEST,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error": "missing_filters",
                "message": "`date` query parameter is required.",
            },
        )
    try:
        iso_date = datetime.strptime(report_date, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return "", None, (
            HTTPStatus.BAD_REQUEST,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error": "invalid_date",
                "message": "`date` must use YYYY-MM-DD format.",
            },
        )
    branch_value = _query_value(params, "branch")
    branch = _canonical_branch_or_none(branch_value)
    if branch_value is not None and branch is None:
        return "", None, (
            HTTPStatus.BAD_REQUEST,
            {
                "ok": False,
                "service": SERVICE_NAME,
                "error": "invalid_branch",
                "message": "`branch` must be one configured branch slug or recognized branch label.",
            },
        )
    return iso_date, branch, None


def _export_path_for_type(
    export_type: str,
    *,
    date: str,
    branch: str | None,
    export_dir: Path,
    root: str | None,
) -> Path:
    if export_type == "sales":
        return export_sales_csv(date, export_dir, branch=branch, root=root)
    if export_type == "bale_release":
        return export_bale_release_csv(date, export_dir, branch=branch, root=root)
    if export_type == "attendance":
        return export_attendance_csv(date, export_dir, branch=branch, root=root)
    return export_daily_operations_json(date, export_dir, branch=branch, root=root)


def _normalize_export_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower().replace("-", "_")
    if normalized in {"sales", "bale_release", "attendance", "daily"}:
        return normalized
    return None


def _canonical_branch_or_none(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return canonical_branch_slug_or_none(cleaned)


def _query_value(params: Mapping[str, list[str]], key: str) -> str | None:
    values = params.get(key)
    if not values:
        return None
    value = values[0].strip()
    return value or None
