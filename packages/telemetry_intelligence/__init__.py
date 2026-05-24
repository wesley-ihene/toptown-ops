"""Compact daily telemetry intelligence for runtime logs."""

from .summarizer import (
    SCHEMA_VERSION,
    discover_log_paths,
    resolve_report_dates,
    summarize_telemetry_for_date,
    summarize_telemetry_for_dates,
    telemetry_output_dir,
)

__all__ = [
    "SCHEMA_VERSION",
    "discover_log_paths",
    "resolve_report_dates",
    "summarize_telemetry_for_date",
    "summarize_telemetry_for_dates",
    "telemetry_output_dir",
]
