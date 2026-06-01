"""Filesystem helpers for operational record storage."""

from . import reader, writer
from .duplicate_archive import archive_duplicate_record
from .naming import build_raw_filename, build_rejected_filename, build_structured_filename
from .paths import (
    ACTIONS_DIR,
    DUPLICATES_DIR,
    FEEDBACK_DIR,
    RECORDS_DIR,
    get_action_path,
    get_action_preview_path,
    get_duplicate_archive_dir,
    get_feedback_path,
    get_raw_path,
    get_rejected_path,
    get_review_path,
    get_structured_path,
)
from .reader import read_structured
from .schema import (
    HR_ATTENDANCE_SCHEMA,
    HR_PERFORMANCE_SCHEMA,
    PRICING_STOCK_RELEASE_SCHEMA,
    SALES_INCOME_SCHEMA,
)
from .writer import write_governed_structured, write_raw, write_rejected, write_structured

__all__ = [
    "HR_ATTENDANCE_SCHEMA",
    "HR_PERFORMANCE_SCHEMA",
    "PRICING_STOCK_RELEASE_SCHEMA",
    "ACTIONS_DIR",
    "DUPLICATES_DIR",
    "FEEDBACK_DIR",
    "RECORDS_DIR",
    "SALES_INCOME_SCHEMA",
    "archive_duplicate_record",
    "build_raw_filename",
    "build_rejected_filename",
    "build_structured_filename",
    "get_action_path",
    "get_action_preview_path",
    "get_duplicate_archive_dir",
    "get_feedback_path",
    "get_raw_path",
    "get_rejected_path",
    "get_review_path",
    "get_structured_path",
    "read_structured",
    "reader",
    "write_raw",
    "write_rejected",
    "write_structured",
    "writer",
    "write_governed_structured",
]
