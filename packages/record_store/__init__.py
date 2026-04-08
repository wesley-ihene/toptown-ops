"""Filesystem helpers for operational record storage."""

from . import reader, writer
from .naming import build_raw_filename, build_rejected_filename, build_structured_filename
from .paths import (
    RECORDS_DIR,
    get_raw_path,
    get_rejected_path,
    get_structured_path,
)
from .reader import read_structured
from .schema import (
    HR_ATTENDANCE_SCHEMA,
    HR_PERFORMANCE_SCHEMA,
    PRICING_STOCK_RELEASE_SCHEMA,
    SALES_INCOME_SCHEMA,
)
from .writer import write_raw, write_rejected, write_structured

__all__ = [
    "HR_ATTENDANCE_SCHEMA",
    "HR_PERFORMANCE_SCHEMA",
    "PRICING_STOCK_RELEASE_SCHEMA",
    "RECORDS_DIR",
    "SALES_INCOME_SCHEMA",
    "build_raw_filename",
    "build_rejected_filename",
    "build_structured_filename",
    "get_raw_path",
    "get_rejected_path",
    "get_structured_path",
    "read_structured",
    "reader",
    "write_raw",
    "write_rejected",
    "write_structured",
    "writer",
]
