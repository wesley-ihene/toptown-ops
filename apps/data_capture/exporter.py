"""Accounting-oriented CSV bundle and dependency-free XLSX exports."""

from __future__ import annotations

import csv
from io import BytesIO, StringIO
import json
from typing import Any, Iterable
from xml.sax.saxutils import escape
from zipfile import ZIP_DEFLATED, ZipFile

REPORT_TYPES = ("sales_income", "supervisor_control", "hr_performance", "hr_attendance", "pricing_stock_release")


def tabular_reports(reports: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Flatten reports without changing any stored values."""

    result = {report_type: [] for report_type in REPORT_TYPES}
    for report in reports:
        common = {
            "report_id": report["report_id"],
            "branch": report["branch"],
            "report_date": report["report_date"],
            "version": report["version"],
            "submitted_by": report["submitted_by_name"],
            "submitted_at": report["submitted_at"],
            "change_reason": report.get("change_reason") or "",
        }
        metrics = {f"total_{key}" if not key.startswith("total_") else key: value for key, value in report["metrics"].items()}
        details = {
            f"detail_{key}": json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
            for key, value in report["details"].items()
        }
        lines = report.get("lines") or [{}]
        for line in lines:
            result[report["report_type"]].append({**common, **metrics, **details, **line})
    return result


def csv_zip(tables: dict[str, list[dict[str, Any]]]) -> bytes:
    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        for name, rows in tables.items():
            archive.writestr(f"{name}.csv", _csv_text(rows))
    return output.getvalue()


def xlsx_workbook(tables: dict[str, list[dict[str, Any]]]) -> bytes:
    """Create a standards-compliant, simple XLSX workbook with inline strings."""

    sheets = [(name[:31], rows) for name, rows in tables.items()]
    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", _content_types(len(sheets)))
        archive.writestr("_rels/.rels", _root_relationships())
        archive.writestr("xl/workbook.xml", _workbook_xml(sheets))
        archive.writestr("xl/_rels/workbook.xml.rels", _workbook_relationships(len(sheets)))
        archive.writestr("xl/styles.xml", _styles_xml())
        for index, (_, rows) in enumerate(sheets, 1):
            archive.writestr(f"xl/worksheets/sheet{index}.xml", _sheet_xml(rows))
    return output.getvalue()


def _csv_text(rows: list[dict[str, Any]]) -> str:
    buffer = StringIO(newline="")
    columns = _columns(rows)
    writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _cell(value) for key, value in row.items()})
    return buffer.getvalue()


def _sheet_xml(rows: list[dict[str, Any]]) -> str:
    columns = _columns(rows)
    all_rows = [dict(zip(columns, columns)), *rows]
    xml_rows = []
    for row_number, row in enumerate(all_rows, 1):
        cells = []
        for column_number, key in enumerate(columns, 1):
            reference = f"{_column_name(column_number)}{row_number}"
            value = _cell(row.get(key, ""))
            cells.append(f'<c r="{reference}" t="inlineStr"><is><t>{escape(value)}</t></is></c>')
        xml_rows.append(f'<row r="{row_number}">{"".join(cells)}</row>')
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' \
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">' \
        f'<sheetData>{"".join(xml_rows)}</sheetData></worksheet>'


def _columns(rows: list[dict[str, Any]]) -> list[str]:
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    return columns or ["report_id"]


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _column_name(number: int) -> str:
    result = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _content_types(count: int) -> str:
    overrides = "".join(
        f'<Override PartName="/xl/worksheets/sheet{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for i in range(1, count + 1)
    )
    return '<?xml version="1.0" encoding="UTF-8"?>' \
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">' \
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>' \
        '<Default Extension="xml" ContentType="application/xml"/>' \
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>' \
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>' \
        f'{overrides}</Types>'


def _root_relationships() -> str:
    return '<?xml version="1.0" encoding="UTF-8"?>' \
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' \
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>' \
        '</Relationships>'


def _workbook_xml(sheets: list[tuple[str, list[dict[str, Any]]]]) -> str:
    entries = "".join(
        f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>'
        for index, (name, _) in enumerate(sheets, 1)
    )
    return '<?xml version="1.0" encoding="UTF-8"?>' \
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">' \
        f'<sheets>{entries}</sheets></workbook>'


def _workbook_relationships(count: int) -> str:
    entries = "".join(
        f'<Relationship Id="rId{i}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{i}.xml"/>'
        for i in range(1, count + 1)
    )
    entries += f'<Relationship Id="rId{count + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
    return '<?xml version="1.0" encoding="UTF-8"?>' \
        f'<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{entries}</Relationships>'


def _styles_xml() -> str:
    return '<?xml version="1.0" encoding="UTF-8"?>' \
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">' \
        '<fonts count="1"><font/></fonts><fills count="1"><fill/></fills><borders count="1"><border/></borders>' \
        '<cellStyleXfs count="1"><xf/></cellStyleXfs><cellXfs count="1"><xf xfId="0"/></cellXfs>' \
        '</styleSheet>'
