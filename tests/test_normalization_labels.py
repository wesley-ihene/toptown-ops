"""Focused tests for shared label normalization."""

from __future__ import annotations

from packages.normalization.labels import internal_field_name, normalize_label


def test_sales_labels_normalize_live_whatsapp_aliases() -> None:
    assert normalize_label("DAY end sales report", report_family="sales").normalized_value == "DAY-END SALES REPORT"
    assert normalize_label("Main Door", report_family="sales").normalized_value == "Traffic"
    assert normalize_label("Customers Served", report_family="sales").normalized_value == "Served"
    assert normalize_label("Gross Sales", report_family="sales").normalized_value == "Total_Sales"
    assert internal_field_name("Gross Sales", report_family="sales") == "gross_sales"


def test_attendance_labels_normalize_status_aliases() -> None:
    assert normalize_label("Present", report_family="attendance").normalized_value == "P"
    assert normalize_label("Off", report_family="attendance").normalized_value == "OFF"
    assert normalize_label("Leave", report_family="attendance").normalized_value == "LEAVE"


def test_bale_labels_normalize_qty_and_amount_aliases() -> None:
    assert normalize_label("Quantity", report_family="bale_summary").normalized_value == "Qty"
    assert normalize_label("Value", report_family="bale_summary").normalized_value == "Amount"
