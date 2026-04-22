"""Unit tests for deterministic pre-ingestion validation."""

from __future__ import annotations

from apps.pre_ingestion_validator import validate_inbound_text


def test_empty_input_is_rejected() -> None:
    result = validate_inbound_text("")

    assert result["status"] == "rejected"
    assert result["cleaned_text"] == ""
    assert _reason_codes(result) == ["empty_input"]


def test_whitespace_only_input_is_rejected() -> None:
    result = validate_inbound_text(" \t\r\n ")

    assert result["status"] == "rejected"
    assert result["cleaned_text"] == ""
    assert _reason_codes(result)[-1] == "empty_input"


def test_crlf_is_normalized() -> None:
    result = validate_inbound_text("Header\r\nBranch: Waigani\rFooter")

    assert result["status"] == "cleaned"
    assert result["cleaned_text"] == "Header\nBranch: Waigani\nFooter"
    assert "normalized_line_endings" in _reason_codes(result)


def test_zero_width_and_control_characters_are_removed() -> None:
    result = validate_inbound_text("DAY-END\u200b SALES\u0007 REPORT")

    assert result["status"] == "cleaned"
    assert result["cleaned_text"] == "DAY-END SALES REPORT"
    assert "removed_control_characters" in _reason_codes(result)


def test_repeated_blank_lines_are_collapsed() -> None:
    result = validate_inbound_text("Line 1\n\n\nLine 2\n\n\n\nLine 3")

    assert result["status"] == "cleaned"
    assert result["cleaned_text"] == "Line 1\n\nLine 2\n\nLine 3"
    assert "collapsed_blank_lines" in _reason_codes(result)


def test_cleanup_only_input_returns_cleaned_text() -> None:
    result = validate_inbound_text("  DAY-END SALES REPORT\r\n\r\n\r\nBranch: Waigani  ")

    assert result["status"] == "cleaned"
    assert result["cleaned_text"] == "DAY-END SALES REPORT\n\nBranch: Waigani"
    assert _reason_codes(result) == [
        "normalized_line_endings",
        "trimmed_whitespace",
        "collapsed_blank_lines",
    ]


def test_mixed_report_risk_is_flagged() -> None:
    result = validate_inbound_text(
        "\n".join(
            [
                "DAY-END SALES REPORT",
                "Gross Sales: 1200",
                "",
                "SUPERVISOR CONTROL REPORT",
                "Floor Check: Passed",
            ]
        )
    )

    assert result["status"] == "accepted"
    assert result["detected_risks"] == ["mixed_report_risk"]
    assert result["warnings"] == ["Multiple strong report headers were detected in one message."]
    assert result["suggested_report_family"] is None


def test_unsupported_payload_kind_is_rejected() -> None:
    result = validate_inbound_text("", payload_kind="image")

    assert result["status"] == "rejected"
    assert _reason_codes(result) == ["unsupported_payload_kind"]


def test_validator_result_shape_is_deterministic() -> None:
    result = validate_inbound_text("DAY-END SALES REPORT")

    assert result == {
        "status": "accepted",
        "cleaned_text": "DAY-END SALES REPORT",
        "reasons": [],
        "warnings": [],
        "detected_risks": [],
        "suggested_report_family": "sales",
        "validator_version": "v1",
    }


def _reason_codes(result: dict[str, object]) -> list[str]:
    reasons = result["reasons"]
    assert isinstance(reasons, list)
    return [reason["code"] for reason in reasons]
