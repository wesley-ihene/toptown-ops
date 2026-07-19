"""Focused tests for the pricing stock release agent."""

from __future__ import annotations

import json
from pathlib import Path

from apps.conversation_router import route_conversation_response
from apps.response_engine import render_whatsapp_response
from apps.pricing_stock_release_agent.parser import _parse_amount, parse_work_item
from apps.pricing_stock_release_agent.worker import process_work_item
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from packages.signal_contracts.work_item import WorkItem


def test_pricing_stock_release_agent_normalizes_bale_input(tmp_path: Path, monkeypatch) -> None:
    outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "Branch: TTC LAE 5TH STREET BRANCH",
                            "Date: Friday, 10/04 /26",
                            "Prepared By: Maria Sine - Supervisor",
                            "# 01. OSH",
                            "Quantity: (4)",
                            "Value: K40",
                            "# 02. Jeans",
                            "Qty: 6",
                            "Amt: K60",
                            "Two (02) bales processed",
                            "Two (02) bales released",
                            "Total Quantity: 10",
                            "Total Amount: K100",
                        ]
                    )
                },
            },
        )
    )

    structured_path = tmp_path / "records" / "structured" / "pricing_stock_release" / "lae_5th_street" / "2026-04-10.json"

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-10"
    assert result.payload["metrics"]["total_qty"] == 10
    assert result.payload["metrics"]["total_amount"] == 100.0
    assert result.payload["confidence"] >= 0.88
    assert {warning["code"] for warning in result.payload["warnings"]} == {"missing_provenance"}
    assert structured_path.exists()
    assert len(sorted(outbox_path.glob("*.json"))) == 1

    payload = json.loads(structured_path.read_text(encoding="utf-8"))
    assert payload["branch"] == "lae_5th_street"
    assert payload["report_date"] == "2026-04-10"


def test_pricing_stock_release_agent_parses_compact_whatsapp_bale_rows(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "📦 DAILY BALE SUMMARY – RELEASED TO RAIL",
                            "",
                            "Branch:Lae 5th street shop ",
                            "Date: Saturday 04/04/2026",
                            "",
                            "Bale #\tItem Name\tTotaQty (pcs)\tTotal Amount (K)",
                            "",
                            "1. Cargo Shorts (40KG) ",
                            "(102)--K4,126.00",
                            "",
                            "2. Men’s T-shirt ss 40kg",
                            "(207)--K2,014.00",
                            "",
                            "Total Bales on Rail: 2",
                            "●Total Quantity: 309",
                            "●Total Amount:K6,140.00",
                            "",
                            "Prepared by : Joyce",
                            "",
                            "Thanks",
                        ]
                    )
                },
            },
        )
    )

    structured_path = tmp_path / "records" / "structured" / "pricing_stock_release" / "lae_5th_street" / "2026-04-04.json"

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-04"
    assert result.payload["metrics"]["bales_processed"] == 2
    assert result.payload["metrics"]["total_qty"] == 309
    assert result.payload["metrics"]["total_amount"] == 6140.0
    assert result.payload["warnings"] == [
        {
            "code": "format_cleanup",
            "severity": "warning",
            "message": "One or more bale rows required safe currency or quantity format cleanup before parsing.",
        },
        {
            "code": "missing_provenance",
            "severity": "warning",
            "message": "Prepared By or Checked By could not be fully extracted from the bale summary.",
        }
    ]
    assert result.payload["confidence"] >= 0.88
    assert result.payload["items"] == [
        {
            "amount": 4126.0,
            "bale_id": "1",
            "item_name": "Cargo Shorts (40KG)",
            "price_per_piece": 40.45,
            "qty": 102,
        },
        {
            "amount": 2014.0,
            "bale_id": "2",
            "item_name": "Men’s T-shirt ss 40kg",
            "price_per_piece": 9.73,
            "qty": 207,
        },
    ]
    assert structured_path.exists()


def test_pricing_stock_release_agent_parses_messy_labeled_bale_rows(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "📦 DAILY BALE SUMMARY – RELEASED TO RAIL",
                            "",
                            "Branch: TTC Bena Road Goroka ",
                            "Day:Thursday ",
                            "Date: 09/04/26",
                            "",
                            "Bale #\tItem Name\tTotal Qty (pcs)\tTotal Amount (K)",
                            "",
                            "#01.Boy Pants 40kg ",
                            "(Qty:112)",
                            "Amt.K1,147.00",
                            "",
                            "#02.Boy T-shirt ss",
                            "(QTY, :290pcs).",
                            "Amt.K1,823.00",
                            "",
                            "#03.Ladies Fashion T-shirt 40kg ",
                            "(Qty,:283pcs)",
                            "Amt.K866.00",
                            "",
                            "#04.Ladies silk blouse 40kg ",
                            "(Qty:183pcs)",
                            "Amt.K663.00",
                            ".",
                            "#05 Table Clothes 40kg ",
                            "(Qty:99pcs)",
                            "Amt. K747.00",
                            "",
                            "Total quantity:967pce",
                            "Total Amount: K5,246.00",
                            "",
                            "Note: 3Bales released for sales and 2bales yet to release. ",
                            "",
                            "Prepared by: Elise Kiea(Pricing Clerk)",
                            "",
                            "Thanks.",
                        ]
                    )
                },
            },
        )
    )

    structured_path = tmp_path / "records" / "structured" / "pricing_stock_release" / "bena_road" / "2026-04-09.json"

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["branch"] == "bena_road"
    assert result.payload["report_date"] == "2026-04-09"
    assert result.payload["metrics"]["bales_processed"] == 5
    assert result.payload["metrics"]["bales_released"] == 3
    assert result.payload["metrics"]["total_qty"] == 967
    assert result.payload["metrics"]["total_amount"] == 5246.0
    assert result.payload["metrics"]["release_ratio"] == 0.6
    assert result.payload["provenance"] == {
        "prepared_by": "Elise Kiea",
        "role": "Pricing Clerk",
    }
    assert result.payload["warnings"] == [
        {
            "code": "format_cleanup",
            "severity": "warning",
            "message": "One or more bale rows required safe currency or quantity format cleanup before parsing.",
        },
        {
            "code": "missing_provenance",
            "severity": "warning",
            "message": "Prepared By or Checked By could not be fully extracted from the bale summary.",
        },
        {
            "code": "low_release_ratio",
            "severity": "warning",
            "message": "Release ratio is below the safe review threshold.",
        },
    ]
    assert result.payload["confidence"] >= 0.88
    assert [item["qty"] for item in result.payload["items"]] == [112, 290, 283, 183, 99]
    assert [item["amount"] for item in result.payload["items"]] == [1147.0, 1823.0, 866.0, 663.0, 747.0]
    assert structured_path.exists()


def test_pricing_parser_consumes_each_bale_block_without_reprocessing_lines() -> None:
    parsed = parse_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAILY BALE SUMMARY - RELEASED TO RAIL",
                            "Branch: TTC Bena Road Goroka",
                            "Date: 01/05/26",
                            "#01. OSH 45kg",
                            "(Qty:255)",
                            "Amt: K2,550.00",
                            "#02. Jeans 40kg",
                            "(Qty:188)",
                            "Amt: K1,880.00",
                            "#03. Bags 20kg",
                            "(Qty:144)",
                            "Amt: K1,443.00",
                            "#04. Shoes 10kg",
                            "(Qty:110)",
                            "Amt: K990.00",
                            "Total Qty: 697",
                            "Total Amount: K6,863.00",
                            "Prepared By: Maria - Supervisor",
                            "Checked By: Peter - Area Supervisor",
                        ]
                    )
                },
            },
        )
    )

    assert parsed.branch == "bena_road"
    assert parsed.report_date == "2026-05-01"
    assert len(parsed.items) == 4
    assert [item.qty for item in parsed.items] == [255, 188, 144, 110]
    assert [item.amount for item in parsed.items] == [2550.0, 1880.0, 1443.0, 990.0]
    assert parsed.declared_total_qty == 697
    assert parsed.declared_total_amount == 6863.0
    assert parsed.warnings == []


def test_pricing_parser_parses_apostrophe_thousand_separator_amounts() -> None:
    assert _parse_amount("K972.00") == 972.0
    assert _parse_amount("K1'267.00") == 1267.0
    assert _parse_amount("K1'168.0") == 1168.0
    assert _parse_amount("K3'456.00") == 3456.0
    assert _parse_amount("K6'86300") == 6863.0


def test_pricing_stock_release_agent_supports_bena_road_01_05_26_real_sample(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAILY BALE SUMMARY - RELEASED TO RAIL",
                            "Branch: TTC Bena Road Goroka",
                            "Date: 01/05/26",
                            "Prepared By: Maria - Supervisor",
                            "#01. OSH 45kg",
                            "Qty: 255pcs",
                            "Amt: K1'267.00",
                            "#02. Jeans 40kg",
                            "Qty: 188 pcs",
                            "Amt: K1'168.0",
                            "#03. Bags 20kg",
                            "Qty: 144",
                            "Amt: K3'456.00",
                            "#04. Shoes 10kg",
                            "Qty: 110",
                            "Amt: K972.00",
                            "Total Qty: 697 pcs",
                            "Total Amount: K6'86300",
                        ]
                    )
                },
            },
        )
    )

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["branch"] == "bena_road"
    assert result.payload["report_date"] == "2026-05-01"
    assert len(result.payload["items"]) == 4
    assert result.payload["metrics"]["total_qty"] == 697
    assert result.payload["metrics"]["total_amount"] == 6863.0
    assert [item["amount"] for item in result.payload["items"]] == [1267.0, 1168.0, 3456.0, 972.0]
    assert result.payload["warnings"] == [
        {
            "code": "format_cleanup",
            "severity": "warning",
            "message": "One or more bale rows required safe currency or quantity format cleanup before parsing.",
        },
        {
            "code": "missing_provenance",
            "severity": "warning",
            "message": "Prepared By or Checked By could not be fully extracted from the bale summary.",
        }
    ]
    assert result.payload["confidence"] >= 0.88


def test_pricing_stock_release_agent_supports_lae_5th_street_30_04_26_total_block_real_sample(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAILY BALE SUMMARY - RELEASED TO RAIL",
                            "Branch: TTC LAE 5TH STREET BRANCH",
                            "Date: 30/04/26",
                            "#01. OSH",
                            "Qty: 469pcs",
                            "Amt: K 1,370.00",
                            "TOTAL:",
                            "Qty: 469",
                            "Amount: K 1,370.00",
                        ]
                    )
                },
            },
        )
    )

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-30"
    assert len(result.payload["items"]) == 1
    assert result.payload["metrics"]["total_qty"] == 469
    assert result.payload["metrics"]["total_amount"] == 1370.0
    assert result.payload["metrics"]["release_ratio"] == 1.0
    assert result.payload["warnings"] == [
        {
            "code": "format_cleanup",
            "severity": "warning",
            "message": "One or more bale rows required safe currency or quantity format cleanup before parsing.",
        },
        {
            "code": "missing_provenance",
            "severity": "warning",
            "message": "Prepared By or Checked By could not be fully extracted from the bale summary.",
        },
    ]
    assert result.payload["confidence"] >= 0.88


def test_pricing_stock_release_agent_supports_lae_5th_street_01_05_26_real_sample(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAILY BALE SUMMARY - RELEASED TO RAIL",
                            "Branch: TTC LAE 5TH STREET BRANCH",
                            "Date: 01/05/26",
                            "Prepared By: Joyce - Supervisor",
                            "#01. OSH",
                            "Qty: 419pcs",
                            "Amt: K2, 179.00",
                            "#02. Jeans",
                            "Qty: 73pcs",
                            "Amt: K1, 113.00",
                            "Total Qty: 492pcs",
                            "Total Amount: K3, 292.00",
                        ]
                    )
                },
            },
        )
    )

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-05-01"
    assert len(result.payload["items"]) == 2
    assert result.payload["metrics"]["total_qty"] == 492
    assert result.payload["metrics"]["total_amount"] == 3292.0
    assert [item["amount"] for item in result.payload["items"]] == [2179.0, 1113.0]
    assert result.payload["warnings"] == [
        {
            "code": "format_cleanup",
            "severity": "warning",
            "message": "One or more bale rows required safe currency or quantity format cleanup before parsing.",
        },
        {
            "code": "missing_provenance",
            "severity": "warning",
            "message": "Prepared By or Checked By could not be fully extracted from the bale summary.",
        }
    ]
    assert result.payload["confidence"] >= 0.88


def test_pricing_stock_release_agent_accepts_waigani_approval_backlog_with_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAILY BALE SUMMARY - RELEASED TO RAIL",
                            "Branch: TTC POM Waigani Branch",
                            "Date: 01/05/26",
                            "Prepared By: Moviyo Alex (Pricing Clerk)",
                            "#01. OSH 45kg",
                            "Qty: 180",
                            "Amt: K900.00",
                            "#02. Soft Toys 10kg",
                            "Qty: 150",
                            "Amt: K750.00",
                            "#03. Ladies T-shirt 39kg",
                            "Qty: 139",
                            "Amt: K695.00",
                            "Three (03) bales processed",
                            "Two (02) bales released for sales",
                            "One (01) bale waiting for approval",
                            "Total Qty: 469",
                            "Total Amount: K2,345.00",
                        ]
                    )
                },
            },
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["branch"] == "waigani"
    assert result.payload["report_date"] == "2026-05-01"
    assert result.payload["metrics"]["bales_processed"] == 3
    assert result.payload["metrics"]["bales_released"] == 2
    assert result.payload["metrics"]["bales_pending_approval"] == 1
    assert result.payload["metrics"]["release_ratio"] == 0.6667
    assert result.payload["metrics"]["total_qty"] == 469
    assert result.payload["metrics"]["total_amount"] == 2345.0
    assert result.payload["confidence"] >= 0.88
    assert "approval_backlog" in warning_codes


def test_pricing_stock_release_agent_parses_checked_by_without_missing_provenance_warning(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAILY BALE SUMMARY - RELEASED TO RAIL",
                            "Branch: TTC Bena Road Goroka",
                            "Date: 02/05/26",
                            "Prepared By: Maria - Supervisor",
                            "Checked By: Peter - Area Supervisor",
                            "#01. OSH 45kg",
                            "Qty: 100",
                            "Amt: K500.00",
                            "Total Qty: 100",
                            "Total Amount: K500.00",
                        ]
                    )
                },
            },
        )
    )

    assert result.payload["status"] == "accepted"
    assert result.payload["confidence"] >= 0.88
    assert result.payload["warnings"] == []
    assert result.payload["provenance"] == {
        "prepared_by": "Maria",
        "role": "Supervisor",
        "checked_by": "Peter",
        "checked_role": "Area Supervisor",
    }


def test_pricing_stock_release_agent_accepts_zero_bale_waigani_daily_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAILY BALE SUMMARY - RELEASED TO RAIL",
                            "Branch: WAIGANI",
                            "Date: 04/06/26",
                            "",
                            "TOTAL",
                            "Total Qty: 0",
                            "Total Amount: K0.00",
                            "",
                            "Bale Release To Rail Count: 0",
                            "Pending Count: 0",
                            "",
                            "Note:",
                            "No bales were broken today because stock was not available during operating hours.",
                        ]
                    )
                },
            },
        )
    )

    assert result.payload["status"] == "accepted_with_warning"
    assert result.payload["branch"] == "waigani"
    assert result.payload["report_date"] == "2026-06-04"
    assert result.payload["items"] == []
    assert result.payload["metrics"] == {
        "bales_processed": 0,
        "bales_released": 0,
        "bales_pending_approval": 0,
        "total_qty": 0,
        "total_amount": 0.0,
        "release_ratio": 0.0,
    }
    assert result.payload["warnings"] == [
        {
            "code": "no_bale_activity",
            "severity": "warning",
            "message": "Report declares no bale activity for the reporting day.",
        },
        {
            "code": "missing_provenance",
            "severity": "warning",
            "message": "Prepared By or Checked By could not be fully extracted from the bale summary.",
        },
    ]
    assert "no_bale_activity" in result.metadata["validation"]["details"]["validation_reasons"]


def test_pricing_stock_release_agent_supersedes_existing_bale_record_for_full_correction(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    original = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "ingress_envelope": {"payload": {"message_id": "wamid.bale-original", "sender_phone": "67570000000"}},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAILY BALE SUMMARY - RELEASED TO RAIL",
                            "Branch: TTC POM Waigani Branch",
                            "Date: 03/05/26",
                            "Prepared By: Maria - Supervisor",
                            "Checked By: Peter - Area Supervisor",
                            "#01. OSH 45kg",
                            "Qty: 100",
                            "Amt: K500.00",
                            "Total Qty: 100",
                            "Total Amount: K500.00",
                        ]
                    )
                },
            },
        )
    )

    corrected = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "ingress_envelope": {"payload": {"message_id": "wamid.bale-correction", "sender_phone": "67570000000"}},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "CORRECTION / REPLACEMENT REPORT",
                            "replaces earlier submitted",
                            "supersede previous record",
                            "retain corrected version as active report",
                            "Branch: TTC POM Waigani Branch",
                            "Date: 03/05/26",
                            "Prepared By: Maria - Supervisor",
                            "Checked By: Peter - Area Supervisor",
                            "#01. OSH 45kg",
                            "Qty: 120",
                            "Amt: K720.00",
                            "Total Qty: 120",
                            "Total Amount: K720.00",
                        ]
                    )
                },
            },
        )
    )

    structured_path = tmp_path / "records" / "structured" / "pricing_stock_release" / "waigani" / "2026-05-03.json"
    active_payload = json.loads(structured_path.read_text(encoding="utf-8"))
    audit_path = Path(str(active_payload["supersedes_record_path"]))
    audit_payload = json.loads(audit_path.read_text(encoding="utf-8"))

    assert original.payload["status"] == "accepted"
    assert corrected.payload["status"] == "accepted"
    assert active_payload["metrics"]["total_qty"] == 120
    assert active_payload["metrics"]["total_amount"] == 720.0
    assert active_payload["replacement_reason"] == "correction_replacement_report"
    assert active_payload["replacement_source_message_id"] == "wamid.bale-correction"
    assert audit_payload["metrics"]["total_qty"] == 100
    assert audit_payload["metrics"]["total_amount"] == 500.0
    assert audit_payload["superseded"] is True
    assert audit_payload["superseded_by_record_path"] == str(structured_path)


def test_correction_notice_without_item_rows_returns_specific_rejection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "CORRECTION / REPLACEMENT REPORT",
                            "Branch: TTC POM Waigani Branch",
                            "Date: 03/05/26",
                            "replaces earlier submitted",
                            "retain corrected version as active report",
                        ]
                    )
                },
            },
        )
    )

    routed = route_conversation_response(result)
    rendered = render_whatsapp_response(routed)

    assert result.payload["status"] == "invalid_input"
    assert result.metadata["validation"]["reason_codes"] == ["correction_request_requires_full_report"]
    assert routed["response_type"] == "rejected_fix_request"
    assert routed["reason"] == "correction_request_requires_full_report"
    assert "Correction request detected, but full replacement report rows are required." in rendered["response_text"]


def test_malformed_replacement_report_does_not_supersede_existing_bale_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "ingress_envelope": {"payload": {"message_id": "wamid.bale-seed", "sender_phone": "67570000000"}},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAILY BALE SUMMARY - RELEASED TO RAIL",
                            "Branch: TTC POM Waigani Branch",
                            "Date: 04/05/26",
                            "Prepared By: Maria - Supervisor",
                            "Checked By: Peter - Area Supervisor",
                            "#01. OSH 45kg",
                            "Qty: 100",
                            "Amt: K500.00",
                            "Total Qty: 100",
                            "Total Amount: K500.00",
                        ]
                    )
                },
            },
        )
    )

    malformed = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "bale_summary"},
                "ingress_envelope": {"payload": {"message_id": "wamid.bale-bad-correction", "sender_phone": "67570000000"}},
                "raw_message": {
                    "text": "\n".join(
                        [
                            "CORRECTION / REPLACEMENT REPORT",
                            "supersede previous record",
                            "Branch: TTC POM Waigani Branch",
                            "Date: 04/05/26",
                            "Prepared By: Maria - Supervisor",
                            "Checked By: Peter - Area Supervisor",
                            "#01. OSH 45kg",
                            "Qty: 120",
                            "Amt: K720.00",
                            "Total Qty: 120",
                            "Total Amount: K700.00",
                        ]
                    )
                },
            },
        )
    )

    structured_path = tmp_path / "records" / "structured" / "pricing_stock_release" / "waigani" / "2026-05-04.json"
    active_payload = json.loads(structured_path.read_text(encoding="utf-8"))

    assert malformed.payload["status"] == "needs_review"
    assert active_payload["metrics"]["total_qty"] == 100
    assert active_payload["metrics"]["total_amount"] == 500.0


def _patch_output_paths(tmp_path: Path, monkeypatch) -> Path:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    outbox_path = tmp_path / "outbox"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))
    monkeypatch.setattr("apps.pricing_stock_release_agent.worker.OUTBOX_PATH", outbox_path)
    return outbox_path
