"""Focused tests for the pricing stock release agent."""

from __future__ import annotations

import json
from pathlib import Path

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

    assert result.payload["status"] == "ready"
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-10"
    assert result.payload["metrics"]["total_qty"] == 10
    assert result.payload["metrics"]["total_amount"] == 100.0
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

    assert result.payload["status"] == "needs_review"
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-04"
    assert result.payload["metrics"]["bales_processed"] == 2
    assert result.payload["metrics"]["total_qty"] == 309
    assert result.payload["metrics"]["total_amount"] == 6140.0
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

    assert result.payload["status"] == "needs_review"
    assert result.payload["branch"] == "bena_road"
    assert result.payload["report_date"] == "2026-04-09"
    assert result.payload["metrics"]["bales_processed"] == 5
    assert result.payload["metrics"]["total_qty"] == 967
    assert result.payload["metrics"]["total_amount"] == 5246.0
    assert result.payload["provenance"] == {
        "prepared_by": "Elise Kiea",
        "role": "Pricing Clerk",
    }
    assert [item["qty"] for item in result.payload["items"]] == [112, 290, 283, 183, 99]
    assert [item["amount"] for item in result.payload["items"]] == [1147.0, 1823.0, 866.0, 663.0, 747.0]
    assert structured_path.exists()


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
