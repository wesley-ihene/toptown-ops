"""Behavior tests for the dedicated staff performance specialist."""

from __future__ import annotations

from pathlib import Path

import packages.record_store.paths as record_paths
from packages.section_registry import resolve_section_alias
from apps.staff_performance_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem


def test_staff_performance_agent_parses_common_variants_and_status_tokens(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "raw_message": {
                    "text": "\n".join(
                        [
                            "TTC WAIGANI BRANCH",
                            "Monday 30/03/26",
                            "➡️STAFF PERFORMANCE REPORT",
                            "1..Alice Demo -Off",
                            "SECTION.. Ladies Tshirt",
                            "Item:-",
                            "Assist: -",
                            "2.Beth Cash - 5 (Cashier)",
                            "SECTION (Vacant).. Shoe Shop",
                            "🔹Total items moved (51)",
                            "Asssist: 07",
                            "3. Chris Sick - Sick",
                            "SECTION - Men's Tshirt,Jackets",
                            "Items: 29",
                            "Assists:07",
                        ]
                    )
                },
                "metadata": {"received_at": "2026-03-30T09:00:00Z"},
            },
        )
    )

    assert result.agent_name == "staff_performance_agent"
    assert result.payload["branch"] == "waigani"
    assert result.payload["report_date"] == "2026-03-30"
    assert len(result.payload["items"]) == 3

    first, second, third = result.payload["items"]
    assert first["duty_status"] == "off_duty"
    assert first["items_moved"] == 0
    assert first["section"] == "ladies_tshirt"
    assert second["performance_grade"] == 5
    assert second["role"] == "Cashier"
    assert second["section"] == "shoe_shop"
    assert second["items_moved"] == 51
    assert second["assisting_count"] == 7
    assert "section_annotation:Vacant" in second["notes"]
    assert third["duty_status"] == "sick"
    assert third["section"] == "mens_tshirt"
    assert third["items_moved"] == 29
    assert third["assisting_count"] == 7

    diagnostics = result.payload["diagnostics"]
    assert diagnostics["normalized_header_candidates"][:3] == [
        "ttc waigani branch",
        "monday 30 03 26",
        "staff performance report",
    ]
    assert result.payload["status"] == "accepted"


def test_section_registry_maps_common_aliases() -> None:
    assert resolve_section_alias("Men's Jeans").section_slug == "mens_jeans"
    assert resolve_section_alias("Ladies Jeans").section_slug == "ladies_jeans"
    assert resolve_section_alias("Men's T-shirt").section_slug == "mens_tshirt"
    assert resolve_section_alias("Men's Tshirt").section_slug == "mens_tshirt"
    assert resolve_section_alias("Men's Shorts").section_slug == "mens_shorts"
    assert resolve_section_alias("Ladies Cotton Capri").section_slug == "ladies_cotton_capri"
    assert resolve_section_alias("Price Room - Sales Tally").section_slug == "pricing_room_sales_tally"
    assert resolve_section_alias("Door Man").section_slug == "door_guard"
    assert resolve_section_alias("Doorman").section_slug == "door_guard"
    assert resolve_section_alias("Household Rummage").section_slug == "household_rummage"
    assert resolve_section_alias("HHR").section_slug == "household_rummage"


def test_staff_performance_agent_extracts_price_room_and_special_assignments_from_live_sample(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "raw_message": {"text": _live_staff_performance_text()},
                "metadata": {"received_at": "2026-04-07T09:43:59Z"},
            },
        )
    )

    payload = result.payload
    assert payload["status"] == "accepted_with_warning"
    assert payload["confidence"] == 0.97
    assert payload["metrics"]["price_room_staff_count"] == 5
    assert payload["metrics"]["special_assignment_count"] == 1
    assert payload["metrics"]["resolved_section_count"] >= 15
    assert payload["metrics"]["unresolved_section_count"] == 2

    items_by_number = {item["record_number"]: item for item in payload["items"]}
    assert items_by_number[4]["section"] == "pricing_room_sales_tally"
    assert items_by_number[5]["section"] == "ladies_jeans"
    assert items_by_number[9]["section"] == "mens_jeans"
    assert items_by_number[10]["section"] == "mens_tshirt"
    assert items_by_number[17]["section"] == "ladies_cotton_capri"
    assert items_by_number[18]["section"] == "mens_shorts"

    price_room_staff = payload["price_room_staff"]
    assert price_room_staff[0] == {"name": "Kerry Iki", "role": None, "notes": []}
    assert price_room_staff[3] == {
        "name": "Rhoda Frank",
        "role": None,
        "notes": ["Work on slow moving bale"],
    }
    assert price_room_staff[4] == {
        "name": "Renate Norman",
        "role": "Till Assistant",
        "notes": [],
    }

    special_assignment = payload["special_assignments"][0]
    assert special_assignment["staff_name"] == "Julie Yorkie"
    assert special_assignment["role"] == "Cashier"
    assert special_assignment["assignment_type"] == "slow_moving_bale_special_price"
    assert special_assignment["pricing_by"] == "Rhoda Frank"
    assert special_assignment["items_sold"] is None

    diagnostics = payload["diagnostics"]
    assert diagnostics["price_room_staff_count"] == 5
    assert diagnostics["special_assignment_count"] == 1
    assert diagnostics["section_resolution_stats"]["resolved_count"] >= 15
    assert diagnostics["unmatched_lines"] == []
    assert payload["review_policy"]["final_status"] == "accepted_with_warning"


def _live_staff_performance_text() -> str:
    from tests.test_orchestrator_agent import _live_staff_performance_text as source_text

    return source_text()


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
