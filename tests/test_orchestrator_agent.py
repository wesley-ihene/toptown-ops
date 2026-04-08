"""Lifecycle tests for the upstream orchestrator audit flow."""

from __future__ import annotations

import json
from pathlib import Path

from apps.orchestrator_agent.worker import process_work_item
from packages.signal_contracts.work_item import WorkItem
import packages.record_store.paths as record_paths


def test_orchestrator_keeps_raw_archive_and_writes_structured_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {
                    "text": "\n".join(
                        [
                            "DAY-END SALES REPORT",
                            "Branch: Waigani",
                            "Date: 2026-04-07",
                            "Gross Sales: 1250",
                            "Cash Sales: 750",
                            "Eftpos Sales: 500",
                            "Traffic: 24",
                            "Served: 19",
                            "Cashier: Alice",
                        ]
                    )
                },
                "metadata": {
                    "received_at": "2026-04-07T11:00:00Z",
                    "sender": "audit-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert not (tmp_path / "records" / "raw" / "whatsapp" / "sales").exists()

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "sales_income"
    assert raw_meta["routing_target"] == "sales_income_agent"
    assert raw_meta["processing_status"] in {"ready", "needs_review"}

    structured_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    assert structured_path.exists()
    structured_payload = _read_json(structured_path)
    assert structured_payload["signal_type"] == "sales_income"
    assert structured_payload["branch"] == "waigani"
    assert structured_payload["report_date"] == "2026-04-07"

    assert result.payload["signal_type"] == "sales_income"


def test_orchestrator_routes_live_staff_performance_message_with_resolved_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _live_staff_performance_text()},
                "metadata": {
                    "received_at": "2026-04-07T09:43:59Z",
                    "sender": "Wesley",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "staff_performance"
    assert raw_meta["branch_hint"] == "lae_malaita"
    assert raw_meta["routing_target"] == "staff_performance_agent"
    assert raw_meta["resolved_report_date"] == "2026-04-07"
    assert raw_meta["raw_report_date"] == "TUESDAY 07 /04/26"
    assert raw_meta["normalized_header_candidates"][:3] == [
        "ttc lae malaita branch",
        "tuesday 07 04 26",
        "staff performance report",
    ]

    structured_path = tmp_path / "records" / "structured" / "hr_performance" / "lae_malaita" / "2026-04-07.json"
    assert structured_path.exists()
    structured_payload = _read_json(structured_path)
    assert structured_payload["source_agent"] == "staff_performance_agent"
    assert structured_payload["branch"] == "lae_malaita"
    assert structured_payload["report_date"] == "2026-04-07"
    assert len(structured_payload["items"]) >= 19
    assert structured_payload["status"] == "accepted_with_warning"
    assert structured_payload["metrics"]["price_room_staff_count"] == 5
    assert structured_payload["metrics"]["special_assignment_count"] == 1
    assert structured_payload["items"][3]["section"] == "pricing_room_sales_tally"
    assert len(structured_payload["price_room_staff"]) == 5
    assert len(structured_payload["special_assignments"]) == 1

    assert result.agent_name == "staff_performance_agent"
    assert result.payload["signal_subtype"] == "staff_performance"
    assert result.payload["branch"] == "lae_malaita"
    assert result.payload["report_date"] == "2026-04-07"
    assert result.payload["status"] == "accepted_with_warning"


def test_orchestrator_unknown_message_writes_rejected_copy_but_keeps_raw_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": "Inventory note from floor team. Please review manually."},
                "metadata": {
                    "received_at": "2026-04-07T11:05:00Z",
                    "sender": "reject-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_text_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.txt")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")

    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert len(rejected_text_paths) == 1
    assert len(rejected_meta_paths) == 1

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "unknown"
    assert raw_meta["routing_target"] is None
    assert raw_meta["processing_status"] == "needs_review"

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["rejection_reason"] == "unknown_report_type"
    assert rejected_meta["attempted_report_type"] == "unknown"

    assert result.payload["status"] == "needs_review"
    assert not (tmp_path / "records" / "structured").exists()


def test_orchestrator_invalid_input_archives_raw_before_quarantine(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": ""},
                "metadata": {
                    "received_at": "2026-04-07T11:10:00Z",
                    "sender": "invalid-smoke",
                    "branch_hint": "blank",
                },
            },
        )
    )

    raw_text_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt")
    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    rejected_text_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.txt")
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")

    assert len(raw_text_paths) == 1
    assert len(raw_meta_paths) == 1
    assert raw_text_paths[0].read_text(encoding="utf-8") == ""
    assert len(rejected_text_paths) == 1
    assert len(rejected_meta_paths) == 1

    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["processing_status"] == "invalid_input"
    assert raw_meta["detected_report_type"] == "unknown"
    assert raw_meta["routing_target"] is None

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["rejection_reason"] == "missing_raw_text"
    assert rejected_meta["attempted_agent"] is None

    assert result.payload["status"] == "invalid_input"


def test_orchestrator_replay_skips_raw_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": "Shift note only. Please call me when you arrive."},
                "metadata": {
                    "received_at": "2026-04-07T11:15:00Z",
                    "sender": "replay-smoke",
                    "branch_hint": "waigani",
                },
                "replay": {
                    "is_replay": True,
                    "source": "rejected",
                    "original_path": "records/rejected/whatsapp/unknown/example.txt",
                    "replayed_at": "2026-04-07T12:00:00Z",
                },
            },
        )
    )

    assert _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.txt") == []
    rejected_meta_paths = _paths(tmp_path / "records" / "rejected" / "whatsapp" / "unknown", "*.meta.json")
    assert len(rejected_meta_paths) == 1

    rejected_meta = _read_json(rejected_meta_paths[0])
    assert rejected_meta["replay"] is True
    assert rejected_meta["replay_source"] == "rejected"
    assert rejected_meta["replay_original_path"] == "records/rejected/whatsapp/unknown/example.txt"
    assert result.payload["status"] == "needs_review"


def test_orchestrator_mixed_report_fans_out_to_multiple_specialists(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_record_paths(monkeypatch, tmp_path)

    result = process_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "source": "whatsapp",
                "raw_message": {"text": _mixed_sales_and_performance_text()},
                "metadata": {
                    "received_at": "2026-04-07T13:00:00Z",
                    "sender": "mixed-smoke",
                    "branch_hint": "waigani",
                },
            },
        )
    )

    raw_meta_paths = _paths(tmp_path / "records" / "raw" / "whatsapp" / "unknown", "*.meta.json")
    assert len(raw_meta_paths) == 1
    raw_meta = _read_json(raw_meta_paths[0])
    assert raw_meta["detected_report_type"] == "mixed"
    assert raw_meta["routing_target"] == "fan_out"
    assert raw_meta["split_child_count"] == 2
    assert raw_meta["split_child_report_types"] == ["sales_income", "staff_performance"]
    assert raw_meta["processing_status"] == "accepted_with_warning"

    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    performance_path = tmp_path / "records" / "structured" / "hr_performance" / "waigani" / "2026-04-07.json"
    assert sales_path.exists()
    assert performance_path.exists()
    assert not (
        tmp_path / "records" / "structured" / "sales_income" / "ttc_waigani_branch" / "2026-04-07.json"
    ).exists()

    assert result.agent_name == "orchestrator_agent"
    assert result.payload["output_path"] == "records/structured/sales_income/waigani/2026-04-07.json"
    assert result.payload["output_paths"] == [
        "records/structured/sales_income/waigani/2026-04-07.json",
        "records/structured/hr_performance/waigani/2026-04-07.json",
    ]
    assert result.payload["derived_output_paths"] == result.payload["output_paths"]
    assert result.payload["segment_count"] == 2
    assert result.payload["written_count"] == 2
    assert result.payload["classification"]["report_type"] == "mixed"
    assert result.payload["routing"]["target_agent"] == "fan_out"
    assert result.payload["routing"]["split_strategy"] == "explicit_report_headers"
    assert result.payload["status"] == "accepted_with_warning"
    assert len(result.payload["outputs"]) == 2
    assert result.payload["lineage"]["message_role"] == "split_parent"
    assert len(result.payload["fanout"]["children"]) == 2
    assert [child["report_family"] for child in result.payload["fanout"]["children"]] == [
        "sales_income",
        "staff_performance",
    ]
    assert result.payload["fanout"]["children"][0]["branch"] == "waigani"
    assert result.payload["fanout"]["children"][1]["branch"] == "waigani"


def _patch_record_paths(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")


def _paths(directory: Path, pattern: str) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob(pattern))


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _live_staff_performance_text() -> str:
    return "\n".join(
        [
            "TTC LAE MALAITA BRANCH",
            "TUESDAY 07 /04/26",
            "",
            "➡️STAFF PERFORMANCE REPORT ",
            "",
            "1..Debra Aegobi -Off",
            "SECTION.. Kids Girl shirt, Baby Overall, kids Girls and Baby pants.  ",
            "🔹Total items moved (-) ",
            "🔹Assist  (-) ",
            "",
            "2..Rodah Paku - 4",
            "SECTION..Kids Girls Dress, Jumpsuit, kids Polo t-shirt ",
            "🔹Total items Moved (51) ",
            "🔹Assist (07) ",
            "",
            "3..Julie Yorkie- 5 (Cashier )",
            "SECTION (Vacant).. Shoe Shop- Shoes, Handbags, Shopping bags ",
            "🔹Total items moved (-) ",
            "🔹Assist(-)",
            "",
            "4..Rodah  Frank - 5",
            "SECTION.. Price Room - Sales Tally ",
            "🔹Total items moved (-) ",
            "🔹Assist (-) ",
            "",
            "5..Jesina Poknga -5",
            "SECTION.. Ladies Jeans, Rip Jeans,Skinny Jeans",
            "🔹Total items moved (37)",
            "🔹Assist(14)",
            "",
            "6..Nathan Moti - Sick",
            "SECTION.. , Beach wear sports wear,Jackets",
            "🔹Total items moved (-) ",
            "🔹Assist (-) ",
            "",
            "7.Matthew Manu -4",
            "SECTION.. Reflectors, workwear, Men's button shirt, Socks ",
            "🔹Total items moved (24) ",
            "🔹Assist (19) ",
            "",
            "8.. Pison Orie -Off",
            "SECTION.. Ladies Tshirt, Ladies Long Dress, Crop Top, Singlet",
            "🔹Total items moved (-) ",
            "🔹Assist(-) ",
            "",
            "9...Herish Waizepa - 4",
            "SECTION.. Men's Jeans, Camouflage, Kids Girls Jeans ",
            "🔹Total items moved (30) ",
            "🔹Assist (12) ",
            "",
            "10..Medlyn Sehamo - Off",
            "SECTION.. Men's T-shirt, Household Rummage ",
            "🔹Total items moved (-) ",
            "🔹Assist(-) ",
            "",
            "11.Movzii Tuwasa -Off",
            "SECTION.. Kids Boy Pants, Kids Shorts, Comforter",
            "🔹Total items moved (-) ",
            "🔹Assist (-)",
            "",
            "",
            "12 ..Amos Waizepo - 4",
            "SECTION.. Ladies Jackets,Teregal Dress, HHR",
            "🔹Total items moved (31) ",
            "🔹Assist (11) ",
            "",
            "13 . Samson Billy - 5",
            "SECTION.. Door Man (Made sales for items on display at doorway)",
            "🔹Total items moved (-) ",
            "🔹Assist (-) ",
            "",
            "14..Raymond Koyem -Off",
            "SECTION.. Doorman (Made sales for items on display at doorway)",
            "Items: -",
            "Assist: -",
            "",
            "15. Tabitha Lonobin -5",
            " SECTION - Men's Tshirt,Jackets",
            "Items: 29",
            "Asssist: 07",
            "",
            "16.Shandy Essau - Off",
            "SECTION. Ladies Silk Blouse, Ladies T-Shirt,Crop top, Ladies Skirt ",
            "Item:-",
            "Assist: -",
            "",
            "17.Dorish Molong -3",
            "SECTION. Ladies Cotton Capri, Ladies Colour Jeans, Cotton Pants ",
            "Items:31",
            "Assist: 13",
            "",
            "18.Gizard Joe - 4",
            "SECTION, Men's Shorts ",
            "Items: 25",
            "Assist: 12",
            "",
            "19.Nason Mapia -5",
            "SECTION. Ladies Jeans, Men's Cotton Pants, Ladies Leggings ",
            "Items: 11",
            "Assists:07",
            "",
            "20.Julie Yorkie (Cashier)(Slow moving bale- special price) Pricing- Rhoda Frank",
            "Items Sold: -",
            "",
            "Staff who work in price room:",
            "1.Kerry Iki ",
            "2.Abilen Yawano ",
            "3.Willmah Langa ",
            "4.Rhoda Frank (Work on slow moving bale)",
            "5.Renate Norman-- Till Assistant ",
            "",
            "THANKS...",
        ]
    )


def _mixed_sales_and_performance_text() -> str:
    return "\n".join(
        [
            "TTC WAIGANI BRANCH",
            "Date: 07/04/2026",
            "",
            "DAY-END SALES REPORT",
            "Gross Sales: 1200",
            "Cash Sales: 600",
            "Eftpos Sales: 600",
            "Till Total: 600",
            "Deposit Total: 600",
            "Traffic: 12",
            "Served: 9",
            "Labor Hours: 4",
            "",
            "STAFF PERFORMANCE REPORT",
            "1.Alice Demo - 5",
            "SECTION. Men's Tshirt",
            "Items: 10",
            "Assist: 2",
        ]
    )
