"""Tests for replay_records historical replay behavior."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import packages.record_store.paths as record_paths
from scripts import replay_records


def test_metadata_precedence_prefers_cli_over_metadata_then_inference(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    text_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-07__inferred_branch__sample.txt",
        _sales_report_text(),
    )
    _write_json(
        text_path.with_suffix(".meta.json"),
        {
            "branch_hint": "meta-branch",
            "received_at": "2026-04-07T11:00:00Z",
            "detected_report_type": "staff_attendance",
            "source": "whatsapp",
            "sender": "meta-sender",
        },
    )
    record = replay_records._load_archived_record(text_path)
    args = argparse.Namespace(
        branch="cli-branch",
        report_type="sales",
        date=None,
        source="raw",
    )

    resolved = replay_records._resolve_replay_metadata(record=record, args=args)

    assert resolved.branch_hint == "cli-branch"
    assert resolved.received_at == "2026-04-07T11:00:00Z"
    assert resolved.report_type == "sales"
    assert resolved.sender == "meta-sender"
    assert resolved.replay["is_replay"] is True


def test_replay_skips_structured_overwrite_without_explicit_flag(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-07__waigani__sample.txt",
        _sales_report_text(),
    )
    existing_structured_path = (
        tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    )
    _write_json(existing_structured_path, {"preserve": "existing"})

    exit_code = replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "orchestrator",
            "--path",
            str(raw_path),
        ]
    )

    assert exit_code == 0
    assert _read_json(existing_structured_path) == {"preserve": "existing"}
    manifest = _latest_manifest(tmp_path)
    assert manifest["results"][0]["status"] == "skipped"
    assert manifest["results"][0]["reason"] == "structured_exists_use_overwrite"


def test_rejected_replay_writes_new_rejected_copy_without_overwriting_old_one(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    rejected_path = _write_text(
        tmp_path / "records" / "rejected" / "whatsapp" / "unknown" / "20260407T004814027636__unknown__unknown_report_type.txt",
        "Shift note only. Please call me when you arrive.",
    )
    _write_json(
        rejected_path.with_suffix(".meta.json"),
        {
            "attempted_report_type": "unknown",
            "received_at": "2026-04-07T11:05:00Z",
            "sender": "rejected-smoke",
            "source": "whatsapp",
        },
    )

    exit_code = replay_records.main(
        [
            "--source",
            "rejected",
            "--mode",
            "orchestrator",
            "--path",
            str(rejected_path),
        ]
    )

    assert exit_code == 0
    rejected_files = sorted((tmp_path / "records" / "rejected" / "whatsapp" / "unknown").glob("*.txt"))
    assert len(rejected_files) == 2
    assert rejected_files[0].name == rejected_path.name
    new_meta = _read_json(rejected_files[1].with_suffix(".meta.json"))
    assert new_meta["original_rejected_path"] == "records/rejected/whatsapp/unknown/20260407T004814027636__unknown__unknown_report_type.txt"
    manifest = _latest_manifest(tmp_path)
    assert manifest["summary"]["rejected"] == 1
    assert manifest["results"][0]["status"] == "rejected"


def test_specialist_mode_fails_clearly_without_valid_report_type_mapping(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "2026-04-07__unknown__sample.txt",
        "Unclassified operational note.",
    )

    exit_code = replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "specialist",
            "--path",
            str(raw_path),
        ]
    )

    assert exit_code == 1
    manifest = _latest_manifest(tmp_path)
    assert manifest["results"][0]["status"] == "failed"
    assert "valid mapped report type" in manifest["results"][0]["reason"]


def test_manifest_is_produced_with_correct_summary_counts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "sales" / "2026-04-07__waigani__sample.txt",
        _sales_report_text(),
    )

    exit_code = replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "orchestrator",
            "--path",
            str(raw_path),
            "--dry-run",
        ]
    )

    assert exit_code == 0
    manifest = _latest_manifest(tmp_path)
    assert manifest["mode"] == "orchestrator"
    assert manifest["source"] == "raw"
    assert manifest["filters"]["dry_run"] is True
    assert manifest["summary"] == {
        "scanned": 1,
        "replayed": 1,
        "written": 0,
        "rejected": 0,
        "skipped": 1,
        "failed": 0,
    }


def test_orchestrator_replay_of_live_staff_performance_raw_file_writes_structured_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "2026-04-07__unknown__f10f29da4705.txt",
        _live_staff_performance_text(),
    )
    _write_json(
        raw_path.with_suffix(".meta.json"),
        {
            "branch_hint": None,
            "detected_report_type": "unknown",
            "routing_target": None,
            "received_at": "2026-04-07T09:43:59Z",
            "sender": "Wesley",
            "source": "whatsapp",
        },
    )

    exit_code = replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "orchestrator",
            "--path",
            str(raw_path),
        ]
    )

    assert exit_code == 0
    structured_path = tmp_path / "records" / "structured" / "hr_performance" / "lae_malaita" / "2026-04-07.json"
    assert structured_path.exists()
    structured = _read_json(structured_path)
    assert structured["source_agent"] == "staff_performance_agent"
    assert structured["branch"] == "lae_malaita"
    assert structured["report_date"] == "2026-04-07"
    assert structured["status"] == "accepted_with_warning"
    assert structured["metrics"]["price_room_staff_count"] == 5
    assert structured["metrics"]["special_assignment_count"] == 1

    manifest = _latest_manifest(tmp_path)
    assert manifest["results"][0]["status"] == "structured_written"
    assert manifest["results"][0]["agent"] == "staff_performance_agent"


def test_orchestrator_replay_of_mixed_raw_file_writes_multiple_structured_records(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "2026-04-07__unknown__mixedsample.txt",
        _mixed_sales_and_performance_text(),
    )
    _write_json(
        raw_path.with_suffix(".meta.json"),
        {
            "branch_hint": "waigani",
            "detected_report_type": "unknown",
            "routing_target": None,
            "received_at": "2026-04-07T13:00:00Z",
            "sender": "mixed-smoke",
            "source": "whatsapp",
        },
    )

    exit_code = replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "orchestrator",
            "--path",
            str(raw_path),
        ]
    )

    assert exit_code == 0
    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    performance_path = tmp_path / "records" / "structured" / "hr_performance" / "waigani" / "2026-04-07.json"
    assert sales_path.exists()
    assert performance_path.exists()

    manifest = _latest_manifest(tmp_path)
    assert manifest["results"][0]["status"] == "structured_written"
    assert manifest["results"][0]["agent"] == "orchestrator_agent"
    assert manifest["results"][0]["written_count"] == 2
    assert manifest["results"][0]["segment_count"] == 2
    assert len(manifest["results"][0]["output_paths"]) == 2
    assert manifest["results"][0]["derived_output_paths"] == [
        "records/structured/sales_income/waigani/2026-04-07.json",
        "records/structured/hr_performance/waigani/2026-04-07.json",
    ]
    segments = manifest["results"][0]["segments"]
    assert len(segments) == 2
    assert segments[0]["report_family"] == "sales_income"
    assert segments[0]["branch"] == "waigani"
    assert segments[0]["report_date"] == "2026-04-07"
    assert segments[0]["output_paths"] == ["records/structured/sales_income/waigani/2026-04-07.json"]
    assert segments[1]["report_family"] == "staff_performance"
    assert segments[1]["branch"] == "waigani"
    assert segments[1]["report_date"] == "2026-04-07"
    assert segments[1]["output_paths"] == ["records/structured/hr_performance/waigani/2026-04-07.json"]
    assert manifest["summary"]["written"] == 2
    assert not (
        tmp_path / "records" / "structured" / "sales_income" / "ttc_waigani_branch" / "2026-04-07.json"
    ).exists()
    sales_payload = _read_json(sales_path)
    assert sales_payload["branch"] == "waigani"


def test_orchestrator_replay_of_sales_and_supervisor_control_mixed_file_writes_two_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_replay_environment(monkeypatch, tmp_path)
    raw_path = _write_text(
        tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "2026-04-07__unknown__mixedsupervisor.txt",
        _mixed_sales_and_supervisor_control_text(),
    )
    _write_json(
        raw_path.with_suffix(".meta.json"),
        {
            "branch_hint": "waigani",
            "detected_report_type": "unknown",
            "routing_target": None,
            "received_at": "2026-04-07T13:00:00Z",
            "sender": "mixed-supervisor",
            "source": "whatsapp",
        },
    )

    exit_code = replay_records.main(
        [
            "--source",
            "raw",
            "--mode",
            "orchestrator",
            "--path",
            str(raw_path),
        ]
    )

    assert exit_code == 0
    sales_path = tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-04-07.json"
    supervisor_path = tmp_path / "records" / "structured" / "supervisor_control" / "waigani" / "2026-04-07.json"
    assert sales_path.exists()
    assert supervisor_path.exists()

    manifest = _latest_manifest(tmp_path)
    assert manifest["results"][0]["status"] == "structured_written"
    assert manifest["results"][0]["agent"] == "orchestrator_agent"
    assert len(manifest["results"][0]["output_paths"]) == 2


def _patch_replay_environment(monkeypatch, tmp_path: Path) -> None:
    records_dir = tmp_path / "records"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setattr(replay_records, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(replay_records, "LOGS_REPLAY_DIR", tmp_path / "logs" / "replay")


def _sales_report_text() -> str:
    return "\n".join(
        [
            "DAY-END SALES REPORT",
            "Branch: Waigani Branch",
            "Date: 07/04/2026",
            "Gross Sales: 1200",
            "Cash Sales: 600",
            "Eftpos Sales: 600",
            "Till Total: 600",
            "Deposit Total: 600",
            "Traffic: 12",
            "Served: 9",
            "Labor Hours: 4",
        ]
    )


def _live_staff_performance_text() -> str:
    return "\n".join(
        [
            "TTC LAE MALAITA BRANCH",
            "TUESDAY 07 /04/26",
            "",
            "➡️STAFF PERFORMANCE REPORT ",
            "",
            "1..Debra Aegobi -Off",
            "SECTION.. Kids Girl shirt, Baby Overall, kids Girls and Baby pants.",
            "🔹Total items moved (-)",
            "🔹Assist  (-)",
            "",
            "2..Rodah Paku - 4",
            "SECTION..Kids Girls Dress, Jumpsuit, kids Polo t-shirt",
            "🔹Total items Moved (51)",
            "🔹Assist (07)",
            "",
            "3..Julie Yorkie- 5 (Cashier )",
            "SECTION (Vacant).. Shoe Shop- Shoes, Handbags, Shopping bags",
            "🔹Total items moved (-)",
            "🔹Assist(-)",
            "",
            "4..Rodah  Frank - 5",
            "SECTION.. Price Room - Sales Tally",
            "🔹Total items moved (-)",
            "🔹Assist (-)",
            "",
            "5..Jesina Poknga -5",
            "SECTION.. Ladies Jeans, Rip Jeans,Skinny Jeans",
            "🔹Total items moved (37)",
            "🔹Assist(14)",
            "",
            "6..Nathan Moti - Sick",
            "SECTION.. , Beach wear sports wear,Jackets",
            "🔹Total items moved (-)",
            "🔹Assist (-)",
            "",
            "7.Matthew Manu -4",
            "SECTION.. Reflectors, workwear, Men's button shirt, Socks",
            "🔹Total items moved (24)",
            "🔹Assist (19)",
            "",
            "8.. Pison Orie -Off",
            "SECTION.. Ladies Tshirt, Ladies Long Dress, Crop Top, Singlet",
            "🔹Total items moved (-)",
            "🔹Assist(-)",
            "",
            "9...Herish Waizepa - 4",
            "SECTION.. Men's Jeans, Camouflage, Kids Girls Jeans",
            "🔹Total items moved (30)",
            "🔹Assist (12)",
            "",
            "10..Medlyn Sehamo - Off",
            "SECTION.. Men's T-shirt, Household Rummage",
            "🔹Total items moved (-)",
            "🔹Assist(-)",
            "",
            "11.Movzii Tuwasa -Off",
            "SECTION.. Kids Boy Pants, Kids Shorts, Comforter",
            "🔹Total items moved (-)",
            "🔹Assist (-)",
            "",
            "12 ..Amos Waizepo - 4",
            "SECTION.. Ladies Jackets,Teregal Dress, HHR",
            "🔹Total items moved (31)",
            "🔹Assist (11)",
            "",
            "13 . Samson Billy - 5",
            "SECTION.. Door Man (Made sales for items on display at doorway)",
            "🔹Total items moved (-)",
            "🔹Assist (-)",
            "",
            "15. Tabitha Lonobin -5",
            " SECTION - Men's Tshirt,Jackets",
            "Items: 29",
            "Asssist: 07",
            "",
            "16.Shandy Essau - Off",
            "SECTION. Ladies Silk Blouse, Ladies T-Shirt,Crop top, Ladies Skirt",
            "Item:-",
            "Assist: -",
            "",
            "17.Dorish Molong -3",
            "SECTION. Ladies Cotton Capri, Ladies Colour Jeans, Cotton Pants",
            "Items:31",
            "Assist: 13",
            "",
            "18.Gizard Joe - 4",
            "SECTION, Men's Shorts",
            "Items: 25",
            "Assist: 12",
            "",
            "19.Nason Mapia -5",
            "SECTION. Ladies Jeans, Men's Cotton Pants, Ladies Leggings",
            "Items: 11",
            "Assists:07",
            "",
            "20.Julie Yorkie (Cashier)(Slow moving bale- special price) Pricing- Rhoda Frank",
            "Items Sold: -",
            "",
            "Staff who work in price room:",
            "1.Kerry Iki",
            "2.Abilen Yawano",
            "3.Willmah Langa",
            "4.Rhoda Frank (Work on slow moving bale)",
            "5.Renate Norman-- Till Assistant",
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


def _mixed_sales_and_supervisor_control_text() -> str:
    return "\n".join(
        [
            "Branch: Waigani Branch",
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
            "SUPERVISOR CONTROL REPORT",
            "Floor Check: Passed",
            "Cashier Reconciled: Yes",
            "- Front door display checked",
        ]
    )


def _latest_manifest(tmp_path: Path) -> dict[str, object]:
    manifest_paths = sorted((tmp_path / "logs" / "replay").glob("*.json"))
    assert manifest_paths
    return _read_json(manifest_paths[-1])


def _write_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))
