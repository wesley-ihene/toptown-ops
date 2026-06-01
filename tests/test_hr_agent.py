"""Focused tests for the HR agent."""

from __future__ import annotations

import json
from pathlib import Path

from apps.conversation_router import route_conversation_response
from apps.hr_agent.parser import parse_work_item as parse_hr_work_item
from apps.response_engine import render_whatsapp_response
from apps.hr_agent.worker import process_work_item
import packages.record_store.automation as record_automation
import packages.record_store.paths as record_paths
from packages.signal_contracts.agent_result import AgentResult
from packages.signal_contracts.work_item import WorkItem


def test_valid_attendance_sample_writes_one_signal_file(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "Mary Kila - Present",
                "Peter Ake - Present",
                "Lena Bina - Present",
                "Notes: Fully staffed",
            ]
        )
    )

    assert result.payload["status"] == "accepted"
    outbox_files = sorted(outbox_path.glob("*.json"))
    assert len(outbox_files) == 1
    event_path = signals_root / "waigani" / "2026-04-07" / "staff_attendance_report__waigani__2026-04-07.json"
    assert event_path.exists()

    payload = json.loads(event_path.read_text(encoding="utf-8"))
    assert payload["signal_type"] == "staff_attendance_report"
    assert payload["branch"] == "waigani"
    assert payload["report_date"] == "2026-04-07"
    assert payload["source_record_type"] == "hr_attendance"
    assert payload["payload"]["attendance_totals"]["present"] == 4
    assert len(payload["payload"]["attendance_records"]) == 4
    assert payload["warnings"] == []

    assert json.loads(outbox_files[0].read_text(encoding="utf-8")) == result.payload


def test_low_coverage_sample_becomes_operational_insight_not_review(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "Mary Kila - Present",
                "Peter Ake - Absent",
                "Lena Bina - Absent",
                "Total Staff: 4",
                "Notes: Skeleton team only",
            ]
        )
    )

    insight_codes = {insight["code"] for insight in result.payload["insights"]}

    assert result.payload["status"] == "accepted"
    assert result.payload["warnings"] == []
    assert insight_codes == {"attendance_inactive_info", "low_coverage"}
    assert result.payload["metrics"]["attendance_gap"] == 2
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (tmp_path / "records" / "structured" / "hr_attendance" / "waigani" / "2026-04-07.json").exists()
    assert (signals_root / "waigani" / "2026-04-07" / "staff_attendance_report__waigani__2026-04-07.json").exists()


def test_unknown_status_sample_raises_unknown_attendance_status(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "Peter Ake - Present",
                "Lena Bina - Present",
                "Mary Kila - Standby",
                "Notes: Skeleton team only",
            ]
        )
    )

    assert result.payload["status"] == "needs_review"
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "unknown_attendance_status" in warning_codes
    assert "low_coverage" not in warning_codes
    assert any(item["staff_name"] == "Mary Kila" and item["status"] == "unknown" for item in result.payload["items"])
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (tmp_path / "records" / "structured" / "hr_attendance" / "waigani" / "2026-04-07.json").exists()
    assert not (signals_root / "waigani" / "2026-04-07" / "staff_attendance_report__waigani__2026-04-07.json").exists()


def test_hr_agent_normalizes_branch_alias_date_and_short_statuses(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Shop: TTC LAE 5TH STREET BRANCH",
                "Date: Friday, 10/04 /26",
                "John Doe - P",
                "Mary Kila - P",
                "Peter Ake - Off",
                "Lena Bina - Leave",
                "Total Staff: (4)",
            ]
        )
    )

    insight_codes = {insight["code"] for insight in result.payload["insights"]}

    assert result.payload["status"] == "accepted"
    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    assert "unknown_attendance_status" not in warning_codes
    assert insight_codes == {"attendance_inactive_info", "low_coverage"}
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-04-10"
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (tmp_path / "records" / "structured" / "hr_attendance" / "lae_5th_street" / "2026-04-10.json").exists()
    assert (signals_root / "lae_5th_street" / "2026-04-10" / "staff_attendance_report__lae_5th_street__2026-04-10.json").exists()


def test_hr_agent_accepts_real_world_attendance_statuses_without_unknown_warning(tmp_path: Path, monkeypatch) -> None:
    signals_root, outbox_path = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - OFF",
                "Mary Kila - LEAVE BREAK",
                "Peter Ake - ABSENT WITH NOTICE",
                "Lena Bina - TRANSFER",
                "Total Staff: 4",
            ]
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    insight_codes = {insight["code"] for insight in result.payload["insights"]}
    item_statuses = {item["staff_name"]: item["status"] for item in result.payload["items"]}

    assert result.payload["status"] == "accepted"
    assert "unknown_attendance_status" not in warning_codes
    assert insight_codes == {"attendance_inactive_info", "low_coverage"}
    assert item_statuses == {
        "John Doe": "off",
        "Mary Kila": "leave",
        "Peter Ake": "awn",
        "Lena Bina": "transfer",
    }
    assert result.payload["metrics"]["present_count"] == 0
    assert result.payload["metrics"]["absent_count"] == 1
    assert result.payload["metrics"]["off_count"] == 2
    assert result.payload["metrics"]["leave_count"] == 1
    assert len(sorted(outbox_path.glob("*.json"))) == 1
    assert (tmp_path / "records" / "structured" / "hr_attendance" / "waigani" / "2026-04-07.json").exists()
    assert (signals_root / "waigani" / "2026-04-07" / "staff_attendance_report__waigani__2026-04-07.json").exists()


def test_hr_agent_accepts_normal_inactive_staff_mix_and_keeps_attendance_gap_metric(
    tmp_path: Path,
    monkeypatch,
) -> None:
    signals_root, _ = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            total_staff="18",
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Staff Alpha - Present",
                "Staff Bravo - Present",
                "Staff Charlie - Present",
                "Staff Delta - Present",
                "Staff Echo - Present",
                "Staff Foxtrot - Present",
                "Staff Golf - Present",
                "Staff Hotel - Present",
                "Staff India - Present",
                "Staff Juliet - Present",
                "Staff Kilo - Present",
                "Staff Lima - Present",
                "Staff Mike - Present",
                "Staff November - Day Off",
                "Staff Oscar - Day Off",
                "Staff Papa - Day Off",
                "Staff Quebec - Absent With Notice",
                "Staff Romeo - Lay Off",
                "Total Staff: 18",
            ],
        )
    )

    assert result.payload["status"] == "accepted"
    assert result.payload["warnings"] == []
    assert result.payload["metrics"]["attendance_gap"] == 5
    assert result.payload["metrics"]["present_count"] == 13
    assert result.payload["metrics"]["off_count"] == 4
    assert result.payload["metrics"]["absent_count"] == 1
    assert result.payload["insights"] == [
        {
            "code": "attendance_inactive_info",
            "severity": "info",
            "message": "5 staff are not active today (Day Off / Leave / Lay Off / Sick / Notice / Absent statuses).",
        }
    ]
    assert (signals_root / "waigani" / "2026-04-07" / "staff_attendance_report__waigani__2026-04-07.json").exists()


def test_hr_agent_total_staff_mismatch_still_requires_review(tmp_path: Path, monkeypatch) -> None:
    signals_root, _ = _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "Mary Kila - Present",
                "Peter Ake - Day Off",
                "Total Staff: 4",
            ]
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}

    assert result.payload["status"] == "needs_review"
    assert "declared_total_staff_mismatch" in warning_codes
    assert result.payload["validation_error_code"] == "declared_total_staff_mismatch"
    assert result.payload["validation_error_message"] == "Normalized attendance rows total 3 but TOTAL_STAFF = 4."
    assert not (signals_root / "waigani" / "2026-04-07" / "staff_attendance_report__waigani__2026-04-07.json").exists()


def test_hr_agent_declared_attendance_summary_mismatch_requires_review(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            total_staff="4",
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "Mary Kila - Present",
                "Peter Ake - Day Off",
                "Lena Bina - Leave",
                "Total Staff: 4",
                "Staff Present: 4",
                "Day Off: 0",
                "Leave: 0",
            ],
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}

    assert result.payload["status"] == "needs_review"
    assert "attendance_totals_mismatch" in warning_codes


def test_hr_agent_accepts_declared_leave_summary_alias_and_preserves_notice_section(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    present_staff = [
        "Anna Kora",
        "Benson Tali",
        "Clara Nima",
        "David Aro",
        "Elisa Tami",
        "Felix Yau",
        "Grace Nena",
        "Henry Kale",
        "Irene Pora",
        "Jonas Wari",
        "Kathy Leka",
        "Lucas Tawa",
        "Miriam Soke",
        "Noah Pari",
        "Olivia Sare",
        "Paul Tima",
        "Queenie Raka",
        "Ruth Wapi",
        "Samuel Tane",
        "Tina Yaro",
        "Ura Bina",
        "Victor Tali",
        "Wendy Kora",
        "Xavier Tame",
        "Yasmin Heni",
    ]
    report_lines = [
        "Branch: TTC Bena Road - Goroka",
        "Date: Friday 22/05/26",
        *[f"{staff_name} - Present" for staff_name in present_staff],
        "Jason Bill - Leave",
        "Summary:",
        "Total Current Staff = 26",
        "Total Staff Present = 25",
        "Total Staff Leave = 1",
        "Total Staff Day Off = 0",
        "Total Staff Late = 0",
        "Total Staff Absent = 0",
        "NOTICE:",
        "One staff resignation notice recorded separately for follow-up.",
    ]

    result = process_work_item(_attendance_work_item(lines=report_lines))

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    item_statuses = {item["staff_name"]: item["status"] for item in result.payload["items"]}

    assert result.payload["status"] == "accepted"
    assert "declared_summary_total_mismatch" not in warning_codes
    assert result.payload["branch"] == "bena_road"
    assert result.payload["report_date"] == "2026-05-22"
    assert result.payload["metrics"]["present"] == 25
    assert result.payload["metrics"]["leave"] == 1
    assert result.payload["metrics"]["total_staff"] == 26
    assert len(result.payload["items"]) == 26
    assert item_statuses["Jason Bill"] == "leave"
    assert any("resignation notice" in note.casefold() for note in result.payload["provenance"]["notes"])


def test_hr_agent_accepts_weekday_prefixed_date_and_excludes_resigned_pending_staff_from_active_total(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    present_staff = [
        "Marryane Sakias",
        "Imelda Patrick",
        "Merolyne Tobby",
        "George Andau",
        "Cloe Wofinga",
        "Doil Wai-ah",
        "Donock Levi",
        "Joyice Andrew",
        "Jackson Kuri",
        "Jennifer Golomb",
        "Sheeba I",
        "Lieb Yawano",
        "Anuty Mina",
        "Joycelyn Alu",
        "Sandra Daniel",
        "Goinake Ihene",
        "Rona Kila",
    ]
    report_lines = [
        "ATTENDANCE REPORT",
        "Branch :LAE _5th Street",
        "Friday , 22/05/26.",
        "",
        *[f"{index}.{staff_name} = P" for index, staff_name in enumerate(present_staff, start=1)],
        "18.Joyce Lovave = Resign /decision pending",
        "",
        "Summary:",
        "Total Staffs present = 17",
        "Resign = 1",
        "Total Staffs = 17",
        "",
        "Thanks",
    ]

    result = process_work_item(_attendance_work_item(lines=report_lines))

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}
    item_statuses = {item["staff_name"]: item["status"] for item in result.payload["items"]}

    assert result.payload["status"] in {"accepted", "accepted_with_warning"}
    assert "missing_report_date" not in warning_codes
    assert result.payload["branch"] == "lae_5th_street"
    assert result.payload["report_date"] == "2026-05-22"
    assert result.payload["metrics"]["present"] == 17
    assert result.payload["metrics"]["non_active"] == 1
    assert result.payload["metrics"]["total_staff"] == 17
    assert result.payload["metrics"]["total_staff_listed"] == 18
    assert item_statuses["Joyce Lovave"] == "non_active"
    assert result.payload.get("validation_error_code") is None
    assert not any(item["staff_name"] == "Thanks" for item in result.payload["items"])


def test_hr_agent_duplicate_staff_names_require_review(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            total_staff="3",
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "John Doe - Present",
                "John Doe - Day Off",
                "Mary Kila - Present",
                "Total Staff: 3",
            ],
        )
    )

    warning_codes = {warning["code"] for warning in result.payload["warnings"]}

    assert result.payload["status"] == "needs_review"
    assert "duplicate_staff_names" in warning_codes


def test_hr_attendance_info_only_outcome_routes_to_accepted_ack(tmp_path: Path, monkeypatch) -> None:
    _patch_output_paths(tmp_path, monkeypatch)

    result = process_work_item(
        _attendance_work_item(
            total_staff="18",
            lines=[
                "Branch: Waigani Branch",
                "Date: 07/04/2026",
                "Staff Alpha - Present",
                "Staff Bravo - Present",
                "Staff Charlie - Present",
                "Staff Delta - Present",
                "Staff Echo - Present",
                "Staff Foxtrot - Present",
                "Staff Golf - Present",
                "Staff Hotel - Present",
                "Staff India - Present",
                "Staff Juliet - Present",
                "Staff Kilo - Present",
                "Staff Lima - Present",
                "Staff Mike - Present",
                "Staff November - Day Off",
                "Staff Oscar - Day Off",
                "Staff Papa - Day Off",
                "Staff Quebec - Absent With Notice",
                "Staff Romeo - Lay Off",
                "Total Staff: 18",
            ],
        )
    )

    routed = route_conversation_response(
        AgentResult(
            agent_name=result.agent_name,
            payload={
                **result.payload,
                "outputs": ["records/structured/hr_attendance/waigani/2026-04-07.json"],
            },
        ),
        source_message_id="wamid.hr-attendance-accepted",
        sender_phone="67570000000",
    )
    rendered = render_whatsapp_response(routed)

    assert routed["response_type"] == "accepted_ack"
    assert rendered["response_type"] == "accepted_ack"
    assert "ISSUES" not in rendered["response_text"]
    assert "TAOP REVIEW REQUIRED" not in rendered["response_text"]


def test_hr_parser_ignores_headers_and_summary_lines_in_numbered_attendance_format() -> None:
    parsed = parse_hr_work_item(
        WorkItem(
            kind="raw_message",
            payload={
                "classification": {"report_type": "staff_attendance"},
                "raw_message": {"text": "\n".join(_numbered_attendance_lines())},
            },
        )
    )

    staff_names = {record.staff_name for record in parsed.records}

    assert parsed.report_date == "2026-03-11"
    assert len(parsed.records) == 27
    assert parsed.records[0].staff_name == "Erone Bana"
    assert parsed.records[-1].staff_name == "Rhoda Frank"
    assert all(record.status == "present" for record in parsed.records)
    assert staff_names.isdisjoint({"Saturday 11", "Saturday 11/03/26", "28", "Not at work", "0"})
    assert parsed.declared_total_staff == 27
    assert parsed.declared_summary_metrics == {
        "total_staff": 27,
        "staff_present": 28,
        "not_at_work": 0,
        "staff_off": 0,
        "suspend": 0,
        "absent": 0,
        "leave": 0,
        "sick": 0,
    }
    assert any("piso is also working" in note.casefold() for note in parsed.notes)


def _patch_output_paths(tmp_path: Path, monkeypatch) -> tuple[Path, Path]:
    records_dir = tmp_path / "records"
    colony_root = tmp_path / "ioi-colony"
    signals_root = colony_root / "SIGNALS" / "normalized"
    outbox_path = tmp_path / "outbox"
    monkeypatch.setattr(record_paths, "RECORDS_DIR", records_dir)
    monkeypatch.setattr(record_paths, "RAW_WHATSAPP_DIR", records_dir / "raw" / "whatsapp")
    monkeypatch.setattr(record_paths, "STRUCTURED_DIR", records_dir / "structured")
    monkeypatch.setattr(record_paths, "REJECTED_DIR", records_dir / "rejected" / "whatsapp")
    monkeypatch.setenv(record_automation.IOI_COLONY_ROOT_ENV_VAR, str(colony_root))
    monkeypatch.setattr("apps.hr_agent.worker.OUTBOX_PATH", outbox_path)
    return signals_root, outbox_path


def _attendance_work_item(
    *,
    total_staff: str = "4",
    lines: list[str] | None = None,
) -> WorkItem:
    report_lines = lines or [
        "Branch: Waigani Branch",
        "Date: 07/04/2026",
        "John Doe - Present",
        "Mary Kila - Present",
        "Peter Ake - Off",
        "Lena Bina - Leave",
        f"Total Staff: {total_staff}",
        "Notes: Skeleton team only",
    ]
    return WorkItem(
        kind="raw_message",
        payload={
            "classification": {"report_type": "staff_attendance"},
            "raw_message": {"text": "\n".join(report_lines)},
        },
    )


def _numbered_attendance_lines() -> list[str]:
    return [
        "TOP TOWN CLOTHING",
        "LAE MARKET BRANCH",
        "MALAITA STREET",
        "",
        "STAFF ATTENDANCE",
        "",
        "Saturday 11/03/26",
        "",
        "1.Erone Bana = P",
        "2.Renate Norman = P",
        "3.Maria Sine = P",
        "4.Peter Ake = P",
        "5.Lena Bina = P",
        "6.John Doe = P",
        "7.Mary Kila = P",
        "8.Timothy Kale = P",
        "9.Sarah Namo = P",
        "10.Ben Kora = P",
        "11.Lucy Wane = P",
        "12.Paula Sore = P",
        "13.Daniel Taka = P",
        "14.Grace Lari = P",
        "15.Nim Jonnah = P",
        "16.Ricky Lomu = P",
        "17.Sabila Seka = P",
        "18.Debra Wotavo = P",
        "19.Kimson David = P",
        "20.Hendry Ambiu = P",
        "21.Francis Ano = P",
        "22.Bethsien Ken = P",
        "23.Stanly Mathias = P",
        "24.Anita Tangoi = P",
        "25.Moviyo Alex = P",
        "26.Xeena Moris = P",
        "27.Rhoda Frank = P",
        "",
        "Note: Please note piso is also working, her attendance is recorded in record book.",
        "",
        "Total staff = 27",
        "Staff present = 28",
        "Not at work = 0",
        "Staff off = 0",
        "Suspend = 0",
        "Absent: 0",
        "Leave : 0",
        "Sick : 0",
        "",
        "Thankyou.",
    ]
