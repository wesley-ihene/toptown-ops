"""Tests for compact telemetry intelligence summaries."""

from __future__ import annotations

import io
import json
from pathlib import Path

from packages.telemetry_intelligence import summarize_telemetry_for_date


def test_skip_spam_is_counted_without_line_by_line_storage(tmp_path: Path) -> None:
    _write_log(
        tmp_path / "LOGS" / "runtime.log",
        [
            (
                "2026-05-24 WARNING SKIP duplicate live WhatsApp message skipped: "
                f"message_id=wamid-{index:02d} branch=waigani report_type=sales reason=duplicate_message"
            )
            for index in range(12)
        ],
    )

    result = summarize_telemetry_for_date("2026-05-24", repo_root=tmp_path)

    events = result["events_payload"]["events"]
    assert len(events) == 1
    assert events[0]["event_type"] == "duplicate_replay"
    assert events[0]["count"] == 12
    assert result["summary_payload"]["event_count"] == 1
    assert result["summary_payload"]["occurrence_count"] == 12


def test_duplicate_replay_events_are_summarized(tmp_path: Path) -> None:
    _write_log(
        tmp_path / "LOGS" / "bridge.log",
        [
            "2026-05-24 INFO duplicate live WhatsApp message skipped: branch=waigani report_type=sales reason=duplicate_message",
        ],
    )
    _write_log(
        tmp_path / "worker_decision_v2.log",
        [
            "2026-05-24 INFO duplicate replay skipped: branch=waigani report_type=sales reason=duplicate_message",
        ],
    )

    result = summarize_telemetry_for_date("2026-05-24", repo_root=tmp_path)

    event = result["events_payload"]["events"][0]
    assert event["event_type"] == "duplicate_replay"
    assert event["count"] == 2
    assert event["reason_codes"] == ["duplicate_message"]
    assert event["sources"] == ["LOGS/bridge.log", "worker_decision_v2.log"]


def test_failed_validations_extract_reason_codes(tmp_path: Path) -> None:
    _write_log(
        tmp_path / "LOGS" / "validation.log",
        [
            (
                "2026-05-24 WARNING validation failed "
                "validation_error_code=declared_summary_total_mismatch "
                "branch=waigani report_type=hr_attendance"
            ),
            (
                "2026-05-24 WARNING validation failed "
                "validation_error_code=declared_summary_total_mismatch "
                "branch=waigani report_type=hr_attendance"
            ),
        ],
    )

    result = summarize_telemetry_for_date("2026-05-24", repo_root=tmp_path)

    event = result["events_payload"]["events"][0]
    assert event["event_type"] == "failed_validation"
    assert event["count"] == 2
    assert event["reason_codes"] == ["declared_summary_total_mismatch"]


def test_raw_logs_are_not_copied_into_outputs(tmp_path: Path) -> None:
    raw_line = (
        "2026-05-24 ERROR anomaly detected branch=lae report_type=sales "
        "token=abcd1234 password=supersecret "
        "payload=THIS IS A VERY LONG RAW LOG LINE THAT SHOULD NEVER BE COPIED INTO TELEMETRY OUTPUTS "
        "WITH ALL OF ITS ORIGINAL WORDING OR SECRET VALUES INTACT EVEN WHEN THE SAMPLE IS KEPT FOR CONTEXT"
    )
    _write_log(tmp_path / "LOGS" / "runtime.log", [raw_line])

    result = summarize_telemetry_for_date("2026-05-24", repo_root=tmp_path)

    events_json = Path(result["events_path"]).read_text(encoding="utf-8")
    summary_json = Path(result["summary_path"]).read_text(encoding="utf-8")
    assert raw_line not in events_json
    assert raw_line not in summary_json
    assert "abcd1234" not in events_json
    assert "supersecret" not in events_json
    sample = result["events_payload"]["events"][0]["sample"]
    assert len(sample) <= 250
    assert "[REDACTED]" in sample


def test_vectorize_candidates_contain_only_compact_lessons(tmp_path: Path) -> None:
    _write_log(
        tmp_path / "LOGS" / "runtime.log",
        [
            "2026-05-24 INFO duplicate replay skipped: branch=waigani report_type=sales reason=duplicate_message",
            "2026-05-24 ERROR high-risk branch=waigani item_id=item_42 priority=high",
        ],
    )

    result = summarize_telemetry_for_date("2026-05-24", repo_root=tmp_path)

    candidates = result["summary_payload"]["vectorize_candidates"]
    assert len(candidates) == 2
    for candidate in candidates:
        assert set(candidate) == {"candidate_id", "event_type", "priority", "lesson"}
        assert "\n" not in candidate["lesson"]
        assert "message_id=" not in candidate["lesson"]
        assert "token=" not in candidate["lesson"]
        assert len(candidate["lesson"]) <= 220


def test_large_logs_are_streamed_line_by_line(tmp_path: Path, monkeypatch) -> None:
    log_path = tmp_path / "LOGS" / "runtime.log"
    log_lines = [
        f"2026-05-24 WARNING validation failed validation_error_code=code_{index} branch=waigani report_type=sales"
        for index in range(1, 4)
    ]
    _write_log(log_path, log_lines)
    original_open = Path.open

    class _StreamingOnly(io.StringIO):
        def __enter__(self) -> "_StreamingOnly":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            self.close()

        def read(self, *args, **kwargs):  # type: ignore[override]
            raise AssertionError("telemetry summarizer must stream log reads line-by-line")

        def readlines(self, *args, **kwargs):  # type: ignore[override]
            raise AssertionError("telemetry summarizer must not bulk-read logs")

    def patched_open(self: Path, *args, **kwargs):
        if self.resolve() == log_path.resolve():
            return _StreamingOnly("\n".join(log_lines) + "\n")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", patched_open)

    result = summarize_telemetry_for_date("2026-05-24", repo_root=tmp_path)

    assert result["summary_payload"]["occurrence_count"] == 3


def test_output_is_idempotent(tmp_path: Path) -> None:
    _write_log(
        tmp_path / "LOGS" / "runtime.log",
        [
            "2026-05-24 INFO duplicate replay skipped: branch=waigani report_type=sales reason=duplicate_message",
            "2026-05-24 WARNING validation failed validation_error_code=declared_summary_total_mismatch branch=waigani report_type=hr_attendance",
        ],
    )

    first = summarize_telemetry_for_date("2026-05-24", repo_root=tmp_path)
    second = summarize_telemetry_for_date("2026-05-24", repo_root=tmp_path)

    assert first["events_payload"] == second["events_payload"]
    assert first["summary_payload"] == second["summary_payload"]
    assert Path(first["events_path"]).read_text(encoding="utf-8") == Path(second["events_path"]).read_text(encoding="utf-8")
    assert Path(first["summary_path"]).read_text(encoding="utf-8") == Path(second["summary_path"]).read_text(encoding="utf-8")


def _write_log(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

