"""Tests for deterministic learning artifact storage."""

from __future__ import annotations

import json
from pathlib import Path

from packages.learning_store import (
    get_learning_artifact_path,
    read_latest_learning_artifact,
    write_daily_summary,
    write_review_summary,
)


def test_learning_artifact_path_is_deterministic(tmp_path: Path) -> None:
    path = get_learning_artifact_path(
        "review_summary",
        "2026-04-07",
        output_root=tmp_path,
    )

    assert path == tmp_path / "records" / "learning" / "review_summary" / "2026-04-07.json"


def test_same_day_summary_overwrite_reuses_same_path_and_updates_payload(tmp_path: Path) -> None:
    first_path = write_daily_summary(
        "2026-04-07",
        {
            "summary": {"reviewed_items": 2},
            "generated_at": "2026-04-07T09:00:00Z",
            "source_paths": ["records/review/2026-04-07/a.json"],
            "analysis_window": {"start_date": "2026-04-06", "end_date": "2026-04-07"},
        },
        output_root=tmp_path,
    )

    second_path = write_daily_summary(
        "2026-04-07",
        {
            "summary": {"reviewed_items": 4},
            "generated_at": "2026-04-07T11:30:00Z",
            "source_paths": ["records/review/2026-04-07/b.json"],
            "analysis_window": {"start_date": "2026-04-07", "end_date": "2026-04-07"},
        },
        output_root=tmp_path,
    )

    assert first_path == second_path
    payload = json.loads(Path(second_path).read_text(encoding="utf-8"))
    assert payload["date"] == "2026-04-07"
    assert payload["summary"] == {"reviewed_items": 4}
    assert payload["generated_at"] == "2026-04-07T11:30:00Z"
    assert payload["source_paths"] == ["records/review/2026-04-07/b.json"]
    assert payload["analysis_window"] == {"start_date": "2026-04-07", "end_date": "2026-04-07"}


def test_write_does_not_mutate_input_payload(tmp_path: Path) -> None:
    payload = {
        "summary": {"reviewed_items": 3},
        "generated_at": "2026-04-07T09:00:00Z",
        "source_paths": ["records/review/2026-04-07/source.json"],
        "analysis_window": {"start_date": "2026-04-01", "end_date": "2026-04-07"},
    }
    payload_before = json.loads(json.dumps(payload))

    write_review_summary("2026-04-07", payload, output_root=tmp_path)

    assert payload == payload_before
    written = json.loads(
        (tmp_path / "records" / "learning" / "review_summary" / "2026-04-07.json").read_text(encoding="utf-8")
    )
    assert written["generated_at"] == payload_before["generated_at"]
    assert written["source_paths"] == payload_before["source_paths"]
    assert written["analysis_window"] == payload_before["analysis_window"]


def test_read_latest_learning_artifact_returns_latest_by_date(tmp_path: Path) -> None:
    write_review_summary(
        "2026-04-07",
        {"summary": {"reviewed_items": 1}},
        generated_at="2026-04-07T09:00:00Z",
        source_paths=["records/review/2026-04-07/a.json"],
        analysis_window={"start_date": "2026-04-07", "end_date": "2026-04-07"},
        output_root=tmp_path,
    )
    write_review_summary(
        "2026-04-09",
        {"summary": {"reviewed_items": 5}},
        generated_at="2026-04-09T09:00:00Z",
        source_paths=["records/review/2026-04-09/b.json"],
        analysis_window={"start_date": "2026-04-08", "end_date": "2026-04-09"},
        output_root=tmp_path,
    )

    latest = read_latest_learning_artifact("review_summary", output_root=tmp_path)

    assert latest is not None
    assert latest["date"] == "2026-04-09"
    assert latest["summary"] == {"reviewed_items": 5}
    assert latest["generated_at"] == "2026-04-09T09:00:00Z"
