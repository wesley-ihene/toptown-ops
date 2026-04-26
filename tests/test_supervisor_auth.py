"""Tests for supervisor authorization decisions."""

from __future__ import annotations

from pathlib import Path

from apps.supervisor_auth import worker


def test_authorize_supervisor_allows_matching_phone_and_branch(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "supervisors.json"
    config_path.write_text(
        '{"supervisors":[{"name":"Alice","sender_phone":"67570000000","branches":["waigani","lae_malaita"]}]}',
        encoding="utf-8",
    )
    monkeypatch.setattr(worker, "_CONFIG_PATH", config_path)
    worker._load_supervisors_cached.cache_clear()

    result = worker.authorize_supervisor(sender_phone="67570000000", branch="waigani")

    assert result["authorized"] is True
    assert result["reason"] == "authorized"
    assert result["supervisor"]["name"] == "Alice"


def test_authorize_supervisor_rejects_branch_mismatch(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "supervisors.json"
    config_path.write_text(
        '{"supervisors":[{"name":"Alice","sender_phone":"67570000000","branches":["waigani"]}]}',
        encoding="utf-8",
    )
    monkeypatch.setattr(worker, "_CONFIG_PATH", config_path)
    worker._load_supervisors_cached.cache_clear()

    result = worker.authorize_supervisor(sender_phone="67570000000", branch="bena_road")

    assert result["authorized"] is False
    assert result["reason"] == "branch_not_allowed"


def test_load_supervisors_handles_missing_config_safely(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(worker, "_CONFIG_PATH", tmp_path / "missing.json")
    worker._load_supervisors_cached.cache_clear()

    assert worker.load_supervisors() == []
