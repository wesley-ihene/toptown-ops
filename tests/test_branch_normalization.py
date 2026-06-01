"""Focused regression tests for canonical branch normalization and persistence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from packages.normalization.branches import canonical_branch_slug, canonical_branch_slug_or_none, normalize_branch
from packages.record_store.duplicate_archive import archive_duplicate_record
from packages.record_store.writer import write_governed_structured, write_json_file
from packages.review_queue import write_review_item
from packages.signal_contracts.work_item import WorkItem


@pytest.mark.parametrize(
    ("raw_branch", "expected_slug"),
    [
        ("TTC Bena Road-Goroka Branch", "bena_road"),
        ("Branch:Goroka, BenaRoad", "bena_road"),
        ("TTC WAIGANI", "waigani"),
        ("TTC POM Waigani Branch", "waigani"),
        ("LAE Malaita Shop", "lae_malaita"),
    ],
)
def test_branch_aliases_resolve_to_canonical_slugs(raw_branch: str, expected_slug: str) -> None:
    result = normalize_branch(raw_branch)

    assert result.normalized_value == expected_slug


def test_unknown_branch_does_not_create_new_slug() -> None:
    assert canonical_branch_slug_or_none("Unknown Satellite Store") is None
    with pytest.raises(ValueError, match="unknown_branch_slug"):
        canonical_branch_slug("Unknown Satellite Store")


def test_structured_write_uses_canonical_slug_only(tmp_path: Path) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()

    result = write_governed_structured(
        "sales_income",
        "TTC POM Waigani Branch",
        "2026-05-01",
        {
            "signal_type": "sales_income",
            "branch": "TTC POM Waigani Branch",
            "report_date": "2026-05-01",
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 600.0,
                "eftpos_sales": 600.0,
                "traffic": 12,
                "served": 9,
            },
            "warnings": [],
            "status": "accepted",
        },
        metadata={
            "validation": {
                "accepted": True,
                "status": "accepted",
                "reason_codes": [],
                "rejections": [],
                "details": {},
            },
            "governance_context": {
                "message_id": "wamid.branch-normalization-1",
                "raw_sha256": "branch-normalization-raw-1",
            },
        },
        root=tmp_path,
        colony_root=colony_root,
    )

    assert result.persisted is True
    assert result.path == tmp_path / "records" / "structured" / "sales_income" / "waigani" / "2026-05-01.json"
    assert not (tmp_path / "records" / "structured" / "sales_income" / "ttc_pom_waigani_branch").exists()

    payload = json.loads(result.path.read_text(encoding="utf-8"))
    assert payload["branch"] == "waigani"


def test_unknown_branch_is_reviewed_without_creating_structured_directory(tmp_path: Path) -> None:
    colony_root = tmp_path / "ioi-colony"
    colony_root.mkdir()
    metadata = {
        "validation": {
            "accepted": True,
            "status": "accepted",
            "reason_codes": [],
            "rejections": [],
            "details": {},
        },
        "governance_context": {
            "message_id": "wamid.branch-normalization-2",
            "raw_sha256": "branch-normalization-raw-2",
        },
    }

    result = write_governed_structured(
        "sales_income",
        "Unknown Satellite Store",
        "2026-05-01",
        {
            "signal_type": "sales_income",
            "branch": "Unknown Satellite Store",
            "report_date": "2026-05-01",
            "metrics": {
                "gross_sales": 1200.0,
                "cash_sales": 600.0,
                "eftpos_sales": 600.0,
            },
            "warnings": [],
            "status": "accepted",
        },
        metadata=metadata,
        root=tmp_path,
        colony_root=colony_root,
    )

    assert result.persisted is False
    assert result.governance.status == "needs_review"
    assert result.governance.reasons == ["unknown_branch_slug"]
    assert metadata["validation"]["reason_codes"] == ["unknown_branch_slug"]
    assert metadata["validation"]["rejections"][0]["reason_code"] == "unknown_branch_slug"
    assert not (tmp_path / "records" / "structured" / "sales_income").exists()


def test_review_write_uses_canonical_branch_only(tmp_path: Path) -> None:
    review_path = write_review_item(
        WorkItem(kind="raw_message", payload={"message_hash": "branch-review-1"}),
        report_type="sales",
        branch="TTC Bena Road-Goroka Branch",
        report_date="2026-05-01",
        confidence=0.42,
        warnings=[],
        reason="unknown_branch_slug",
        output_root=tmp_path,
    )

    path = Path(review_path)
    assert path == tmp_path / "records" / "review" / "2026_05_01" / "bena_road" / "sales" / "branch_review_1.json"
    assert not (tmp_path / "records" / "review" / "2026_05_01" / "ttc_bena_road_goroka").exists()

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["branch"] == "bena_road"


def test_duplicate_archive_uses_canonical_branch_only(tmp_path: Path) -> None:
    raw_meta_path = tmp_path / "records" / "raw" / "whatsapp" / "unknown" / "sample.meta.json"
    write_json_file(
        raw_meta_path,
        {
            "branch_hint": "ttc_bena_road_goroka",
            "report_type": "sales",
            "raw_txt_path": "records/raw/whatsapp/unknown/sample.txt",
        },
    )

    archive_path = archive_duplicate_record(
        source_message_id="wamid.dup-branch",
        sender_phone="67570000000",
        branch="ttc_bena_road_goroka",
        report_type="sales",
        report_date="2026-05-01",
        raw_txt_path="records/raw/whatsapp/unknown/sample.txt",
        raw_meta_path=str(raw_meta_path),
        duplicate_reason="duplicate_semantic",
        duplicate_basis="semantic_sha256:abc",
        output_root=tmp_path,
    )

    payload = json.loads(archive_path.read_text(encoding="utf-8"))
    assert payload["branch"] == "bena_road"
