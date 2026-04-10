"""Persist learning proposals separately from live config and rules."""

from __future__ import annotations

from packages.record_store.naming import safe_segment
from packages.record_store.paths import get_proposal_path
from packages.record_store.writer import write_json_file


def write_proposal_record(
    *,
    generated_date: str,
    report_type: str,
    proposal_type: str,
    proposal_key: str,
    payload: dict[str, object],
) -> str:
    """Write one proposal payload and return its path."""

    proposal_path = get_proposal_path(generated_date, report_type, proposal_type) / f"{safe_segment(proposal_key)}.json"
    write_json_file(proposal_path, payload)
    return str(proposal_path)
