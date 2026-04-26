"""Focused tests for branch resolution from normalized headers."""

from __future__ import annotations

from apps.branch_resolver_agent.worker import resolve_branch
from apps.header_normalizer_agent.worker import normalize_headers


def test_branch_resolver_handles_stylized_unicode_branch_header() -> None:
    header_result = normalize_headers(
        "\n".join(
            [
                "𝕋𝕋ℂ ℙ𝕆𝕄",
                "𝕎𝔸𝕀𝔾𝔸ℕ𝕀 𝔹ℝ𝔸ℕℂℍ",
            ]
        )
    )

    resolved = resolve_branch(header_result)

    assert resolved.branch_hint == "waigani"
    assert resolved.branch_display_name == "Waigani"
    assert header_result.normalized_lines() == ["ttc pom", "waigani branch"]
