"""Focused tests for shared branch normalization."""

from __future__ import annotations

from packages.normalization.branches import normalize_branch
from packages.normalization.engine import normalize_report


def test_normalize_branch_resolves_alias_variants() -> None:
    result = normalize_branch("TTC LAE 5TH STREET BRANCH")

    assert result.normalized_value == "lae_5th_street"
    assert result.metadata["matched_alias"] == "ttc lae 5th street branch"


def test_normalize_branch_passes_through_canonical_slug() -> None:
    result = normalize_branch("waigani")

    assert result.normalized_value == "waigani"
    assert result.succeeded is True


def test_normalize_branch_resolves_stylized_unicode_header() -> None:
    result = normalize_branch("𝕎𝔸𝕀𝔾𝔸ℕ𝕀 𝔹ℝ𝔸ℕℂℍ")

    assert result.normalized_value == "waigani"
    assert result.metadata["matched_alias"] == "waigani branch"


def test_normalize_branch_resolves_decorative_punctuation_and_reordered_tokens() -> None:
    result = normalize_branch("BRANCH::WAIGANI")

    assert result.normalized_value == "waigani"
    assert result.metadata["matched_alias"] == "waigani branch"


def test_normalize_branch_resolves_branch_variant_with_extra_noise() -> None:
    result = normalize_branch("Shop >>> TTC LAE 5TH STREET BRANCH !!!")

    assert result.normalized_value == "lae_5th_street"
    assert result.metadata["matched_alias"] == "ttc lae 5th street branch"


def test_normalize_report_is_idempotent_for_recoverable_sales_text() -> None:
    raw_text = "\n".join(
        [
            "DAY end sales report",
            "Shop: TTC LAE 5TH STREET BRANCH",
            "Date: Friday, 10/04 /26",
            "Total Sales: K3,489. 00",
            "Cash Sales: 1 236.00",
            "Card Sales: 2 253.00",
            "Main Door: 15",
            "Customers Served: 12",
        ]
    )

    first = normalize_report(raw_text, report_family="sales")
    second = normalize_report(first.normalized_text or "", report_family="sales")

    assert first.normalized_text == second.normalized_text
    assert first.normalized_fields["branch"] == "lae_5th_street"
    assert first.normalized_fields["report_date"] == "2026-04-10"


def test_normalize_branch_rejects_unknown_alias() -> None:
    result = normalize_branch("Unknown Satellite Store")

    assert result.normalized_value is None
    assert result.hard_errors == ["unknown_branch_alias"]


def test_normalize_branch_rejects_ambiguous_branch_header() -> None:
    result = normalize_branch("TTC LAE BRANCH")

    assert result.normalized_value is None
    assert result.hard_errors == ["unknown_branch_alias"]
