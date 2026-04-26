"""Tests for human-tolerant WhatsApp normalization."""

from __future__ import annotations

from packages.human_tolerance import analyze_human_whatsapp_text, normalize_human_whatsapp_text


def test_unicode_header_normalization_recovers_ascii_branch_text() -> None:
    normalized = normalize_human_whatsapp_text("𝕋𝕋ℂ ℙ𝕆𝕄\n𝕎𝔸𝕀𝔾𝔸ℕ𝕀 𝔹ℝ𝔸ℕℂℍ")

    assert "TTC POM" in normalized
    assert "WAIGANI BRANCH" in normalized


def test_attendance_normalization_injects_branch_and_date_lines() -> None:
    result = analyze_human_whatsapp_text(
        "\n".join(
            [
                "𝕋𝕋ℂ ℙ𝕆𝕄",
                "𝕎𝔸𝕀𝔾𝔸ℕ𝕀 𝔹ℝ𝔸ℕℂℍ",
                "",
                "SUNDAY:26/04/26",
                "",
                "STAFFS ATTENDANCE.",
                "",
                "1.Alice Koko = ✔️",
                "2.Grace Masson = OFF",
            ]
        )
    )

    assert result.applied is True
    assert "Branch: waigani" in result.normalized_text
    assert "Date: 26/04/26" in result.normalized_text
    assert result.normalized_fields["branch"] == "waigani"
    assert result.normalized_fields["date"] == "2026-04-26"


def test_attendance_normalization_replaces_title_with_canonical_title() -> None:
    normalized = normalize_human_whatsapp_text(
        "\n".join(
            [
                "WAIGANI BRANCH",
                "MONDAY 01/01/26",
                "STAFFS ATTENDANCE REPORT",
                "1.Alice = P",
                "2.Bob = OFF",
                "3.Cleo = LEAVE",
            ]
        )
    )

    assert "ATTENDANCE REPORT" in normalized
    assert "STAFFS ATTENDANCE REPORT" not in normalized


def test_attendance_normalization_converts_real_world_status_variants() -> None:
    normalized = normalize_human_whatsapp_text(
        "\n".join(
            [
                "WAIGANI BRANCH",
                "SATURDAY:26/04/26",
                "STAFFS ATTENDANCE",
                "1.Alice = ABSENT WITH NOTICE",
                "2.Bob = LEAVE BREAK",
                "3.Cleo = DAY OFF",
                "4.Dan = NILL",
            ]
        )
    )

    assert "1.Alice = AWN" in normalized
    assert "2.Bob = LEAVE" in normalized
    assert "3.Cleo = OFF" in normalized
    assert "4.Dan = NIL" in normalized
