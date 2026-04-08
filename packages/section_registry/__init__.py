"""Deterministic section registry for retail section normalization."""

from __future__ import annotations

from dataclasses import dataclass
import re

_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")

SECTION_ALIASES: dict[str, tuple[str, ...]] = {
    "cashier": ("cashier", "cashier counter"),
    "door_guard": ("door guard", "door man", "doorman", "door"),
    "pricing_room": ("pricing room", "pricing area", "price room"),
    "pricing_room_sales_tally": ("price room sales tally", "pricing room sales tally"),
    "household_rummage": ("household rummage", "hhr"),
    "mens_jeans": ("mens jeans", "men s jeans", "men jeans"),
    "ladies_jeans": ("ladies jeans",),
    "mens_tshirt": ("mens tshirt", "mens t shirt", "men s tshirt", "men s t shirt"),
    "mens_shorts": ("mens shorts", "men s shorts"),
    "ladies_cotton_capri": ("ladies cotton capri",),
    "ladies_tshirt": ("ladies tshirt", "ladies t shirt"),
    "ladies_jackets": ("ladies jackets", "ladies jacket"),
    "kids_boy_pants": ("kids boy pants",),
    "kids_shorts": ("kids shorts",),
    "comforter": ("comforter",),
    "mens_button_shirt": ("mens button shirt", "men s button shirt"),
    "reflectors_workwear": ("reflectors", "workwear"),
    "beach_wear_sportswear": ("beach wear sports wear", "beach wear", "sports wear"),
    "shoe_shop": ("shoe shop", "shoes handbags shopping bags"),
}


@dataclass(slots=True)
class SectionMatch:
    """Resolved section slug plus evidence."""

    section_slug: str
    matched_alias: str
    confidence: float


def normalize_section_text(value: str) -> str:
    """Normalize free-form section text for deterministic matching."""

    normalized = _NON_ALPHANUMERIC_PATTERN.sub(" ", value.casefold())
    return " ".join(normalized.split())


def resolve_section_alias(value: str) -> SectionMatch | None:
    """Resolve the best section alias from a raw section string."""

    normalized = normalize_section_text(value)
    if not normalized:
        return None

    if "price room" in normalized and "sales tally" in normalized:
        return SectionMatch(
            section_slug="pricing_room_sales_tally",
            matched_alias="price room sales tally",
            confidence=1.0,
        )

    segments = [segment.strip() for segment in re.split(r"[,;/]+", value) if segment.strip()]
    normalized_segments = [normalize_section_text(segment) for segment in segments]
    candidates: list[tuple[int, int, SectionMatch]] = []

    for index, segment in enumerate(normalized_segments):
        for section_slug, aliases in SECTION_ALIASES.items():
            for alias in aliases:
                normalized_alias = normalize_section_text(alias)
                if segment == normalized_alias:
                    candidates.append(
                        (
                            index,
                            0,
                            SectionMatch(
                                section_slug=section_slug,
                                matched_alias=normalized_alias,
                                confidence=1.0,
                            ),
                        )
                    )
                elif normalized_alias and normalized_alias in segment:
                    candidates.append(
                        (
                            index,
                            1,
                            SectionMatch(
                                section_slug=section_slug,
                                matched_alias=normalized_alias,
                                confidence=0.85,
                            ),
                        )
                    )

    if candidates:
        _, _, best_match = min(candidates, key=lambda item: (item[0], item[1], -len(item[2].matched_alias)))
        return best_match

    return None
