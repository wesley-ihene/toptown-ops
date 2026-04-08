"""Normalize early header lines before deterministic routing."""

from __future__ import annotations

from dataclasses import dataclass
import re

_DASH_TRANSLATION = str.maketrans(
    {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
    }
)
_EMOJI_PREFIX_PATTERN = re.compile(r"^[^A-Za-z0-9]+")
_REPEATED_PUNCTUATION_PATTERN = re.compile(r"([.,:;!?\-])\1+")
_NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")


@dataclass(slots=True)
class HeaderCandidate:
    """One raw and normalized header candidate line."""

    line_number: int
    raw_line: str
    normalized_line: str


@dataclass(slots=True)
class HeaderNormalizationResult:
    """Header normalization result for the first non-empty lines."""

    candidates: list[HeaderCandidate]

    def normalized_lines(self) -> list[str]:
        """Return normalized header strings."""

        return [candidate.normalized_line for candidate in self.candidates]


def normalize_headers(text: str, *, max_lines: int = 8) -> HeaderNormalizationResult:
    """Return the first non-empty lines plus stable normalized variants."""

    candidates: list[HeaderCandidate] = []
    for index, raw_line in enumerate(text.splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        candidates.append(
            HeaderCandidate(
                line_number=index,
                raw_line=stripped,
                normalized_line=_normalize_header_line(stripped),
            )
        )
        if len(candidates) >= max_lines:
            break
    return HeaderNormalizationResult(candidates=candidates)


def _normalize_header_line(value: str) -> str:
    """Normalize one header line for deterministic matching."""

    cleaned = value.translate(_DASH_TRANSLATION)
    cleaned = _EMOJI_PREFIX_PATTERN.sub("", cleaned)
    cleaned = _REPEATED_PUNCTUATION_PATTERN.sub(r"\1", cleaned)
    cleaned = cleaned.replace("/", " / ")
    cleaned = cleaned.replace("(", " ").replace(")", " ")
    normalized = _NON_ALPHANUMERIC_PATTERN.sub(" ", cleaned.casefold())
    return " ".join(normalized.split())
