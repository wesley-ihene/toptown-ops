"""Safe numeric normalization helpers."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re

from .types import AppliedRule, NormalizedValue

_CURRENCY_TOKEN_PATTERN = re.compile(r"\bpgk\b", flags=re.IGNORECASE)
_SPACED_DECIMAL_PATTERN = re.compile(r"(?<=\d)\s*\.\s*(?=\d)")
_RATIO_PATTERN = re.compile(r"(?P<left>\d+(?:\.\d+)?)\s*/\s*(?P<right>\d+(?:\.\d+)?)")


def normalize_decimal(
    raw_value: str,
    *,
    allow_percent: bool = False,
    scalar_from_ratio: bool = False,
    allow_currency_tokens: bool = True,
) -> NormalizedValue:
    """Return a canonical decimal string when the value is safely recoverable."""

    raw = raw_value
    cleaned = raw_value.strip()
    result = NormalizedValue(raw_value=raw, metadata={"value_type": "decimal"})
    if not cleaned:
        result.hard_errors.append("empty_numeric_value")
        return result

    candidate = cleaned
    if scalar_from_ratio:
        ratio_match = _RATIO_PATTERN.search(candidate)
        if ratio_match is not None:
            candidate = ratio_match.group("left")
            result.applied_rules.append(
                AppliedRule(
                    name="ratio_scalar_selected",
                    raw_value=cleaned,
                    normalized_value=candidate,
                    details={"right_side": ratio_match.group("right")},
                )
            )
        elif "/" in candidate:
            result.hard_errors.append("ambiguous_ratio_value")
            return result
    elif "/" in candidate:
        result.hard_errors.append("ambiguous_ratio_value")
        return result

    if "%" in candidate:
        if not allow_percent:
            result.hard_errors.append("unexpected_percent_suffix")
            return result
        stripped = candidate.replace("%", "").strip()
        result.applied_rules.append(
            AppliedRule(
                name="percent_suffix_removed",
                raw_value=candidate,
                normalized_value=stripped,
            )
        )
        candidate = stripped

    if allow_currency_tokens:
        stripped = _CURRENCY_TOKEN_PATTERN.sub("", candidate)
        stripped = stripped.replace("$", "").replace("K", "").replace("k", "")
        if stripped != candidate:
            result.applied_rules.append(
                AppliedRule(
                    name="currency_tokens_removed",
                    raw_value=candidate,
                    normalized_value=stripped.strip(),
                )
            )
            candidate = stripped.strip()

    if candidate.startswith("(") and candidate.endswith(")"):
        unwrapped = candidate[1:-1].strip()
        result.applied_rules.append(
            AppliedRule(
                name="parenthesized_numeric_unwrapped",
                raw_value=candidate,
                normalized_value=unwrapped,
            )
        )
        candidate = unwrapped
    elif "(" in candidate or ")" in candidate:
        result.hard_errors.append("ambiguous_parenthesized_value")
        return result

    decimal_collapsed = _SPACED_DECIMAL_PATTERN.sub(".", candidate)
    if decimal_collapsed != candidate:
        result.applied_rules.append(
            AppliedRule(
                name="decimal_spacing_collapsed",
                raw_value=candidate,
                normalized_value=decimal_collapsed,
            )
        )
        candidate = decimal_collapsed

    compact = re.sub(r"\s+", " ", candidate).strip()
    canonical = _canonical_numeric_string(compact)
    if canonical is None:
        result.hard_errors.append("ambiguous_numeric_value")
        return result

    try:
        Decimal(canonical)
    except InvalidOperation:
        result.hard_errors.append("invalid_decimal_value")
        return result

    result.normalized_value = canonical
    result.confidence = 1.0
    result.metadata["decimal"] = canonical
    if canonical != compact:
        result.applied_rules.append(
            AppliedRule(
                name="numeric_grouping_removed",
                raw_value=compact,
                normalized_value=canonical,
            )
        )
    return result


def normalize_int(raw_value: str, *, scalar_from_ratio: bool = False) -> NormalizedValue:
    """Return a canonical integer string when the value is integral."""

    result = normalize_decimal(raw_value, scalar_from_ratio=scalar_from_ratio)
    if not result.succeeded:
        return result

    try:
        decimal_value = Decimal(result.normalized_value or "")
    except InvalidOperation:
        result.normalized_value = None
        result.hard_errors.append("invalid_decimal_value")
        return result

    if decimal_value != decimal_value.to_integral_value():
        result.normalized_value = None
        result.hard_errors.append("non_integral_value")
        return result

    result.normalized_value = str(int(decimal_value))
    result.metadata["decimal"] = result.normalized_value
    return result


def _canonical_numeric_string(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None

    sign = ""
    if text[0] in {"+", "-"}:
        sign, text = text[0], text[1:]
    if not text:
        return None

    if text.count(".") > 1:
        return None

    int_part, dot, fractional_part = text.partition(".")
    int_part = int_part.strip()
    fractional_part = fractional_part.strip()

    if not int_part:
        return None
    if "," in fractional_part or " " in fractional_part:
        return None
    if fractional_part and not fractional_part.isdigit():
        return None

    if "," in int_part and " " in int_part:
        return None

    if "," in int_part:
        if not re.fullmatch(r"\d{1,3}(,\d{3})*", int_part):
            return None
        int_part = int_part.replace(",", "")
    elif " " in int_part:
        if not re.fullmatch(r"\d{1,3}( \d{3})*", int_part):
            return None
        int_part = int_part.replace(" ", "")
    elif not int_part.isdigit():
        return None

    canonical = f"{sign}{int_part}"
    if dot:
        canonical = f"{canonical}.{fractional_part}"
    return canonical
