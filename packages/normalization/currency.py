"""Currency normalization helpers."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re

from .numbers import normalize_decimal
from .types import AppliedRule, NormalizedValue

_PGK_CODE_PATTERN = re.compile(r"\bpgk\b", flags=re.IGNORECASE)
_KINA_SYMBOL_PATTERN = re.compile(r"(^|\s)k(?=\s|\d)", flags=re.IGNORECASE)


def normalize_money(raw_value: str) -> NormalizedValue:
    """Return a canonical money string with two decimal places."""

    result = normalize_decimal(raw_value, allow_currency_tokens=True)
    result.metadata["value_type"] = "money"
    if not result.succeeded:
        return result

    currency_code = _detect_currency_code(raw_value)
    try:
        amount = Decimal(result.normalized_value or "").quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        result.normalized_value = None
        result.hard_errors.append("invalid_money_value")
        return result

    normalized_money = f"{amount:.2f}"
    if result.normalized_value != normalized_money:
        result.applied_rules.append(
            AppliedRule(
                name="money_quantized_two_decimals",
                raw_value=result.normalized_value,
                normalized_value=normalized_money,
            )
        )
    result.normalized_value = normalized_money
    result.metadata["currency_code"] = currency_code
    return result


def _detect_currency_code(raw_value: str) -> str | None:
    if _PGK_CODE_PATTERN.search(raw_value):
        return "PGK"
    if _KINA_SYMBOL_PATTERN.search(raw_value) or raw_value.strip().startswith(("K", "k")):
        return "PGK"
    return None
