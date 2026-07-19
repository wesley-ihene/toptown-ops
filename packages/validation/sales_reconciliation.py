"""Shared sales reconciliation helpers for validation and governance."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import re
from typing import Any

from packages.common.normalizer import parse_money

TOLERANCE = 0.01
_KEY_VALUE_PATTERN = re.compile(r"^\s*([^:=]+)\s*[:=]\s*(.+?)\s*$")
_NORMALIZE_PATTERN = re.compile(r"[^a-z0-9]+")
_MONEY_TOKEN_PATTERN = re.compile(r"(?<![#/])(?:[Kk]\s*)?-?\d(?:[\d, ]*\d)?(?:\.\d+)?")

_DECLARED_TOTAL_CASH_LABELS = frozenset({"total cash"})
_DECLARED_TOTAL_CARD_LABELS = frozenset({"total card"})
_DECLARED_TOTAL_SALES_LABELS = frozenset({"total sales"})
_TILL_CASH_LABELS = frozenset({"t cash"})
_TILL_CARD_LABELS = frozenset({"t card"})
_Z_READING_LABELS = frozenset({"z reading"})
_CASH_OVER_LABELS = frozenset({"cash over", "c over"})
_CASH_DOWN_LABELS = frozenset({"cash down", "c down"})
_ITEM_RETURN_LABELS = frozenset(
    {
        "items return",
        "items returns",
        "item return",
        "item returns",
        "returned item",
        "returned items",
        "return",
        "returns",
        "return adjustment",
        "item return adjustment",
        "refund",
        "refunds",
        "refund amount",
        "refund amounts",
    }
)
_EFTPOS_RECONCILIATION_LABELS = frozenset(
    {
        "card reconciliation",
        "eftpos reconciliation",
        "eftpos card reconciliation",
        "card adjustment",
        "eftpos adjustment",
        "card variance",
        "eftpos variance",
    }
)


@dataclass(slots=True, frozen=True)
class SalesReconciliation:
    """Computed reconciliation diagnostics for one sales payload."""

    operational_sales: float | None = None
    expected_z_reading: float | None = None
    expected_total_sales: float | None = None
    cash_over: float = 0.0
    cash_down: float = 0.0
    item_return_adjustment: float = 0.0
    eftpos_reconciliation_adjustment: float = 0.0
    variance_explained_by_cash_over: float = 0.0
    variance_explained_by_cash_down: float = 0.0
    variance_explained: bool = True
    unexplained_variance: float = 0.0
    declared_total_cash: float | None = None
    expected_total_cash: float | None = None
    declared_total_card: float | None = None
    expected_total_card: float | None = None
    declared_total_sales: float | None = None
    declared_z_reading: float | None = None
    diagnostics_present: bool = False

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-safe reconciliation payload."""

        payload: dict[str, Any] = {
            "operational_sales": self.operational_sales,
            "expected_z_reading": self.expected_z_reading,
            "expected_total_sales": self.expected_total_sales,
            "cash_over": self.cash_over,
            "cash_down": self.cash_down,
            "item_return_adjustment": self.item_return_adjustment,
            "eftpos_reconciliation_adjustment": self.eftpos_reconciliation_adjustment,
            "variance_explained_by_cash_over": self.variance_explained_by_cash_over,
            "variance_explained_by_cash_down": self.variance_explained_by_cash_down,
            "variance_explained": self.variance_explained,
            "unexplained_variance": self.unexplained_variance,
        }
        optional_fields = {
            "declared_total_cash": self.declared_total_cash,
            "expected_total_cash": self.expected_total_cash,
            "declared_total_card": self.declared_total_card,
            "expected_total_card": self.expected_total_card,
            "declared_total_sales": self.declared_total_sales,
            "declared_z_reading": self.declared_z_reading,
        }
        for field_name, value in optional_fields.items():
            if value is not None:
                payload[field_name] = value
        return payload


@dataclass(slots=True, frozen=True)
class _RawEvidence:
    declared_total_cash: float | None = None
    declared_total_card: float | None = None
    declared_total_sales: float | None = None
    till_cash_total: float | None = None
    till_card_total: float | None = None
    z_reading_total: float | None = None
    cash_over: float | None = None
    cash_down: float | None = None
    item_return_adjustment: float | None = None
    eftpos_reconciliation_adjustment: float | None = None
    diagnostics_present: bool = False


@dataclass(slots=True, frozen=True)
class _MetricEvidence:
    declared_total_cash: float | None = None
    declared_total_card: float | None = None
    declared_total_sales: float | None = None
    declared_z_reading: float | None = None
    cash_over: float | None = None
    cash_down: float | None = None
    item_return_adjustment: float | None = None
    eftpos_reconciliation_adjustment: float | None = None
    diagnostics_present: bool = False


def build_sales_reconciliation(
    *,
    metrics: Mapping[str, Any] | None = None,
    raw_text: str | None = None,
) -> SalesReconciliation:
    """Return explicit reconciliation diagnostics for one sales payload."""

    raw_evidence = _extract_raw_evidence(raw_text)
    metric_evidence = _extract_metric_evidence(metrics)

    declared_total_cash = _coalesce(raw_evidence.declared_total_cash, metric_evidence.declared_total_cash)
    declared_total_card = _coalesce(raw_evidence.declared_total_card, metric_evidence.declared_total_card)
    declared_total_sales = _coalesce(raw_evidence.declared_total_sales, metric_evidence.declared_total_sales)
    declared_z_reading = _coalesce(raw_evidence.z_reading_total, metric_evidence.declared_z_reading)

    baseline_total_cash = _coalesce(raw_evidence.till_cash_total, metric_evidence.declared_total_cash)
    baseline_total_card = _coalesce(raw_evidence.till_card_total, metric_evidence.declared_total_card)
    operational_sales = _sum_money(baseline_total_cash, baseline_total_card)

    cash_over = _money_or_zero(_coalesce(raw_evidence.cash_over, metric_evidence.cash_over))
    cash_down = _money_or_zero(_coalesce(raw_evidence.cash_down, metric_evidence.cash_down))
    item_return_adjustment = _money_or_zero(
        _coalesce(raw_evidence.item_return_adjustment, metric_evidence.item_return_adjustment)
    )
    eftpos_reconciliation_adjustment = _money_or_zero(
        _coalesce(raw_evidence.eftpos_reconciliation_adjustment, metric_evidence.eftpos_reconciliation_adjustment)
    )

    expected_z_reading = None
    expected_total_sales = None
    expected_total_cash = baseline_total_cash
    expected_total_card = baseline_total_card
    if operational_sales is not None:
        policy = _select_reconciliation_policy(
            operational_sales=operational_sales,
            declared_total_cash=declared_total_cash,
            declared_total_card=declared_total_card,
            declared_total_sales=declared_total_sales,
            declared_z_reading=declared_z_reading,
            baseline_total_cash=baseline_total_cash,
            baseline_total_card=baseline_total_card,
            cash_over=cash_over,
            cash_down=cash_down,
            item_return_adjustment=item_return_adjustment,
            eftpos_reconciliation_adjustment=eftpos_reconciliation_adjustment,
        )
        expected_total_cash = policy["expected_total_cash"]
        expected_total_card = policy["expected_total_card"]
        expected_z_reading = policy["expected_z_reading"]
        expected_total_sales = policy["expected_total_sales"]

    unexplained_differences: list[float] = []
    for declared_value, expected_value in (
        (declared_total_cash, expected_total_cash),
        (declared_total_card, expected_total_card),
        (declared_total_sales, expected_total_sales),
        (declared_z_reading, expected_z_reading),
    ):
        if _money_differs(declared_value, expected_value):
            unexplained_differences.append(round(abs((declared_value or 0.0) - (expected_value or 0.0)), 2))

    unexplained_variance = round(max(unexplained_differences, default=0.0), 2)

    return SalesReconciliation(
        operational_sales=operational_sales,
        expected_z_reading=expected_z_reading,
        expected_total_sales=expected_total_sales,
        cash_over=cash_over,
        cash_down=cash_down,
        item_return_adjustment=item_return_adjustment,
        eftpos_reconciliation_adjustment=eftpos_reconciliation_adjustment,
        variance_explained_by_cash_over=cash_over,
        variance_explained_by_cash_down=cash_down,
        variance_explained=unexplained_variance <= TOLERANCE,
        unexplained_variance=unexplained_variance,
        declared_total_cash=declared_total_cash,
        expected_total_cash=expected_total_cash,
        declared_total_card=declared_total_card,
        expected_total_card=expected_total_card,
        declared_total_sales=declared_total_sales,
        declared_z_reading=declared_z_reading,
        diagnostics_present=raw_evidence.diagnostics_present or metric_evidence.diagnostics_present,
    )


def empty_sales_reconciliation() -> SalesReconciliation:
    """Return the default empty reconciliation view."""

    return SalesReconciliation()


def _extract_raw_evidence(raw_text: str | None) -> _RawEvidence:
    if not isinstance(raw_text, str) or not raw_text.strip():
        return _RawEvidence()

    declared_total_cash: float | None = None
    declared_total_card: float | None = None
    declared_total_sales: float | None = None
    till_cash_total = 0.0
    till_card_total = 0.0
    z_reading_total = 0.0
    till_cash_count = 0
    till_card_count = 0
    z_reading_count = 0
    cash_over_total = 0.0
    cash_down_total = 0.0
    item_return_total = 0.0
    eftpos_reconciliation_total = 0.0
    cash_over_count = 0
    cash_down_count = 0
    item_return_count = 0
    eftpos_reconciliation_count = 0
    diagnostics_present = False

    for raw_line in raw_text.splitlines():
        stripped_line = raw_line.strip()
        match = _KEY_VALUE_PATTERN.match(stripped_line)
        if match is None:
            embedded_amount = _embedded_adjustment_amount(stripped_line)
            if embedded_amount is None:
                continue
            normalized_label, amount = embedded_amount
        else:
            normalized_label = _normalize_key(match.group(1))
            amount = parse_money(match.group(2))
            if amount is None:
                embedded_amount = _embedded_adjustment_amount(stripped_line)
                if embedded_amount is None:
                    continue
                normalized_label, amount = embedded_amount
            elif normalized_label == "cash variance":
                if _mentions_cash_over(stripped_line):
                    normalized_label = "cash over"
                elif _mentions_cash_down(stripped_line):
                    normalized_label = "cash down"
                else:
                    continue

        if normalized_label in _DECLARED_TOTAL_CASH_LABELS:
            declared_total_cash = amount
            diagnostics_present = True
            continue
        if normalized_label in _DECLARED_TOTAL_CARD_LABELS:
            declared_total_card = amount
            diagnostics_present = True
            continue
        if normalized_label in _DECLARED_TOTAL_SALES_LABELS:
            declared_total_sales = amount
            diagnostics_present = True
            continue
        if normalized_label in _TILL_CASH_LABELS:
            till_cash_total += amount
            till_cash_count += 1
            diagnostics_present = True
            continue
        if normalized_label in _TILL_CARD_LABELS:
            till_card_total += amount
            till_card_count += 1
            diagnostics_present = True
            continue
        if normalized_label in _Z_READING_LABELS:
            z_reading_total += amount
            z_reading_count += 1
            diagnostics_present = True
            continue
        if normalized_label in _CASH_OVER_LABELS:
            cash_over_total += abs(amount)
            cash_over_count += 1
            diagnostics_present = True
            continue
        if normalized_label in _CASH_DOWN_LABELS:
            cash_down_total += abs(amount)
            cash_down_count += 1
            diagnostics_present = True
            continue
        if normalized_label in _ITEM_RETURN_LABELS:
            item_return_total += -abs(amount) if amount >= 0 else amount
            item_return_count += 1
            diagnostics_present = True
            continue
        if normalized_label in _EFTPOS_RECONCILIATION_LABELS:
            eftpos_reconciliation_total += amount
            eftpos_reconciliation_count += 1
            diagnostics_present = True

    return _RawEvidence(
        declared_total_cash=declared_total_cash,
        declared_total_card=declared_total_card,
        declared_total_sales=declared_total_sales,
        till_cash_total=round(till_cash_total, 2) if till_cash_count else None,
        till_card_total=round(till_card_total, 2) if till_card_count else None,
        z_reading_total=round(z_reading_total, 2) if z_reading_count else None,
        cash_over=round(cash_over_total, 2) if cash_over_count else None,
        cash_down=round(cash_down_total, 2) if cash_down_count else None,
        item_return_adjustment=round(item_return_total, 2) if item_return_count else None,
        eftpos_reconciliation_adjustment=round(eftpos_reconciliation_total, 2)
        if eftpos_reconciliation_count
        else None,
        diagnostics_present=diagnostics_present,
    )


def _extract_metric_evidence(metrics: Mapping[str, Any] | None) -> _MetricEvidence:
    normalized_metrics = _normalized_metric_values(metrics)
    if not normalized_metrics:
        return _MetricEvidence()

    explicit_total_card = _number_from_normalized_metrics(
        normalized_metrics,
        "total_card",
        "t_card",
        "card_sales",
    )
    mobile_money_sales = _number_from_normalized_metrics(normalized_metrics, "mobile_money_sales", "mobile_money")
    declared_total_card = explicit_total_card
    if declared_total_card is None:
        eftpos_sales = _number_from_normalized_metrics(normalized_metrics, "eftpos_sales", "card_sales", "t_card")
        declared_total_card = _coalesce(_sum_money(eftpos_sales, mobile_money_sales), eftpos_sales, mobile_money_sales)

    cash_over = _abs_or_none(_number_from_normalized_metrics(normalized_metrics, "cash_over", "c_over"))
    cash_down = _abs_or_none(_number_from_normalized_metrics(normalized_metrics, "cash_down", "c_down"))
    item_return_adjustment = _number_from_normalized_metrics(
        normalized_metrics,
        "item_return_adjustment",
        "item_returns",
        "item_returns_total",
        "total_returns",
        "item_return",
        "return_adjustment",
        "returns",
        "return",
        "refund",
        "refunds",
        "refund_amount",
        "cash_adjustment_return",
    )
    if item_return_adjustment is not None and item_return_adjustment > 0:
        item_return_adjustment = -item_return_adjustment
    eftpos_reconciliation_adjustment = _number_from_normalized_metrics(
        normalized_metrics,
        "eftpos_reconciliation_adjustment",
        "card_reconciliation",
        "eftpos_reconciliation",
        "card_adjustment",
        "eftpos_adjustment",
        "card_variance",
        "eftpos_variance",
    )

    diagnostics_present = any(
        value is not None
        for value in (
            _number_from_normalized_metrics(normalized_metrics, "z_reading"),
            cash_over,
            cash_down,
            item_return_adjustment,
            eftpos_reconciliation_adjustment,
        )
    )

    return _MetricEvidence(
        declared_total_cash=_number_from_normalized_metrics(
            normalized_metrics,
            "total_cash",
            "cash_sales",
            "t_cash",
        ),
        declared_total_card=declared_total_card,
        declared_total_sales=_number_from_normalized_metrics(
            normalized_metrics,
            "net_sales",
            "gross_sales",
            "total_sales",
        ),
        declared_z_reading=_number_from_normalized_metrics(normalized_metrics, "z_reading"),
        cash_over=cash_over,
        cash_down=cash_down,
        item_return_adjustment=item_return_adjustment,
        eftpos_reconciliation_adjustment=eftpos_reconciliation_adjustment,
        diagnostics_present=diagnostics_present,
    )


def _normalized_metric_values(metrics: Mapping[str, Any] | None) -> dict[str, float]:
    if not isinstance(metrics, Mapping):
        return {}

    normalized: dict[str, float] = {}
    for key, value in metrics.items():
        if not isinstance(key, str):
            continue
        parsed = _to_float_or_none(value)
        if parsed is None:
            continue
        normalized[_normalize_key(key)] = parsed
    return normalized


def _number_from_normalized_metrics(normalized_metrics: Mapping[str, float], *aliases: str) -> float | None:
    for alias in aliases:
        value = normalized_metrics.get(_normalize_key(alias))
        if value is not None:
            return value
    return None


def _sum_money(left: float | None, right: float | None) -> float | None:
    parts = [value for value in (left, right) if value is not None]
    if len(parts) < 2:
        return None
    return round(sum(parts), 2)


def _money_or_zero(value: float | None) -> float:
    return round(value, 2) if value is not None else 0.0


def _abs_or_none(value: float | None) -> float | None:
    return None if value is None else round(abs(value), 2)


def _select_reconciliation_policy(
    *,
    operational_sales: float,
    declared_total_cash: float | None,
    declared_total_card: float | None,
    declared_total_sales: float | None,
    declared_z_reading: float | None,
    baseline_total_cash: float | None,
    baseline_total_card: float | None,
    cash_over: float,
    cash_down: float,
    item_return_adjustment: float,
    eftpos_reconciliation_adjustment: float,
) -> dict[str, float]:
    """Select the reconciliation policy that best matches the declared totals."""

    legacy_expected_total_sales = round(
        operational_sales
        + cash_over
        - cash_down
        + item_return_adjustment
        + eftpos_reconciliation_adjustment,
        2,
    )
    legacy_expected_z_reading = round(operational_sales + cash_down - cash_over, 2)

    z_includes_returns_expected_total_sales = round(
        operational_sales
        + cash_over
        - cash_down
        + eftpos_reconciliation_adjustment,
        2,
    )
    z_includes_returns_expected_z_reading = round(
        operational_sales - item_return_adjustment + cash_down - cash_over,
        2,
    )

    policies = (
        {
            "expected_total_cash": baseline_total_cash,
            "expected_total_card": baseline_total_card,
            "expected_total_sales": legacy_expected_total_sales,
            "expected_z_reading": legacy_expected_z_reading,
        },
        {
            "expected_total_cash": baseline_total_cash,
            "expected_total_card": baseline_total_card,
            "expected_total_sales": z_includes_returns_expected_total_sales,
            "expected_z_reading": z_includes_returns_expected_z_reading,
        },
    )
    if baseline_total_cash is not None and abs(item_return_adjustment) > TOLERANCE:
        policies = (
            *policies,
            {
                "expected_total_cash": round(baseline_total_cash + item_return_adjustment, 2),
                "expected_total_card": baseline_total_card,
                "expected_total_sales": z_includes_returns_expected_total_sales,
                "expected_z_reading": round(operational_sales + cash_down - cash_over, 2),
            },
        )
    return min(
        policies,
        key=lambda policy: _policy_distance(
            declared_total_cash=declared_total_cash,
            declared_total_card=declared_total_card,
            declared_total_sales=declared_total_sales,
            declared_z_reading=declared_z_reading,
            expected_total_cash=policy["expected_total_cash"],
            expected_total_card=policy["expected_total_card"],
            expected_total_sales=policy["expected_total_sales"],
            expected_z_reading=policy["expected_z_reading"],
        ),
    )


def _policy_distance(
    *,
    declared_total_cash: float | None,
    declared_total_card: float | None,
    declared_total_sales: float | None,
    declared_z_reading: float | None,
    expected_total_cash: float | None,
    expected_total_card: float | None,
    expected_total_sales: float,
    expected_z_reading: float,
) -> float:
    cash_distance = abs((declared_total_cash or expected_total_cash or 0.0) - (expected_total_cash or 0.0))
    card_distance = abs((declared_total_card or expected_total_card or 0.0) - (expected_total_card or 0.0))
    sales_distance = abs((declared_total_sales or expected_total_sales) - expected_total_sales)
    z_distance = abs((declared_z_reading or expected_z_reading) - expected_z_reading)
    return round(cash_distance + card_distance + sales_distance + z_distance, 2)


def _embedded_adjustment_amount(line: str) -> tuple[str, float] | None:
    """Return one embedded adjustment label and amount from a free-form line."""

    amounts = _extract_money_values(line)
    if not amounts:
        return None
    amount = round(sum(abs(value) for value in amounts), 2)
    if _mentions_cash_over(line):
        return ("cash over", amount)
    if _mentions_cash_down(line):
        return ("cash down", amount)
    normalized = _normalize_key(line)
    if any(
        token in normalized
        for token in ("item return", "items return", "returned item", "refund amount", "refund")
    ):
        return ("item return", amount)
    return None


def _extract_money_values(line: str) -> list[float]:
    values: list[float] = []
    for match in _MONEY_TOKEN_PATTERN.finditer(line):
        amount = parse_money(match.group(0))
        if amount is not None:
            values.append(amount)
    return values


def _mentions_cash_over(line: str) -> bool:
    normalized = _normalize_key(line)
    return (
        "cash over" in normalized
        or "c over" in normalized
        or ("cash variance" in normalized and "over" in normalized)
    )


def _mentions_cash_down(line: str) -> bool:
    normalized = _normalize_key(line)
    return (
        "cash down" in normalized
        or "c down" in normalized
        or ("cash variance" in normalized and "down" in normalized)
    )


def _money_differs(left: float | None, right: float | None) -> bool:
    return left is not None and right is not None and abs(left - right) > TOLERANCE


def _coalesce(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def _to_float_or_none(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return round(float(value), 2)
    return None


def _normalize_key(value: str) -> str:
    cleaned = value.casefold().strip().replace("_", " ").replace("/", " ").replace("-", " ")
    cleaned = _NORMALIZE_PATTERN.sub(" ", cleaned)
    return " ".join(cleaned.split())
