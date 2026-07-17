"""Pure validation and reconciliation for data-capture submissions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from statistics import median
from typing import Any, Iterable

MONEY_QUANTUM = Decimal("0.01")
WEIGHT_QUANTUM = Decimal("0.001")
SUPERVISOR_ROLES = frozenset({"supervisor", "acting_supervisor"})
STAFF_ROLES = frozenset(
    {
        "cashier",
        "door guard (main)",
        "door guard (small)",
        "pricing room",
        "pricing clerk",
        "sales person",
        "till monitor",
        "supervisor",
        "none",
    }
)
ATTENDANCE_STATUSES = frozenset({"present", "off", "leave", "sick", "absent"})
ABSENT_ACTIVITY_STATUSES = frozenset({"off", "leave", "sick", "absent"})


class SubmissionError(ValueError):
    """A submission failed one or more defensible-entry rules."""

    def __init__(self, errors: Iterable[str], *, warnings: Iterable[str] = ()) -> None:
        self.errors = list(errors)
        self.warnings = list(warnings)
        super().__init__("; ".join(self.errors))


@dataclass(frozen=True, slots=True)
class ValidatedReport:
    """Normalized report ready for authoritative persistence."""

    report_type: str
    report_date: str
    lines: tuple[dict[str, Any], ...]
    metrics: dict[str, Any]
    details: dict[str, Any]
    warnings: tuple[str, ...] = ()


def validate_report_date(value: Any, *, today: date | None = None) -> tuple[str, list[str]]:
    """Validate an ISO report date and return non-blocking age warnings."""

    today = today or date.today()
    try:
        parsed = date.fromisoformat(str(value or ""))
    except ValueError as error:
        raise SubmissionError(["Report date must be a valid date."]) from error
    if parsed > today:
        raise SubmissionError(["Future report dates are not allowed."])
    warnings = []
    if (today - parsed).days > 3:
        warnings.append("Report date is more than three days old.")
    return parsed.isoformat(), warnings


def validate_sales(
    data: dict[str, Any],
    *,
    roster: Iterable[str],
    supervisors: Iterable[str],
    variance_threshold: Decimal = Decimal("5.00"),
    today: date | None = None,
) -> tuple[ValidatedReport, ValidatedReport]:
    """Validate one combined sales/control form and emit its two records."""

    report_date, warnings = validate_report_date(data.get("report_date"), today=today)
    errors: list[str] = []
    roster_by_key = _identity_map(roster)
    supervisors_by_key = _identity_map(supervisors)
    raw_rows = _rows(data.get("tills"))
    if not raw_rows:
        errors.append("At least one till row is required.")

    tills: list[dict[str, Any]] = []
    total_cash = Decimal("0")
    total_card = Decimal("0")
    total_z = Decimal("0")
    total_variance = Decimal("0")
    for index, raw in enumerate(raw_rows, 1):
        prefix = f"Till {index}"
        label = _required_text(raw.get("label"), f"{prefix} label", errors)
        cashier = _known_name(raw.get("cashier"), roster_by_key, f"{prefix} cashier", errors)
        assistant = _optional_known_name(raw.get("assistant"), roster_by_key, f"{prefix} assistant", errors)
        cash = _decimal(raw.get("cash"), f"{prefix} T/Cash", errors, quantum=MONEY_QUANTUM)
        card = _decimal(raw.get("card"), f"{prefix} T/Card", errors, quantum=MONEY_QUANTUM)
        z_reading = _decimal(raw.get("z_reading"), f"{prefix} Z/Reading", errors, quantum=MONEY_QUANTUM)
        variance = cash + card - z_reading
        reason = _text(raw.get("reason"))
        if abs(variance) > variance_threshold and not reason:
            errors.append(f"{prefix} needs an Over/Short Reason because variance exceeds K{variance_threshold:.2f}.")
        tills.append(
            {
                "record_number": index,
                "till_label": label,
                "cashier": cashier,
                "assistant": assistant,
                "cash": _money(cash),
                "card": _money(card),
                "z_reading": _money(z_reading),
                "variance": _money(variance),
                "balanced": abs(variance) <= variance_threshold,
                "over_short_reason": reason,
            }
        )
        total_cash += cash
        total_card += card
        total_z += z_reading
        total_variance += variance

    main_door = _integer(data.get("main_door"), "Main Door", errors)
    served = _integer(data.get("served"), "Guests / Customers Served", errors)
    staff_on_duty = _integer(data.get("staff_on_duty"), "Staff on Duty", errors)
    labour_hours = _decimal(data.get("labour_hours"), "Labour Hours", errors, quantum=WEIGHT_QUANTUM)
    if served > main_door:
        warnings.append("Customers served is greater than Main Door traffic.")
    total_sales = total_cash + total_card
    balanced_by = _known_name(data.get("balanced_by"), supervisors_by_key, "Balanced By", errors)
    notes = _text(data.get("notes"))

    staffing_issue = _choice_detail(data, "staffing_issue", "staffing_issue_detail", "none", errors)
    stock_issue = _choice_detail(data, "stock_issue", "stock_issue_detail", "no", errors)
    pricing_issue = _choice_detail(data, "pricing_issue", "pricing_issue_detail", "no", errors)
    escalated = _required_text(data.get("exceptions_escalated"), "Exceptions Escalated to Ops Manager", errors)
    variance_explanation = _text(data.get("cash_variance_explanation"))
    if total_variance != 0 and not variance_explanation:
        errors.append("A Cash Variance explanation is required when total variance is non-zero.")
    if not _truthy(data.get("supervisor_confirmation")):
        errors.append("Supervisor Confirmation is required.")
    if errors:
        raise SubmissionError(errors, warnings=warnings)

    sales_metrics = {
        "total_cash": _money(total_cash),
        "total_card": _money(total_card),
        "total_sales": _money(total_sales),
        "total_z_reading": _money(total_z),
        "total_variance": _money(total_variance),
        "main_door": main_door,
        "served": served,
        "staff_on_duty": staff_on_duty,
        "labour_hours": _decimal_text(labour_hours),
        "conversion_rate": _ratio(served, main_door),
        "sale_per_customer": _ratio(total_sales, served, money=True),
        "sales_per_labour_hour": _ratio(total_sales, labour_hours, money=True),
    }
    sales = ValidatedReport(
        report_type="sales_income",
        report_date=report_date,
        lines=tuple(tills),
        metrics=sales_metrics,
        details={"balanced_by": balanced_by, "notes": notes},
        warnings=tuple(warnings),
    )
    control = ValidatedReport(
        report_type="supervisor_control",
        report_date=report_date,
        lines=(),
        metrics={"cash_variance": _money(total_variance)},
        details={
            "cash_variance_explanation": variance_explanation,
            "staffing_issues": staffing_issue,
            "stock_issues_affecting_sales": stock_issue,
            "pricing_or_system_issues": pricing_issue,
            "exceptions_escalated": escalated,
            "supervisor_confirmation": True,
        },
        warnings=tuple(warnings),
    )
    return sales, control


def validate_staff_performance(
    data: dict[str, Any],
    *,
    roster: Iterable[str],
    sections: Iterable[str],
    attendance: dict[str, str] | None = None,
    today: date | None = None,
) -> ValidatedReport:
    """Validate staff activity against roster, sections, and attendance."""

    report_date, warnings = validate_report_date(data.get("report_date"), today=today)
    errors: list[str] = []
    roster_by_key = _identity_map(roster)
    section_by_key = _identity_map(sections)
    attendance = {key.casefold(): value for key, value in (attendance or {}).items()}
    raw_rows = _rows(data.get("staff"))
    if not raw_rows:
        errors.append("At least one staff row is required.")
    seen: set[str] = set()
    lines: list[dict[str, Any]] = []
    moved_total = 0
    assisting_total = 0
    for index, raw in enumerate(raw_rows, 1):
        staff_name = _known_name(raw.get("staff_name"), roster_by_key, f"Staff row {index} name", errors)
        section = _known_name(raw.get("section"), section_by_key, f"Staff row {index} section", errors)
        role = _text(raw.get("role")).casefold()
        if role not in STAFF_ROLES:
            errors.append(f"Staff row {index} has an invalid role.")
        identity = staff_name.casefold()
        if identity and identity in seen:
            errors.append(f"Duplicate staff row: {staff_name}.")
        seen.add(identity)
        moved = _integer(raw.get("items_moved"), f"{staff_name or f'Staff row {index}'} Total Items Moved", errors)
        assisting = _integer(raw.get("assisting"), f"{staff_name or f'Staff row {index}'} Assisting", errors)
        status = attendance.get(identity)
        if status in ABSENT_ACTIVITY_STATUSES and (moved or assisting):
            errors.append(f"{staff_name} is marked {status} in attendance and cannot have non-zero activity.")
        lines.append(
            {
                "record_number": index,
                "staff_name": staff_name,
                "section": section,
                "role": role,
                "items_moved": moved,
                "assisting_count": assisting,
                "attendance_status": status,
            }
        )
        moved_total += moved
        assisting_total += assisting
    if errors:
        raise SubmissionError(errors, warnings=warnings)
    return ValidatedReport(
        report_type="hr_performance",
        report_date=report_date,
        lines=tuple(lines),
        metrics={"total_items_moved": moved_total, "total_assisting": assisting_total, "staff_count": len(lines)},
        details={},
        warnings=tuple(warnings),
    )


def validate_attendance(
    data: dict[str, Any], *, roster: Iterable[str], today: date | None = None
) -> ValidatedReport:
    """Require exactly one canonical status for every active roster member."""

    report_date, warnings = validate_report_date(data.get("report_date"), today=today)
    errors: list[str] = []
    roster_by_key = _identity_map(roster)
    if not roster_by_key:
        errors.append("The active branch roster is empty; attendance cannot be submitted.")
    raw_statuses = data.get("statuses")
    statuses = raw_statuses if isinstance(raw_statuses, dict) else {}
    submitted = {str(key).casefold(): str(value).casefold() for key, value in statuses.items()}
    missing = [name for key, name in roster_by_key.items() if submitted.get(key) not in ATTENDANCE_STATUSES]
    unknown = [key for key in submitted if key not in roster_by_key]
    if missing:
        errors.append("Missing attendance status for: " + ", ".join(missing) + ".")
    if unknown:
        errors.append("Attendance contains staff not in the active roster.")
    if len(submitted) != len(roster_by_key):
        errors.append(f"Report size ({len(submitted)}) does not match active roster size ({len(roster_by_key)}).")
    if errors:
        raise SubmissionError(errors, warnings=warnings)
    counts = {status: 0 for status in ATTENDANCE_STATUSES}
    lines = []
    for index, (key, staff_name) in enumerate(roster_by_key.items(), 1):
        status = submitted[key]
        counts[status] += 1
        lines.append({"record_number": index, "staff_name": staff_name, "status": status})
    return ValidatedReport(
        report_type="hr_attendance",
        report_date=report_date,
        lines=tuple(lines),
        metrics={
            "present_count": counts["present"],
            "off_count": counts["off"],
            "annual_leave_count": counts["leave"],
            "sick_count": counts["sick"],
            "absent_count": counts["absent"],
            "total_staff_records": len(lines),
        },
        details={},
        warnings=tuple(warnings),
    )


def validate_bales(
    data: dict[str, Any], *, product_master: Iterable[str] = (), today: date | None = None
) -> ValidatedReport:
    """Validate bale rows, compute totals, and flag item/price resolution risks."""

    report_date, warnings = validate_report_date(data.get("report_date"), today=today)
    errors: list[str] = []
    products = _identity_map(product_master)
    rows = _rows(data.get("bales"))
    if not rows:
        errors.append("At least one bale row is required.")
    lines: list[dict[str, Any]] = []
    total_weight = Decimal("0")
    total_qty = 0
    total_amount = Decimal("0")
    unit_prices: list[tuple[int, Decimal]] = []
    for index, raw in enumerate(rows, 1):
        item = _required_text(raw.get("item"), f"Bale {index} item", errors)
        item_resolved = item.casefold() in products if products else False
        if not item_resolved:
            warnings.append(f"Bale {index} item is not resolved to the product master: {item}.")
        weight = _decimal(raw.get("weight"), f"Bale {index} Wt", errors, quantum=WEIGHT_QUANTUM)
        qty = _integer(raw.get("qty"), f"Bale {index} Qty", errors)
        amount = _decimal(raw.get("amount"), f"Bale {index} Amount", errors, quantum=MONEY_QUANTUM)
        unit_price = amount / qty if qty else None
        if unit_price is not None:
            unit_prices.append((index, unit_price))
        lines.append(
            {
                "record_number": index,
                "item_name": products.get(item.casefold(), item),
                "item_resolved": item_resolved,
                "weight_kg": _decimal_text(weight),
                "qty": qty,
                "amount": _money(amount),
                "price_per_piece": _money(unit_price) if unit_price is not None else None,
            }
        )
        total_weight += weight
        total_qty += qty
        total_amount += amount
    _validate_claimed_total(data, "claimed_total_qty", total_qty, "Total Qty", errors)
    _validate_claimed_decimal(data, "claimed_total_weight", total_weight, "Total Wt", errors, WEIGHT_QUANTUM)
    _validate_claimed_decimal(data, "claimed_total_amount", total_amount, "Total Amount", errors, MONEY_QUANTUM)
    if len(unit_prices) >= 3:
        center = Decimal(str(median([float(value) for _, value in unit_prices])))
        if center > 0:
            for index, value in unit_prices:
                if abs(value - center) / center > Decimal("0.50"):
                    warnings.append(f"Bale {index} unit price is more than 50% from the report median; review Qty and Amount.")
    if errors:
        raise SubmissionError(errors, warnings=warnings)
    return ValidatedReport(
        report_type="pricing_stock_release",
        report_date=report_date,
        lines=tuple(lines),
        metrics={
            "bales_released": len(lines),
            "total_weight_kg": _decimal_text(total_weight),
            "total_qty": total_qty,
            "total_amount": _money(total_amount),
        },
        details={},
        warnings=tuple(warnings),
    )


def _decimal(value: Any, label: str, errors: list[str], *, quantum: Decimal) -> Decimal:
    text = str(value if value is not None else "").strip()
    try:
        parsed = Decimal(text)
        if not parsed.is_finite() or parsed < 0:
            raise InvalidOperation
        if parsed.as_tuple().exponent < quantum.as_tuple().exponent:
            errors.append(f"{label} has too many decimal places.")
        return parsed.quantize(quantum, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        errors.append(f"{label} must be a non-negative number.")
        return Decimal("0").quantize(quantum)


def _integer(value: Any, label: str, errors: list[str]) -> int:
    text = str(value if value is not None else "").strip()
    try:
        if not text or Decimal(text) != Decimal(text).to_integral_value():
            raise ValueError
        parsed = int(text)
        if parsed < 0:
            raise ValueError
        return parsed
    except (InvalidOperation, OverflowError, ValueError):
        errors.append(f"{label} must be a non-negative whole number.")
        return 0


def _rows(value: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in value] if isinstance(value, list) else []


def _identity_map(values: Iterable[str]) -> dict[str, str]:
    return {str(value).strip().casefold(): str(value).strip() for value in values if str(value).strip()}


def _known_name(value: Any, known: dict[str, str], label: str, errors: list[str]) -> str:
    text = _text(value)
    resolved = known.get(text.casefold())
    if resolved is None:
        errors.append(f"{label} must be selected from current master data.")
        return text
    return resolved


def _optional_known_name(value: Any, known: dict[str, str], label: str, errors: list[str]) -> str | None:
    text = _text(value)
    return _known_name(text, known, label, errors) if text else None


def _required_text(value: Any, label: str, errors: list[str]) -> str:
    text = _text(value)
    if not text:
        errors.append(f"{label} is required.")
    return text


def _text(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    return str(value or "").strip().casefold() in {"1", "true", "yes", "on"}


def _money(value: Decimal | None) -> str | None:
    return format(value.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP), "f") if value is not None else None


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _ratio(numerator: Decimal | int, denominator: Decimal | int, *, money: bool = False) -> str | None:
    if not denominator:
        return None
    value = Decimal(numerator) / Decimal(denominator)
    return _money(value) if money else format(value.quantize(Decimal("0.0001")), "f")


def _choice_detail(
    data: dict[str, Any], choice_key: str, detail_key: str, negative_value: str, errors: list[str]
) -> dict[str, Any]:
    choice = _text(data.get(choice_key)).casefold()
    detail = _text(data.get(detail_key))
    if choice not in {negative_value, "describe"}:
        errors.append(f"{choice_key.replace('_', ' ').title()} must be selected.")
    if choice == "describe" and not detail:
        errors.append(f"{choice_key.replace('_', ' ').title()} details are required.")
    return {"has_issue": choice == "describe", "detail": detail or None}


def _validate_claimed_total(data: dict[str, Any], key: str, computed: int, label: str, errors: list[str]) -> None:
    if _text(data.get(key)):
        claimed = _integer(data.get(key), label, errors)
        if claimed != computed:
            errors.append(f"Entered {label} does not reconcile to row total {computed}.")


def _validate_claimed_decimal(
    data: dict[str, Any], key: str, computed: Decimal, label: str, errors: list[str], quantum: Decimal
) -> None:
    if _text(data.get(key)):
        claimed = _decimal(data.get(key), label, errors, quantum=quantum)
        if claimed != computed:
            errors.append(f"Entered {label} does not reconcile to row total {format(computed, 'f')}.")
