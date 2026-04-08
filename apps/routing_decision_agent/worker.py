"""Combine intake signals into one explicit routing contract."""

from __future__ import annotations

from dataclasses import dataclass, field

from apps.branch_resolver_agent.worker import BranchResolution
from apps.date_resolver_agent.worker import DateResolution
from apps.header_normalizer_agent.worker import HeaderNormalizationResult
from apps.report_family_classifier_agent.worker import FamilyClassification
from packages.report_registry import route_for_family


@dataclass(slots=True)
class RoutingDecision:
    """Final routing contract for one raw WhatsApp report."""

    detected_report_type: str
    branch_hint: str | None
    report_date: str | None
    raw_report_date: str | None
    routing_target: str | None
    processing_status: str
    review_reason: str | None
    confidence: float
    evidence: list[str] = field(default_factory=list)
    normalized_header_candidates: list[str] = field(default_factory=list)
    specialist_report_type: str | None = None


def build_routing_decision(
    *,
    header_result: HeaderNormalizationResult,
    branch_resolution: BranchResolution,
    date_resolution: DateResolution,
    family_classification: FamilyClassification,
) -> RoutingDecision:
    """Return the explicit routing decision for one inbound message."""

    family_route = route_for_family(family_classification.report_family)
    review_reason = None
    processing_status = "routed"
    if family_classification.report_family == "unknown":
        processing_status = "needs_review"
        review_reason = "unknown_report_family"
    elif family_route.target_agent is None:
        processing_status = "needs_review"
        review_reason = "family_requires_manual_review"
    elif branch_resolution.branch_hint is None:
        processing_status = "needs_review"
        review_reason = "branch_unresolved"
    elif date_resolution.iso_date is None:
        processing_status = "needs_review"
        review_reason = "report_date_unresolved"

    evidence = (
        list(branch_resolution.evidence)
        + list(date_resolution.evidence)
        + list(family_classification.evidence)
    )
    confidence_inputs = [
        branch_resolution.confidence,
        date_resolution.confidence,
        family_classification.confidence,
    ]
    confidence = round(sum(confidence_inputs) / len(confidence_inputs), 4)

    return RoutingDecision(
        detected_report_type=family_classification.report_family,
        branch_hint=branch_resolution.branch_hint,
        report_date=date_resolution.iso_date,
        raw_report_date=date_resolution.raw_date,
        routing_target=family_route.target_agent,
        processing_status=processing_status,
        review_reason=review_reason,
        confidence=confidence,
        evidence=evidence,
        normalized_header_candidates=header_result.normalized_lines(),
        specialist_report_type=family_route.specialist_type,
    )
