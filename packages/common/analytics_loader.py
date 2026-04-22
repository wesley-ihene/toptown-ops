"""Shared file-backed analytics loaders for Phase 4 API and dashboard routes."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
import json
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from packages.branch_registry import CANONICAL_BRANCHES, canonical_branch_slug
from packages.common.paths import REPO_ROOT

BRANCH_ANALYTICS_PRODUCTS: dict[str, str] = {
    "staff": "staff_daily",
    "branch_daily": "branch_daily",
    "section": "section_daily",
}
COMPARISON_PRODUCT = "branch_comparison"


@dataclass(frozen=True, slots=True)
class AnalyticsNotFoundError:
    """Structured not-found error payload for analytics lookups."""

    product: str
    branch: str | None
    report_date: str | None
    expected_path: str | None
    message: str = "No analytics output matched the requested filters."

    def as_dict(self) -> dict[str, Any]:
        return {
            "error": "analytics_not_found",
            "message": self.message,
            "product": self.product,
            "branch": self.branch,
            "report_date": self.report_date,
            "expected_path": self.expected_path,
        }


def analytics_root(root: str | Path | None = None) -> Path:
    """Return the root analytics directory for a repo or repo override."""

    return (Path(root) if root is not None else REPO_ROOT) / "analytics"


def canonical_branch_or_none(value: str | None) -> str | None:
    """Return one canonical branch slug or `None` for blank values."""

    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return canonical_branch_slug(cleaned)


def display_branch_name(branch: str) -> str:
    """Return one readable branch label for dashboard and API metadata."""

    return CANONICAL_BRANCHES.get(branch, branch.replace("_", " ").title())


def list_available_branches(*, root: str | Path | None = None) -> list[dict[str, Any]]:
    """List branches present in any branch-scoped analytics product."""

    branch_dates = _available_branch_dates(analytics_root(root))
    return [
        {
            "slug": branch,
            "display_name": display_branch_name(branch),
            "available_dates": sorted(dates, reverse=True),
        }
        for branch, dates in sorted(branch_dates.items())
    ]


def list_available_dates(
    *,
    branch: str | None = None,
    root: str | Path | None = None,
) -> list[str]:
    """List available dates across analytics products, optionally for one branch."""

    branch_dates = _available_branch_dates(analytics_root(root))
    selected_branch = canonical_branch_or_none(branch)
    if selected_branch is not None:
        return sorted(branch_dates.get(selected_branch, set()), reverse=True)
    all_dates: set[str] = set()
    for dates in branch_dates.values():
        all_dates.update(dates)
    return sorted(all_dates, reverse=True)


def list_available_comparison_dates(*, root: str | Path | None = None) -> list[str]:
    """List available branch comparison dates."""

    comparison_root = analytics_root(root) / COMPARISON_PRODUCT
    if not comparison_root.exists():
        return []
    return sorted((path.stem for path in comparison_root.glob("*.json")), reverse=True)


def load_branch_analytics(
    product: str,
    *,
    branch: str,
    report_date: str,
    root: str | Path | None = None,
) -> tuple[dict[str, Any] | None, AnalyticsNotFoundError | None]:
    """Load one branch-scoped analytics JSON file by logical product name."""

    canonical_branch = canonical_branch_slug(branch)
    path = expected_analytics_path(
        product=product,
        branch=canonical_branch,
        report_date=report_date,
        root=root,
    )
    payload = _read_json(path)
    if payload is None:
        return None, AnalyticsNotFoundError(
            product=product,
            branch=canonical_branch,
            report_date=report_date,
            expected_path=_display_path(path, root=root),
        )
    return payload, None


def load_branch_comparison(
    *,
    report_date: str,
    root: str | Path | None = None,
) -> tuple[dict[str, Any] | None, AnalyticsNotFoundError | None]:
    """Load one branch comparison analytics JSON file."""

    path = expected_analytics_path(
        product=COMPARISON_PRODUCT,
        branch=None,
        report_date=report_date,
        root=root,
    )
    payload = _read_json(path)
    if payload is None:
        return None, AnalyticsNotFoundError(
            product=COMPARISON_PRODUCT,
            branch=None,
            report_date=report_date,
            expected_path=_display_path(path, root=root),
        )
    return payload, None


def expected_analytics_path(
    *,
    product: str,
    branch: str | None,
    report_date: str | None,
    root: str | Path | None = None,
) -> Path:
    """Return the expected file path for one analytics payload."""

    base = analytics_root(root)
    if report_date is None:
        return base
    if product in BRANCH_ANALYTICS_PRODUCTS:
        if branch is None:
            raise ValueError(f"branch is required for analytics product `{product}`")
        return base / BRANCH_ANALYTICS_PRODUCTS[product] / canonical_branch_slug(branch) / f"{report_date}.json"
    if product == COMPARISON_PRODUCT:
        return base / COMPARISON_PRODUCT / f"{report_date}.json"
    raise ValueError(f"unknown analytics product `{product}`")


def build_catalog(*, root: str | Path | None = None, branch: str | None = None) -> dict[str, Any]:
    """Return one deterministic analytics catalog for filters and navigation."""

    selected_branch = canonical_branch_or_none(branch)
    return {
        "analytics_root": _display_path(analytics_root(root), root=root),
        "available_branches": list_available_branches(root=root),
        "available_dates": list_available_dates(branch=selected_branch, root=root),
        "available_comparison_dates": list_available_comparison_dates(root=root),
        "selected_branch": selected_branch,
    }


def _available_branch_dates(root: Path) -> dict[str, set[str]]:
    branch_dates: dict[str, set[str]] = defaultdict(set)
    for product_dir in BRANCH_ANALYTICS_PRODUCTS.values():
        product_root = root / product_dir
        if not product_root.exists():
            continue
        for branch_dir in sorted(product_root.iterdir()):
            if not branch_dir.is_dir():
                continue
            for payload_path in branch_dir.glob("*.json"):
                branch_dates[branch_dir.name].add(payload_path.stem)
    return branch_dates


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, Mapping) else None


def _display_path(path: Path, *, root: str | Path | None = None) -> str:
    base_root = Path(root) if root is not None else REPO_ROOT
    try:
        return str(path.relative_to(base_root))
    except ValueError:
        return str(path)
