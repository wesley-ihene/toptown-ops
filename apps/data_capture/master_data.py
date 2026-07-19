"""Fail-closed adapters for branch roster, section, and product master data."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
from typing import Any

import yaml

from packages.common.paths import REPO_ROOT


@dataclass(frozen=True, slots=True)
class MasterData:
    """Read-only master-data view used by entry validation."""

    root: Path = REPO_ROOT
    database: Path | None = None

    def branches(self) -> list[str]:
        payload = self._yaml(self.root / "config" / "branches.yaml")
        rows = payload.get("branches", []) if isinstance(payload, dict) else []
        return [str(row.get("slug", "")).strip() for row in rows if isinstance(row, dict) and row.get("slug")]

    def staff(self, branch: str) -> list[str]:
        """Read the active DB roster, falling back only before the first import."""

        database_names = self._database_staff(branch)
        if database_names is not None:
            return database_names
        return self._markdown_staff().get(branch, [])

    def staff_seed(self) -> list[tuple[str, str]]:
        """Return active markdown rows for the explicit one-time importer."""

        return [
            (branch, name)
            for branch, names in self._markdown_staff().items()
            for name in names
        ]

    def _database_staff(self, branch: str) -> list[str] | None:
        if self.database is None or not self.database.exists():
            return None
        try:
            uri = f"file:{self.database.resolve()}?mode=ro"
            with sqlite3.connect(uri, uri=True) as connection:
                table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='staff'"
                ).fetchone()
                if table is None:
                    return None
                if connection.execute("SELECT COUNT(*) FROM staff").fetchone()[0] == 0:
                    return None
                rows = connection.execute(
                    "SELECT full_name FROM staff WHERE branch=? AND is_active=1 "
                    "ORDER BY full_name COLLATE NOCASE",
                    (branch,),
                )
                return [str(row[0]) for row in rows]
        except sqlite3.Error:
            # Once a populated DB exists, fail closed instead of silently using a stale file.
            return []

    def _markdown_staff(self) -> dict[str, list[str]]:
        """Parse active names from STAFF/master_staff_list.md branch tables.

        Expected headings are ``## <branch slug>``. The first table column is
        the staff name; an optional Active column accepts yes/true/active/1.
        """

        path = self.root / "STAFF" / "master_staff_list.md"
        if not path.exists():
            return {}
        current: str | None = None
        headers: list[str] = []
        result: dict[str, list[str]] = {}
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            heading = re.match(r"^##\s+(.+?)\s*$", raw_line)
            if heading:
                current = _slug(heading.group(1))
                headers = []
                continue
            if current is None or not raw_line.strip().startswith("|"):
                continue
            cells = [cell.strip() for cell in raw_line.strip().strip("|").split("|")]
            if not cells or all(re.fullmatch(r":?-+:?", cell) for cell in cells):
                continue
            if not headers:
                headers = [cell.casefold() for cell in cells]
                continue
            record = dict(zip(headers, cells))
            name = record.get("staff name") or record.get("name") or cells[0]
            active = record.get("active", "yes").casefold()
            if name and active in {"yes", "true", "active", "1", "y"}:
                result.setdefault(current, []).append(name)
        return {branch: _unique(names) for branch, names in result.items()}

    def sections(self, branch: str) -> list[str]:
        payload = self._yaml(self.root / "config" / "sections" / f"{branch}.yaml")
        section_root = payload.get("sections", {}) if isinstance(payload, dict) else {}
        rows = section_root.get("hr_performance", []) if isinstance(section_root, dict) else []
        values = []
        for row in rows:
            if isinstance(row, str):
                values.append(row)
            elif isinstance(row, dict):
                values.append(str(row.get("name") or row.get("slug") or ""))
        return _unique(values)

    def products(self) -> list[str]:
        payload = self._yaml(self.root / "config" / "products.yaml")
        rows = payload.get("products", []) if isinstance(payload, dict) else []
        return _unique(
            str(row.get("name", "")) if isinstance(row, dict) else str(row)
            for row in rows
        )

    def readiness(self, branch: str) -> list[str]:
        problems = []
        if branch not in self.branches():
            problems.append("Branch is not present in config/branches.yaml.")
        if not self.staff(branch):
            problems.append("Active staff roster is empty; import or add staff before capture.")
        if not self.sections(branch):
            problems.append(f"Staff-performance sections are empty in config/sections/{branch}.yaml.")
        if not self.products():
            problems.append("Product master is empty or config/products.yaml is missing; bale items will be flagged unresolved.")
        return problems

    @staticmethod
    def _yaml(path: Path) -> Any:
        if not path.exists():
            return {}
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")


def _unique(values: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in values:
        value = str(raw).strip()
        if value and value.casefold() not in seen:
            seen.add(value.casefold())
            result.append(value)
    return result
