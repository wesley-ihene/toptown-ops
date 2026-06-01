"""Normalize legacy ghost branch directories into canonical branch slugs."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
from typing import Any

BRANCH_MOVES: dict[str, str] = {
    "ttc_bena_road_goroka": "bena_road",
    "ttc_pom_waigani_branch": "waigani",
}
MOVE_ROOTS = (
    Path("records/structured"),
    Path("records/review"),
    Path("analytics"),
)
SCAN_ROOTS = (
    Path("records/structured"),
    Path("records/review"),
    Path("records/rejected"),
    Path("records/duplicates"),
    Path("analytics"),
)
BRANCH_SCALAR_FIELDS = {
    "branch",
    "branch_slug",
    "branch_hint",
    "attempted_branch_hint",
}
BRANCH_MAP_FIELDS = {
    "branches",
    "by_branch",
}


@dataclass(slots=True)
class MovePlan:
    source: str
    target: str
    action: str
    note: str


@dataclass(slots=True)
class RewritePlan:
    path: str
    changed_fields: list[str]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Move legacy ghost branch files into canonical branch directories."
    )
    parser.add_argument("--apply", action="store_true", help="Apply planned moves and rewrites.")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]
    move_plans = _plan_moves(repo_root)
    rewrite_plans = _plan_rewrites(repo_root)
    backup_root = _backup_root(repo_root) if args.apply and (move_plans or rewrite_plans) else None

    if args.apply:
        applied_moves = _apply_moves(repo_root, move_plans, backup_root=backup_root)
        _prune_empty_ghost_directories(repo_root)
        rewrite_plans = _plan_rewrites(repo_root)
        applied_rewrites = _apply_rewrites(repo_root, rewrite_plans, backup_root=backup_root)
    else:
        applied_moves = []
        applied_rewrites = []

    summary = {
        "mode": "apply" if args.apply else "dry_run",
        "backup_root": None if backup_root is None else str(backup_root.relative_to(repo_root)),
        "branch_moves": BRANCH_MOVES,
        "planned_moves": [asdict(plan) for plan in move_plans],
        "planned_rewrites": [asdict(plan) for plan in rewrite_plans],
        "applied_moves": [asdict(plan) for plan in applied_moves],
        "applied_rewrites": [asdict(plan) for plan in applied_rewrites],
        "counts": {
            "planned_moves": len(move_plans),
            "planned_rewrites": len(rewrite_plans),
            "applied_moves": len(applied_moves),
            "applied_rewrites": len(applied_rewrites),
        },
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def _plan_moves(repo_root: Path) -> list[MovePlan]:
    plans: list[MovePlan] = []
    for relative_root in MOVE_ROOTS:
        root = repo_root / relative_root
        if not root.exists():
            continue
        for source_path in sorted(path for path in root.rglob("*") if path.is_file()):
            relative_path = source_path.relative_to(repo_root)
            normalized_relative_path = _normalized_relative_path(relative_path)
            if normalized_relative_path == relative_path:
                continue

            target_path = repo_root / normalized_relative_path
            if not target_path.exists():
                action = "move"
                note = "target_missing"
            elif _same_file_content(source_path, target_path):
                action = "dedupe"
                note = "identical_target_exists"
            else:
                action = "conflict_move"
                conflict_target = _conflict_target(target_path, relative_path)
                target_path = conflict_target
                note = "target_exists_with_different_content"

            plans.append(
                MovePlan(
                    source=str(relative_path),
                    target=str(target_path.relative_to(repo_root)),
                    action=action,
                    note=note,
                )
            )
    return plans


def _plan_rewrites(repo_root: Path) -> list[RewritePlan]:
    plans: list[RewritePlan] = []
    for relative_root in SCAN_ROOTS:
        root = repo_root / relative_root
        if not root.exists():
            continue
        for path in sorted(candidate for candidate in root.rglob("*.json") if candidate.is_file()):
            payload = _read_json(path)
            if payload is None:
                continue
            rewritten, changed_fields = _rewrite_branch_payload(payload)
            if rewritten == payload:
                continue
            plans.append(
                RewritePlan(
                    path=str(path.relative_to(repo_root)),
                    changed_fields=sorted(changed_fields),
                )
            )
    return plans


def _apply_moves(repo_root: Path, plans: list[MovePlan], *, backup_root: Path | None) -> list[MovePlan]:
    applied: list[MovePlan] = []
    for plan in plans:
        source_path = repo_root / plan.source
        target_path = repo_root / plan.target
        if not source_path.exists():
            continue
        _backup_file(source_path, repo_root=repo_root, backup_root=backup_root)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if plan.action == "dedupe":
            source_path.unlink()
        else:
            shutil.move(str(source_path), str(target_path))
        applied.append(plan)
    return applied


def _apply_rewrites(repo_root: Path, plans: list[RewritePlan], *, backup_root: Path | None) -> list[RewritePlan]:
    applied: list[RewritePlan] = []
    for plan in plans:
        path = repo_root / plan.path
        payload = _read_json(path)
        if payload is None:
            continue
        rewritten, changed_fields = _rewrite_branch_payload(payload)
        if rewritten == payload:
            continue
        _backup_file(path, repo_root=repo_root, backup_root=backup_root)
        _write_json(path, rewritten)
        applied.append(RewritePlan(path=plan.path, changed_fields=sorted(changed_fields)))
    return applied


def _normalized_relative_path(relative_path: Path) -> Path:
    return Path(*(_normalized_path_part(part) for part in relative_path.parts))


def _normalized_path_part(part: str) -> str:
    if part in BRANCH_MOVES:
        return BRANCH_MOVES[part]

    path_part = Path(part)
    suffix = "".join(path_part.suffixes)
    stem = part[: -len(suffix)] if suffix else part
    if stem in BRANCH_MOVES:
        return BRANCH_MOVES[stem] + suffix
    return part


def _rewrite_branch_payload(payload: Any) -> tuple[Any, set[str]]:
    changed_fields: set[str] = set()

    def rewrite(value: Any, *, parent_key: str | None = None) -> Any:
        if isinstance(value, dict):
            if parent_key in BRANCH_MAP_FIELDS:
                return _rewrite_branch_map(value, changed_fields=changed_fields)

            output: dict[str, Any] = {}
            for key, child in value.items():
                rewritten_child = rewrite(child, parent_key=key)
                if key in BRANCH_SCALAR_FIELDS and isinstance(rewritten_child, str):
                    normalized_branch = BRANCH_MOVES.get(rewritten_child)
                    if normalized_branch is not None and normalized_branch != rewritten_child:
                        rewritten_child = normalized_branch
                        changed_fields.add(key)
                output[key] = rewritten_child
            return output
        if isinstance(value, list):
            return [rewrite(child, parent_key=parent_key) for child in value]
        return value

    rewritten = rewrite(payload)
    return rewritten, changed_fields


def _rewrite_branch_map(value: dict[str, Any], *, changed_fields: set[str]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, child in value.items():
        normalized_key = BRANCH_MOVES.get(key, key)
        rewritten_child, nested_fields = _rewrite_branch_payload(child)
        changed_fields.update(nested_fields)
        if normalized_key != key:
            changed_fields.add("branch_map_key")

        if normalized_key not in output:
            output[normalized_key] = rewritten_child
            continue

        existing = output[normalized_key]
        if _is_number(existing) and _is_number(rewritten_child):
            output[normalized_key] = existing + rewritten_child
            continue
        if existing == rewritten_child:
            continue
        output[normalized_key] = existing
    return output


def _same_file_content(source_path: Path, target_path: Path) -> bool:
    return _sha256_path(source_path) == _sha256_path(target_path)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _conflict_target(target_path: Path, relative_source_path: Path) -> Path:
    digest = hashlib.sha256(str(relative_source_path).encode("utf-8")).hexdigest()[:8]
    suffix = "".join(target_path.suffixes)
    stem = target_path.name[: -len(suffix)] if suffix else target_path.name
    return target_path.with_name(f"{stem}__normalized_conflict__{digest}{suffix}")


def _backup_root(repo_root: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_root = repo_root / "tmp" / "branch_normalization_backup" / timestamp
    backup_root.mkdir(parents=True, exist_ok=True)
    return backup_root


def _backup_file(path: Path, *, repo_root: Path, backup_root: Path | None) -> None:
    if backup_root is None or not path.exists():
        return
    backup_path = backup_root / path.relative_to(repo_root)
    if backup_path.exists():
        return
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, backup_path)


def _prune_empty_ghost_directories(repo_root: Path) -> None:
    candidate_dirs: list[Path] = []
    for relative_root in MOVE_ROOTS:
        root = repo_root / relative_root
        if not root.exists():
            continue
        for ghost_branch in BRANCH_MOVES:
            candidate_dirs.extend(path for path in root.rglob(ghost_branch) if path.is_dir())

    for directory in sorted(candidate_dirs, key=lambda path: len(path.parts), reverse=True):
        if any(directory.iterdir()):
            continue
        directory.rmdir()


def _read_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_json(path: Path, payload: Any) -> None:
    serialized = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)
    path.write_text(f"{serialized}\n", encoding="utf-8")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


if __name__ == "__main__":
    raise SystemExit(main())
