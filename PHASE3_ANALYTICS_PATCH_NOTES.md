# Phase 3 Analytics Patch Notes

## Scope

Phase 3 adds a deterministic analytics layer on top of existing structured `sales_income` and `hr_performance` records. Ingestion and routing were not redesigned.

## Added analytics builders

- `analytics/phase3.py`
  - branch daily analytics
  - staff leaderboard analytics
  - section productivity analytics
  - branch comparison analytics

## Added analytics agents

- `apps/branch_daily_analytics_agent/`
- `apps/section_productivity_agent/`
- `apps/staff_leaderboard_agent/`
- `apps/branch_comparison_agent/`

## Added scripts

- `scripts/build_branch_daily_analytics.py`
- `scripts/build_staff_leaderboard.py`
- `scripts/build_section_productivity.py`
- `scripts/build_branch_comparison.py`

## Output paths

- `analytics/branch_daily/<branch>/<date>.json`
- `analytics/staff_daily/<branch>/<date>.json`
- `analytics/section_daily/<branch>/<date>.json`
- `analytics/branch_comparison/<date>.json`

## Deterministic rules

- All branch handling uses canonical branch slugs.
- Missing structured inputs emit warnings instead of crashing.
- Labor hours are derived from `gross_sales / sales_per_labor_hour` when not explicitly present in the structured sales record.
- Staff daily outputs include deterministic `summary_counts` plus ranked `top_activity_score` entries.
- Section productivity uses the explicit formula `productivity_index = (items_moved * 0.6) + (assisting_count * 0.4)`.
- Branch comparison rankings are stable and deterministic with branch slug tie-breaks.
- All analytics payloads preserve source-record traceability.

## Verified commands

Targeted tests:

```bash
pytest -q tests/test_phase3_analytics.py
```

Compile check:

```bash
python3 -m compileall analytics apps scripts tests
```

Generate analytics locally:

```bash
python3 scripts/build_branch_daily_analytics.py --branch waigani --date 2026-04-07 --overwrite
python3 scripts/build_staff_leaderboard.py --branch waigani --date 2026-04-07 --overwrite
python3 scripts/build_section_productivity.py --branch waigani --date 2026-04-07 --overwrite
python3 scripts/build_branch_comparison.py --date 2026-04-07 --overwrite
```
