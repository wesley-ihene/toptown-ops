# Phase 5 CEO Dashboard Patch Notes

## Scope

Phase 5 adds a read-only CEO dashboard and executive API layer on top of the existing analytics JSON outputs.

This patch does not redesign ingestion, parsing, or Phase 3 analytics calculations. All CEO views aggregate existing analytics files only.

## Router wiring update

- Wired the main portal API dispatcher to try `apps.ceo_api.routes` before the existing Phase 4 analytics router.
- `/api/ceo/*` and legacy `/api/executive/*` requests now resolve through the shared server entrypoint without changing Phase 4 route behavior.
- No breaking changes were introduced for `/api/analytics/*`.

## Added components

- `analytics/phase5_executive.py`
  - CEO overview aggregation by date
  - per-branch executive scorecards
  - cross-branch staff leadership summaries
  - cross-branch section leadership summaries
  - deterministic executive alerts with traceability
- `apps/ceo_api/`
  - read-only `/api/ceo/...` endpoint handlers
- `apps/ceo_dashboard_ui/`
  - lightweight server-rendered CEO dashboard HTML

## CEO routes

- `GET /api/ceo/overview?date=<date>`
- `GET /api/ceo/branches?date=<date>`
- `GET /api/ceo/staff?date=<date>`
- `GET /api/ceo/sections?date=<date>`
- `GET /api/ceo/alerts?date=<date>`
- `GET /api/ceo/catalog`
- `GET /api/ceo/dashboard?branch=<branch>&date=<date>`
- `GET /ceo?branch=<branch>&date=<date>`

Legacy `/api/executive/...` aliases remain available for backward compatibility, but the primary contract is now `/api/ceo/...`.

## Executive outputs

- Overview totals:
  - total gross sales
  - total active staff
  - total traffic
  - total served
  - branch coverage and data gaps
  - top and weakest branches by operational score
  - top branch by sales
  - top branch by conversion
- Branch scorecards:
  - gross sales
  - operational score
  - conversion rate
  - active staff count
  - staff productivity index
  - warning and flag counts
  - readiness and missing-input indicators
- Staff summaries:
  - top activity staff
  - top items staff
  - top assisting staff
  - weakest on-duty staff
  - idle on-duty staff
- Section summaries:
  - strongest sections
  - weakest sections
  - unresolved section hotspots
- Alerts:
  - critical and warning counts
  - deterministic alert rules
  - branch/date/source traceability

## Alert rules

- `branch_missing_sales_input`
- `branch_missing_staff_input`
- `record_needs_review`
- `record_accepted_with_warning`
- `low_conversion_rate`
- `idle_on_duty_staff`
- `unresolved_sections_present`
- `low_sales_per_active_staff`
- `high_warning_density`
- `branch_data_missing`

## Behavior guarantees

- CEO outputs are derived only from existing analytics files.
- CEO routes are read-only and file-backed.
- Missing analytics files return clean JSON 404 responses in the API.
- Missing or incomplete branch data surfaces as readiness gaps and executive alerts instead of crashing the app.
- Phase 4 operational routes remain intact.

## Verified commands

```bash
pytest -q tests/test_phase5_ceo_dashboard_api.py
pytest -q tests/test_phase4_dashboard_api.py
pytest -q tests/test_phase3_analytics.py
python3 -m compileall analytics apps packages scripts tests
python3 scripts/serve_phase4_dashboard.py --host 127.0.0.1 --port 8010
```
