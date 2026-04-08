# Phase 4 Dashboard Patch Notes

## Scope

Phase 4 adds a read-only operational API and browser dashboard on top of the existing analytics JSON outputs under `analytics/`.

This patch does not redesign ingestion, analytics calculations, or downstream colony behavior.

## Added components

- `packages/common/analytics_loader.py`
  - shared analytics path logic
  - branch/date discovery
  - safe JSON loading
  - structured not-found errors
- `apps/dashboard_api/`
  - file-backed read-only API route handlers
- `apps/dashboard_ui/`
  - server-rendered dashboard page builders
- `analytics/phase4_portal.py`
  - lightweight HTTP dispatcher and server wrapper
- `scripts/serve_phase4_dashboard.py`
  - local server entrypoint

## API routes

- `GET /api/analytics/staff?branch=<branch>&date=<date>`
- `GET /api/analytics/branch_daily?branch=<branch>&date=<date>`
- `GET /api/analytics/section?branch=<branch>&date=<date>`
- `GET /api/analytics/branch_comparison?date=<date>`
- `GET /api/analytics/branches`
- `GET /api/analytics/dates?branch=<branch>`
- `GET /api/dashboard?branch=<branch>&date=<date>`
- `GET /dashboard`
- `GET /health`

## Dashboard views

- Executive Overview
- Staff Performance View
- Sales vs Staffing Efficiency View
- Section Productivity View
- Branch Comparison View

## Behavior guarantees

- All API reads are file-backed from existing analytics outputs.
- Missing analytics files return clean JSON 404s in the API layer.
- Missing analytics files render helpful 404 or partial-data messages in the dashboard.
- Branch and date filters are deterministic and derived from files already present under `analytics/`.
- The dashboard does not recalculate analytics values.

## Verified commands

```bash
pytest -q tests/test_phase4_dashboard_api.py
pytest -q tests/test_phase3_analytics.py
python3 -m compileall analytics apps packages scripts tests
python3 scripts/serve_phase4_dashboard.py --host 127.0.0.1 --port 8010
```
