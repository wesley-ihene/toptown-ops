# Phase 7 WhatsApp Executive Alerts Patch Notes

Phase 7 adds deterministic, file-backed executive alert generation after successful post-processing.

## What changed

- Added [executive_alerts.py](/home/clawadmin/.openclaw/workspace/toptown-ops/packages/common/executive_alerts.py)
  - builds executive alerts from existing analytics and CEO aggregation outputs
  - writes daily summary and per-branch alert artifacts under `alerts/executive/<date>/`
  - adds deterministic `dedupe_key` values
  - classifies alert severities as `critical`, `warning`, or `info`
  - supports required alert classes:
    - `low_conversion_rate`
    - `idle_on_duty_staff`
    - `branch_missing_sales_input`
    - `branch_missing_staff_input`
    - `unresolved_sections_present`
    - `record_needs_review`
    - `record_accepted_with_warning`
    - `low_sales_per_active_staff`
    - `weak_branch_operational_score`
    - `critical_branch_gap`

- Added [whatsapp_alert_formatter.py](/home/clawadmin/.openclaw/workspace/toptown-ops/packages/common/whatsapp_alert_formatter.py)
  - formats summary and per-branch alerts into compact WhatsApp-ready preview text

- Updated [automation.py](/home/clawadmin/.openclaw/workspace/toptown-ops/packages/record_store/automation.py)
  - post-process order is now:
    1. analytics rebuild
    2. branch comparison rebuild
    3. Colony export
    4. executive alert generation
  - alert artifacts are refreshed idempotently with overwrite enabled

- Updated [routes.py](/home/clawadmin/.openclaw/workspace/toptown-ops/apps/ceo_api/routes.py)
  - `/api/ceo/alerts?date=<date>` returns the daily summary artifact
  - `/api/ceo/alerts/feed?date=<date>` returns the WhatsApp-ready summary preview
  - `/api/ceo/alerts/branch?branch=<branch>&date=<date>` returns one branch alert artifact
  - legacy WhatsApp alert aliases remain supported

## Alert artifact paths

- Daily summary JSON:
  - `alerts/executive/<date>/summary.json`
- Daily summary WhatsApp preview:
  - `alerts/executive/<date>/summary.whatsapp.txt`
- Per-branch JSON:
  - `alerts/executive/<date>/<branch>.json`
- Per-branch WhatsApp preview:
  - `alerts/executive/<date>/<branch>.whatsapp.txt`

## API routes

- `GET /api/ceo/alerts?date=<date>`
- `GET /api/ceo/alerts/feed?date=<date>`
- `GET /api/ceo/alerts/branch?branch=<branch>&date=<date>`

Executive aliases are also supported under `/api/executive/...`.

## Outbound WhatsApp sending

Outbound WhatsApp sending was intentionally deferred.

Reason:
- the repo has an inbound WhatsApp webhook bridge, but no clean, production-safe outbound delivery integration point for executive alert sending
- this patch stops at deterministic artifacts plus WhatsApp-ready preview messages

## Verification commands

```bash
pytest -q tests/test_phase5_ceo_dashboard_api.py tests/test_phase6_automation.py
python3 -m compileall analytics apps packages tests/test_phase5_ceo_dashboard_api.py tests/test_phase6_automation.py
```
