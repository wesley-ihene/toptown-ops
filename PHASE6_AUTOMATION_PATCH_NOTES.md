# Phase 6 Automation Patch Notes

Phase 6 adds automatic downstream post-processing after every successful structured write.

## What changed

- Added [automation.py](/home/clawadmin/.openclaw/workspace/toptown-ops/packages/record_store/automation.py)
  - shared postprocess helper for one `branch` and `report_date`
  - rebuilds:
    - `analytics/branch_daily/<branch>/<date>.json`
    - `analytics/staff_daily/<branch>/<date>.json`
    - `analytics/section_daily/<branch>/<date>.json`
    - `analytics/branch_comparison/<date>.json`
  - exports fresh IOI Colony signals via existing `scripts/export_colony_signals.py` library functions
  - emits explicit structured logs for start, completion, and failure states

- Updated [writer.py](/home/clawadmin/.openclaw/workspace/toptown-ops/packages/record_store/writer.py)
  - runs postprocess automatically after a successful structured JSON write
  - derives the effective repo root from the actual structured path
  - keeps the structured write committed even if downstream automation fails
  - logs downstream automation failures with branch/date/report type context

- Added [test_phase6_automation.py](/home/clawadmin/.openclaw/workspace/toptown-ops/tests/test_phase6_automation.py)
  - structured write triggers analytics rebuild
  - structured write triggers Colony export
  - repeat writes refresh outputs safely
  - branch comparison rebuild occurs for the affected date
  - downstream failure does not erase structured success
  - replay path triggers the same automation flow

## Trigger path

Automation is triggered from the shared structured write completion path:

- specialist worker
- `packages.record_store.writer.write_structured(...)`
- postprocess helper

This means both live ingestion and replay-driven specialist writes use the same downstream refresh behavior.

## Failure behavior

If postprocess fails after the structured file is written:

- the structured output remains on disk
- analytics or export may be partially refreshed up to the point of failure
- an explicit `downstream_automation_failure` log is emitted with:
  - `branch`
  - `report_date`
  - `affected_record_types`
  - `output_paths`
  - `status`
  - `error`

Manual recovery remains available through the existing rebuild and export commands.

## IOI Colony root resolution

Automatic export resolves IOI Colony in this order:

1. explicit `colony_root` argument
2. `TOPTOWN_IOI_COLONY_ROOT`
3. sibling repo at `../ioi-colony`

If none are available, postprocess logs a failure after the structured write succeeds.

## Verification commands

```bash
pytest -q tests/test_phase6_automation.py tests/test_phase3_analytics.py tests/test_export_colony_signals.py tests/test_orchestrator_agent.py tests/test_replay_records.py
python3 -m compileall packages/record_store tests/test_phase6_automation.py
```
