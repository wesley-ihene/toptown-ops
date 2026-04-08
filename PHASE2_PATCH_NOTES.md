# Phase 2 Patch Notes

## Scope

Phase 2 adds deterministic mixed-report detection, explicit section splitting, routing fan-out, supervisor-control support, and split lineage metadata without changing the existing single-report specialist pipeline.

## What changed

- Added `apps/mixed_content_detector_agent/` for message-level mixed-content classification:
  - `single_report`
  - `mixed_report`
  - `single_report_with_noncritical_trailing_notes`
- Added `apps/report_splitter_agent/` for deterministic segment generation with:
  - `segment_id`
  - `segment_index`
  - `detected_report_family`
  - `raw_text`
  - `start_line`
  - `end_line`
  - `split_confidence`
- Added `apps/supervisor_control_agent/` so mixed `sales + supervisor control` messages produce two real structured outputs.
- Wired the live `orchestrator_agent` to:
  - detect explicit mixed report headers
  - split one raw message into multiple child work items
  - route each child through the existing single-report specialist path
  - return one parent result with `fanout` summaries, `outputs`, and `lineage`
- Preserved the single-report flow when no clean mixed split is detected.
- Extended replay handling so one mixed parent replay can write multiple structured artifacts.
- Extended webhook output discovery to expose all mixed child outputs.

## Mixed split strategy

- Strategy: `explicit_report_headers`
- Current explicit child families supported:
  - `sales_income`
  - `attendance`
  - `pricing_stock_release`
  - `staff_performance`
  - `supervisor_control`

## Added lineage metadata

- Parent payload:
  - `lineage.message_role = split_parent`
  - `routing.split_strategy`
  - `routing.child_count`
  - `routing.child_report_types`
- Child payload summaries:
  - `message_role = split_child`
  - `segment_id`
  - `segment_index`
  - `parent_message_hash`
  - `parent_raw_txt_path`
  - `parent_raw_sha256`
  - `split_source_agent`
  - `derived_from_mixed_report = true`
  - `child_count`
  - `child_report_family`
  - `header_line_number`

## Verified commands

Targeted tests:

```bash
pytest -q tests/test_orchestrator_agent.py tests/test_replay_records.py tests/test_staff_performance_agent.py
```

Compile check:

```bash
python3 -m compileall apps packages scripts tests
```

Replay the stable mixed-report fixture:

```bash
python3 scripts/replay_records.py --source raw --mode orchestrator --path tests/fixtures/records/raw/whatsapp/unknown/2026-04-07__unknown__mixed_supervisor_control_sample.txt --overwrite
```

Expected outputs:

```text
records/structured/sales_income/waigani/2026-04-07.json
records/structured/supervisor_control/waigani/2026-04-07.json
```

## Current boundary

- The single-report path remains unchanged.
- Mixed detection is deterministic and explicit-header-based.
- Phase 2 still does not add fuzzy mixed inference across loosely interleaved body text without explicit family boundaries.
