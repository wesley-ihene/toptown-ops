# Phase 2.1 Patch Notes

## Scope

Phase 2.1 is a targeted cleanup for mixed-report fan-out. It does not redesign Phase 2 routing or introduce new downstream behavior.

## What changed

- Sales mixed segments now use the shared upstream branch canonicalization path, so `TTC WAIGANI BRANCH` resolves to `waigani` before structured path selection.
- Sales structured writes now normalize both branch and report date before writing, matching the stricter HR write path.
- Mixed parent orchestrator payloads now expose:
  - `output_path`
  - `output_paths`
  - `derived_output_paths`
  - `segment_count`
  - `written_count`
- Mixed child summaries now expose per-segment:
  - `report_family`
  - `branch`
  - `report_date`
  - `output_paths`
- Replay manifest entries for mixed fan-out now expose:
  - `written_count`
  - `segment_count`
  - `derived_output_paths`
  - `segments`
- Replay summary `written` now counts actual structured artifacts written, not just raw parent records processed.

## Regression coverage

- Mixed `sales + staff_performance` replay writes two outputs.
- Replay summary reports `written = 2` for that run.
- Mixed sales segment canonicalizes `TTC WAIGANI BRANCH` to `waigani`.
- No mixed replay writes to `records/structured/.../ttc_waigani_branch/...`.

## Local verification

Targeted tests:

```bash
pytest -q tests/test_orchestrator_agent.py tests/test_replay_records.py
```

Optional compile check:

```bash
python3 -m compileall apps packages scripts tests
```

Replay the existing mixed raw sample:

```bash
python3 scripts/replay_records.py --source raw --mode orchestrator --path records/raw/whatsapp/unknown/2026-04-07__waigani__be04464cdfe0.txt --overwrite
```

Expected structured outputs:

```text
records/structured/sales_income/waigani/2026-04-07.json
records/structured/hr_performance/waigani/2026-04-07.json
```

Expected manifest behavior:

- the mixed result includes `written_count: 2`
- the mixed result includes `segment_count: 2`
- `summary.written` is `2`
- no output path contains `ttc_waigani_branch`
