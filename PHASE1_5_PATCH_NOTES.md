# Phase 1.5 Patch Notes

## Scope

Phase 1.5 improves `staff_performance` structured quality on top of the completed Phase 1 intake hardening. Intake routing was not redesigned.

## What changed

- Strengthened section normalization with deterministic alias mapping in `packages/section_registry/`.
- Improved `staff_performance_agent` parsing so common retail section text resolves to stable canonical section slugs while always preserving `raw_section`.
- Added structured extraction for the trailing `Staff who work in price room:` block under `price_room_staff`.
- Added structured extraction for special assignment / continuation rows under `special_assignments`.
- Refined final status policy:
  - `accepted`
  - `accepted_with_warning`
  - `needs_review`
  - `invalid_input`
- Preserved and extended diagnostics:
  - `normalized_header_candidates`
  - `unmatched_lines`
  - `remainder_lines`
  - `section_resolution_stats`
  - `special_assignment_count`
  - `price_room_staff_count`

## Verified local commands

Targeted tests:

```bash
pytest -q tests/test_orchestrator_agent.py tests/test_replay_records.py tests/test_staff_performance_agent.py
```

Compile check:

```bash
python3 -m compileall apps packages scripts tests
```

Replay the exact archived Lae Malaita file:

```bash
python3 scripts/replay_records.py --source raw --mode orchestrator --path records/raw/whatsapp/unknown/2026-04-07__unknown__f10f29da4705.txt --overwrite
```

Expected structured output:

```text
records/structured/hr_performance/lae_malaita/2026-04-07.json
```

## Phase 1.5 outcome

- The same raw file still routes to `staff_performance_agent`.
- More performance rows now receive canonical `section` values.
- Price-room staff are captured structurally.
- The Julie Yorkie continuation row is captured structurally.
- The live sample now lands as `accepted_with_warning` instead of blanket `needs_review`.

## Phase 2 still open

- Broader retail section taxonomy coverage for long-tail merchandising phrases.
- Cross-message staff identity reconciliation and role normalization.
- Higher-order business logic for price-room assignments, slow-moving bale workflows, and downstream performance analytics.
