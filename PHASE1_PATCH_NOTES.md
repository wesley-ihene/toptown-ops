# Phase 1 Patch Notes

## Scope

Phase 1 hardens upstream WhatsApp intake for branch/date/report-family detection and routes `staff_performance` reports into a dedicated deterministic specialist parser.

## Added intake agents

- `apps/header_normalizer_agent/`
- `apps/branch_resolver_agent/`
- `apps/date_resolver_agent/`
- `apps/report_family_classifier_agent/`
- `apps/routing_decision_agent/`
- `apps/field_canonicalizer_agent/`
- `apps/staff_status_resolver_agent/`
- `apps/staff_performance_agent/`

## Key behavior changes

- Header scanning now checks the first 8 non-empty lines instead of line 1 only.
- Branch aliases are resolved deterministically through `packages/branch_registry/`.
- Dates like `TUESDAY 07 /04/26`, `07/04/26`, `Monday 30/03/26`, and `2026-04-07` normalize to ISO.
- Report-family classification now exposes confidence and evidence and supports:
  - `staff_performance`
  - `staff_sales`
  - `sales_income`
  - `attendance`
  - `pricing_stock_release`
  - `supervisor_control`
- Raw metadata now keeps explicit routing evidence:
  - `resolved_report_date`
  - `raw_report_date`
  - `routing_confidence`
  - `routing_evidence`
  - `normalized_header_candidates`
  - `routing_review_reason`
  - `specialist_report_type`
- `staff_performance` routes to `staff_performance_agent`.
- Staff performance parsing now tolerates mixed line styles including:
  - `🔹Total items moved (51)`
  - `Items: 29`
  - `Item:-`
  - `Assist (07)`
  - `Assists:07`
  - `Asssist: 07`
- Unmatched lines are preserved under `diagnostics.unmatched_lines`.

## Verified replay commands

Run the exact failing archived message through orchestrator replay:

```bash
python3 scripts/replay_records.py --source raw --mode orchestrator --path records/raw/whatsapp/unknown/2026-04-07__unknown__f10f29da4705.txt
```

Expected structured output:

```text
records/structured/hr_performance/lae_malaita/2026-04-07.json
```

Expected routing outcome:

- `detected_report_type = staff_performance`
- `branch_hint = lae_malaita`
- `routing_target = staff_performance_agent`
- `resolved_report_date = 2026-04-07`

## Targeted test command

```bash
pytest -q tests/test_orchestrator_agent.py tests/test_replay_records.py tests/test_staff_performance_agent.py
```
