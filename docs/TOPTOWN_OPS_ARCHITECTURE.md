# TopTown Ops Architecture

`toptown-ops` is part of the TopTown AI Operations Platform (TAOP). Within TAOP, this repository is the Operations Engine. The separate `ioi-colony` repository is the Intelligence Engine. Together they form one integrated platform with an explicit upstream-to-downstream handoff.

This repo owns intake, orchestration, specialist parsing, upstream review/reject handling, provenance, proposal generation, observability, and structured signal export. It does not own downstream intelligence, colony memory, or direct WhatsApp replies.

## Platform Architecture

TopTown AI Operations Platform (TAOP):

- TopTown Ops (Operations Engine): ingestion, normalization, governance, structured outputs
- IOI Colony (Intelligence Engine): consumes approved signals and produces downstream intelligence

Pipeline:

`WhatsApp -> TopTown Ops -> Governance -> IOI Colony -> Intelligence Outputs`

## Layered Agent Design

Current flow:

`WhatsApp intake -> Orchestra -> Orchestrator Agent -> strict specialist -> optional fallback extraction -> validation -> acceptance -> accepted/review/rejected storage -> signal export -> IOI Colony`

Layer responsibilities:

- `packages/whatsapp_ingest` and `apps/orchestra`: intake, raw payload normalization, conservative routing.
- `apps/orchestrator_agent`: raw archive, policy checks, specialist dispatch, fallback dispatch, downstream outcome coordination.
- strict specialists under `apps/*_agent`: schema-specific parsing as the primary path.
- `apps/fallback_extraction_agent`: schema-bound recovery extraction only after strict parsing fails.
- `packages/sop_validation`: required fields, totals and cross-field checks, canonical branch/date normalization.
- `packages/report_acceptance`: accept/review/reject decisions based on validation output plus report-type confidence thresholds.

## Policy Guard

`apps/orchestrator_agent/policy_guard.py` runs before specialist execution. It enforces:

- mixed-report rejection
- duplicate and idempotency checks
- report-type-aware fallback eligibility
- early hard rejects

Policy outcomes are persisted into raw and rejected metadata under `policy_guard`.

## Validation vs Acceptance

Validation and acceptance are separate layers by design.

- Validation answers: "Is the structured payload internally valid and normalized?"
- Acceptance answers: "What should happen to a valid or invalid result?"

Validation does not decide routing. Acceptance does not repeat validation rules.

Outcomes:

- valid + high confidence -> `accept`
- valid + medium confidence -> `review`
- invalid or low confidence -> `reject`

Thresholds and fallback settings are file-based in `config/report_policy.json` and are report-type aware.

## Fallback Agent

Fallback extraction is a secondary path, not a replacement for specialists.

Rules:

- it runs only after strict specialist parsing fails
- it is enabled per report type by config
- it returns schema-bound output with `parse_mode`, `confidence`, `warnings`, and `provenance`
- it still passes through Validation and Acceptance before any record is accepted, reviewed, or rejected

Current supported report families:

- `sales`
- `bale_summary` / `pricing_stock_release`
- `attendance`
- `staff_performance`
- `supervisor_control`

## Review Queue

Review items are stored separately from accepted structured records and rejected records.

- accepted records: `records/structured/...`
- review queue: `records/review/<date>/<branch>/<report_type>/<message_hash>.json`
- rejected records: `records/rejected/...`

Each review item includes:

- `report_type`
- `branch`
- `date`
- `confidence`
- `warnings`
- `provenance`
- `reason`

The file layout keeps the queue queryable by date, branch, and report type.

## Provenance

Provenance is stored separately from business payloads in `records/provenance/<outcome>/<date>/<branch>/<report_type>/<raw_message_hash>.json`.

Each provenance record captures:

- raw message hash
- parser used
- parse mode
- confidence
- warnings
- validation outcome
- acceptance outcome
- downstream record or export references

This keeps business records stable while preserving traceability across accept, review, and reject paths.

## Learning Proposals

`packages/review_learning` analyzes repeated fallback patterns from persisted provenance and writes proposal-only artifacts to `records/proposals/...`.

Proposal categories currently include:

- new alias candidates
- parser rule candidates
- prompt improvement suggestions
- confidence threshold adjustment proposals

The learning layer does not mutate live config, prompts, parsers, or production thresholds. Review is explicit and manual.

## Observability

Observability is lightweight and file-based. Daily summary artifacts are written under `records/observability/daily/<date>/summary.json`.

Current metrics include:

- intake volume
- accept, review, and reject counts
- fallback activation count and rate
- per-agent processed, failed, and failure-rate metrics
- branch-wise warning and low-confidence quality signals
- colony export success and failure counts

Observability is updated from processing provenance and export automation rather than from a separate service.

## Storage Summary

Primary stores:

- raw intake: `records/raw/whatsapp/...`
- accepted structured records: `records/structured/...`
- review queue: `records/review/...`
- rejected records: `records/rejected/whatsapp/...`
- provenance: `records/provenance/...`
- learning proposals: `records/proposals/...`
- observability summaries: `records/observability/daily/...`

## IOI Colony Boundary

The boundary with `ioi-colony` stays explicit inside TAOP.

`toptown-ops` owns:

- raw WhatsApp intake
- orchestration and policy enforcement
- specialist and fallback parsing
- validation, acceptance, review, rejection
- upstream storage, provenance, proposals, and observability
- export of standardized downstream signals

`ioi-colony` owns:

- normalized signal consumption
- downstream memory
- fusion, reporting, ranking, and decision support

The contract remains one-way:

`toptown-ops -> Signal Outbox / exported signals -> ioi-colony`

The Intelligence Engine should not read raw WhatsApp inputs or upstream internal state directly.
