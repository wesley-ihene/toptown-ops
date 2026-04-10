# toptown-ops

`toptown-ops` is the external upstream operations layer in front of IOI Colony.

Current flow:

WhatsApp -> Orchestra -> Orchestrator Agent -> Specialist Agents -> optional Fallback Agent -> Validation -> Acceptance -> accepted/review/rejected stores -> Signal Outbox -> IOI Colony

## Purpose

This repository receives operational inputs, coordinates specialist work, applies upstream policy and review controls, and emits standardized signals for downstream processing by IOI Colony. It is intentionally separate from IOI Colony so upstream orchestration can evolve without changing the business brain directly.

## Role Separation

- Orchestra = operations manager
- Specialist Agents = domain specialists
- IOI Colony = brain of the business
- Codex = engineer
- Personal Assistant = CEO's right hand

## Relationship To IOI Colony

`toptown-ops` is upstream of IOI Colony.

- Upstream responsibility: intake, classification, routing, agent work coordination, standardized signal emission
- Downstream responsibility: IOI Colony consumes signals and performs colony-side memory, reporting, ranking, and decision support work

No production logic should be shared implicitly across this boundary. Integration should happen through explicit contracts only.

## Operational Records Platform

`toptown-ops` is the operational records platform.

- Specialist agents create daily structured records.
- Structured records live in `records/structured`.
- Raw WhatsApp messages are stored in `records/raw`.
- Review items are stored in `records/review`.
- Rejected reports go to `records/rejected`.
- Provenance is stored in `records/provenance`.
- Learning proposals are stored in `records/proposals`.
- Daily observability summaries are stored in `records/observability`.
- `ioi-colony` is a separate system that consumes exported signals.
- Do not merge the two systems.

Implementation details and current layer boundaries are documented in [docs/TOPTOWN_OPS_ARCHITECTURE.md](/home/clawadmin/.openclaw/workspace/toptown-ops/docs/TOPTOWN_OPS_ARCHITECTURE.md).
