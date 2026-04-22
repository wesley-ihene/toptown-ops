# toptown-ops

`toptown-ops` is part of the TopTown AI Operations Platform (TAOP).

Within TAOP, `toptown-ops` is the Operations Engine. The separate `ioi-colony` repository is the Intelligence Engine. Together they form one integrated operational intelligence platform while remaining two distinct codebases.

Current flow:

WhatsApp -> Orchestra -> Orchestrator Agent -> Specialist Agents -> optional Fallback Agent -> Validation -> Acceptance -> accepted/review/rejected stores -> Signal Outbox -> IOI Colony

## Purpose

This repository handles WhatsApp ingestion, orchestration, specialist-agent work, governance, structured record generation, and export of approved signals. It is intentionally separate from `ioi-colony` so the Operations Engine can evolve independently from the downstream Intelligence Engine.

## Architecture Overview

TopTown AI Operations Platform (TAOP) is an integrated AI-driven operational intelligence platform for TopTown Clothing.

It consists of two core subsystems:

1. TopTown Ops (`toptown-ops`) - the Operations Engine responsible for ingestion, normalization, governance, and structured outputs.
2. IOI Colony (`ioi-colony`) - the Intelligence Engine responsible for consuming approved signals and generating downstream intelligence.

Together they form a unified pipeline:

`WhatsApp -> TopTown Ops -> Governance -> IOI Colony -> Intelligence Outputs`

## Role Separation

- Orchestra = operations manager
- Specialist Agents = domain specialists
- `toptown-ops` = TAOP Operations Engine
- `ioi-colony` = TAOP Intelligence Engine
- Codex = engineer
- Personal Assistant = CEO's right hand

## Relationship To IOI Colony

`toptown-ops` is upstream of `ioi-colony` within TAOP.

- Operations Engine responsibility: intake, classification, routing, specialist coordination, governance, structured records, standardized signal emission
- Intelligence Engine responsibility: `ioi-colony` consumes approved signals and performs downstream memory, reporting, ranking, and decision-support work

No production logic should be shared implicitly across this boundary. Integration should happen through explicit contracts only.

## Operational Records Platform

`toptown-ops` is the repo-local operational records and governance subsystem inside TAOP.

- Specialist agents create daily structured records.
- Structured records live in `records/structured`.
- Raw WhatsApp messages are stored in `records/raw`.
- Review items are stored in `records/review`.
- Rejected reports go to `records/rejected`.
- Provenance is stored in `records/provenance`.
- Learning proposals are stored in `records/proposals`.
- Daily observability summaries are stored in `records/observability`.
- `ioi-colony` is a separate repository that consumes exported approved signals.
- TAOP is an integrated platform, not a merged codebase.

Implementation details and current layer boundaries are documented in [docs/TOPTOWN_OPS_ARCHITECTURE.md](/home/clawadmin/.openclaw/workspace/toptown-ops/docs/TOPTOWN_OPS_ARCHITECTURE.md).
