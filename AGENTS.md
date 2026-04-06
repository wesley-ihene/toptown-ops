# AGENTS.md

## Mission

Build `toptown-ops` as a minimal, safe upstream system that stays outside IOI Colony while preparing a clean path for future integration.

## Core Rules

1. Keep Orchestra and specialist agents outside IOI Colony.
2. Preserve separation of concerns between intake, orchestration, specialist work, and downstream colony processing.
3. Do not let agents reply directly to WhatsApp.
4. Route all downstream outputs through a standardized signal contract.
5. Make the smallest safe patch needed for each change.
6. Avoid fake business logic, hidden transformations, and silent fallbacks.
7. Keep modules importable and scaffolding reversible.

## Boundary Rules

- `toptown-ops` is the upstream coordination layer.
- IOI Colony remains the downstream business brain.
- Cross-repo integration must use explicit adapters or contracts.
- Do not blur responsibilities across Orchestra, agents, and IOI Colony.

## Patching Rules

- Read before writing.
- Prefer minimal, surgical, reversible edits.
- Do not introduce live endpoint connections during scaffolding.
- Do not add dependencies unless they are explicitly required.
- Do not change IOI Colony production logic as part of work in this repo.

## Signal Discipline

- Standardize outputs before they leave Orchestra or any specialist agent.
- Preserve a single downstream path: Signal Outbox -> IOI Colony.
- Keep contracts explicit, typed where practical, and easy to version later.
