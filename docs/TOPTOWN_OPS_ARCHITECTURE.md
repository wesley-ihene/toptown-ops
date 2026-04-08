# Toptown Ops Architecture

## Audit Result

`toptown-ops` is aligned with the blueprint as the upstream specialist-agent intake and normalization layer.

The repo already separates:

- raw intake and routing
- specialist parsing and normalization
- structured record storage
- downstream export as a distinct future step

This is consistent with the repo mission in `AGENTS.md` and `README.md`: Orchestra and specialist agents stay outside IOI Colony, raw WhatsApp does not flow directly into colony logic, and the downstream path is intended to be contract-driven.

## Confirmed System Role

The current repo behavior confirms `toptown-ops` is the upstream operational layer:

- `apps/orchestra/intake.py` creates raw work items from inbound messages.
- `apps/orchestrator_agent/worker.py` archives raw input, classifies it conservatively, routes it to a specialist, and treats structured records as the usable source of truth.
- Specialist workers under `apps/sales_income_agent`, `apps/hr_agent`, and `apps/pricing_stock_release_agent` parse domain-specific reports into structured outputs.
- Structured outputs are written under `records/structured/...` through the shared record-store helpers in `packages/record_store`.
- CEO analytics reads only structured records and writes derived outputs only under `REPORTS/ceo/...`.

The current repo does not implement the downstream bridge yet. `scripts/export_colony_signals.py` is still a placeholder, which means the boundary is conceptually correct but not fully wired.

## Current Upstream Flow

Current effective flow:

`raw inbound message -> Orchestra intake -> orchestrator routing -> specialist agent -> records/structured/<record_type>/<branch>/<date>.json`

Current structured record families:

- `sales_income`
- `hr_performance`
- `hr_attendance`
- `pricing_stock_release`

Current storage boundaries:

- raw archive: `records/raw/whatsapp/...`
- rejected/quarantine: `records/rejected/whatsapp/...`
- structured truth: `records/structured/...`
- derived CEO reports: `REPORTS/ceo/...`

## Boundary Interpretation

Inside `toptown-ops`:

- raw message intake
- classification
- specialist parsing
- branch and date normalization
- structured record writing
- upstream analytics built only on structured records

Outside `toptown-ops`:

- income opportunity ranking
- colony memory and reinforcement
- blackboard logic
- report aggregation for colony intelligence
- any direct WhatsApp reply path

That split matches the intended upstream/downstream separation.

## Structured Truth Contract

The stable upstream truth currently available to downstream systems is:

- `records/structured/sales_income/<branch>/<YYYY-MM-DD>.json`
- `records/structured/hr_performance/<branch>/<YYYY-MM-DD>.json`
- `records/structured/hr_attendance/<branch>/<YYYY-MM-DD>.json`
- `records/structured/pricing_stock_release/<branch>/<YYYY-MM-DD>.json`

Contract assumptions already visible in the repo:

- branch is expected to be canonical
- file date is ISO `YYYY-MM-DD`
- structured records are read-only once written
- downstream analytics should tolerate missing record families and produce partial outputs instead of fabricating values

## Alignment Against Blueprint

Aligned:

- upstream orchestration is outside IOI Colony
- specialist logic is outside IOI Colony
- raw WhatsApp storage is upstream, not mixed into Colony code in this repo
- structured records are the normalized source of truth
- derived analytics stay separate from ingestion outputs

Misaligned:

- the downstream export bridge is not implemented yet
- the signal contract package is still only minimally typed
- the record schema helpers still reflect placeholder shapes rather than a fully locked cross-repo export schema

Migration-required:

- implement the explicit adapter from `records/structured/...` into Colony normalized signals
- version the handoff contract before enabling cross-repo automation
- keep all downstream Colony compatibility logic inside the adapter, not inside specialist workers
