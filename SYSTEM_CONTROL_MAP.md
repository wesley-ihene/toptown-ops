# SYSTEM CONTROL MAP — TAOP CORE

## Purpose

This document freezes the operating architecture of TAOP Core.

The objective is to ensure intelligence grows inside a controlled machine, not inside chaos.

---

# 1. Authoritative System Flow

WhatsApp
→ Orchestra
→ Orchestrator Agent
→ Specialist Agents
→ Validation
→ Acceptance / Review / Rejection
→ Signal Outbox
→ IOI Colony
→ Intelligence Outputs

This flow is mandatory.

No agent may bypass it.

---

# 2. Engine Separation

## TopTown Ops = Operations Engine

Responsibilities:
- message intake
- classification
- routing
- parsing
- normalization
- validation
- approval/rejection
- structured record creation
- signal export

## IOI Colony = Intelligence Engine

Responsibilities:
- downstream intelligence
- memory
- reporting
- analytics
- ranking
- forecasting
- decision support

IOI Colony must not control operational intake or approvals.

---

# 3. Authority Rules

## Orchestrator Agent

The Orchestrator is the central authority.

It controls:
- routing
- specialist selection
- flow coordination
- escalation
- signal handoff

## Specialist Agents

Specialist agents are workers.

They may:
- parse
- classify
- calculate
- recommend
- structure reports

They may NOT:
- directly reply to WhatsApp
- bypass validation
- bypass acceptance
- export directly to IOI Colony
- invent missing business rules

---

# 4. Governance Rules

All operational outputs must pass through:

1. validation
2. acceptance decision
3. structured record storage
4. signal contract generation

Outputs must be:
- traceable
- reversible
- explainable
- contract-driven

---

# 5. Normalization Rules

There must be one canonical source for:

- branch normalization
- product normalization
- section normalization
- date normalization
- label normalization

Duplicate alias tables are prohibited.

Unknown values must fail explicitly or enter review.

No silent fallback.

---

# 6. Signal Contract Rules

Only accepted structured records may enter Signal Outbox.

Signal outputs must include:
- source message reference
- branch
- date
- report type
- structured payload
- validation status
- provenance
- export timestamp

---

# 7. WhatsApp Reply Rules

Agents must not reply directly to WhatsApp.

All outbound replies must pass through:
- response engine
- outbound reply agent
- approved message contract

---

# 8. Review / Rejection Rules

Reports must enter review when:
- required fields are missing
- branch cannot be resolved
- item/category cannot be resolved
- pricing governance fails
- supervisor override is unclear
- duplicate report risk exists

Reports must be rejected when:
- critical fields are invalid
- data is contradictory
- governance breach is unresolved
- required approval is absent

---

# 9. Repository Boundary Rules

## toptown-ops

May produce governed operational signals.

Must not contain IOI Colony intelligence logic.

## ioi-colony

May consume approved signals.

Must not mutate upstream operational records.

Integration must happen only through explicit contracts.

---

# 10. Forbidden Actions

Do not:
- merge toptown-ops and ioi-colony
- create direct agent-to-WhatsApp replies
- create hidden business logic
- duplicate normalization logic
- introduce silent fallbacks
- bypass Orchestrator
- bypass validation
- export unaccepted records
- rewrite full pipeline without approval

---

# 11. Engineering Mode

Default mode:

SAFE SYSTEM EVOLUTION

Rules:
- read before writing
- verify before modifying
- make minimal reversible changes
- preserve backward compatibility
- reduce duplication
- reduce ambiguity
- reduce runtime fragility

---

# 12. Freeze Decision

This architecture is frozen as the governing map for TAOP Core.

Future development must comply with this document unless formally updated.
