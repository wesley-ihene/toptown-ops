# toptown-ops

`toptown-ops` is the external upstream operations layer that will sit in front of IOI Colony.

Planned flow:

WhatsApp -> Orchestra -> Specialist Agents -> Signal Outbox -> IOI Colony

## Purpose

This repository will receive operational inputs, coordinate specialist work, and emit standardized signals for downstream processing by IOI Colony. It is intentionally separate from IOI Colony so upstream orchestration can evolve without changing the business brain directly.

## Role Separation

- Orchestra = operations manager
- Specialist Agents = domain specialists
- IOI Colony = brain of the business
- Codex = engineer
- Personal Assistant = CEO's right hand

## Relationship To IOI Colony

`toptown-ops` is upstream of IOI Colony.

- Upstream responsibility: intake, classification, routing, agent work coordination, standardized signal emission
- Downstream responsibility: IOI Colony consumes signals and performs colony-side memory, reporting, and decision support work

No production logic should be shared implicitly across this boundary. Integration should happen through explicit contracts only.

## First Planned Build Order

1. orchestra
2. income agent
3. hr agent
4. pricing agent
5. colony adapter
