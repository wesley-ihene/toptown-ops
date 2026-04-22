# Duplication Cleanup Wave 1

Wave 1 is documentation and ownership clarification only within the TopTown AI Operations Platform (TAOP).

Scope:
- freeze duplicated TopTown executive/intelligence modules
- record the TopTown Ops (Operations Engine) vs IOI Colony (Intelligence Engine) ownership boundary in code and docs
- define the Phase 3 split between retained facts and deprecated interpretation

Ownership rule:
- TopTown Ops answers: what happened operationally?
- IOI Colony answers: what matters, why, and what should we do?

Frozen modules in Wave 1:
- `analytics/phase5_executive.py`
- `packages/common/executive_alerts.py`
- `apps/ceo_api/routes.py`
- `apps/ceo_dashboard_ui/routes.py`

Phase 3 split rule:
- Keep factual KPI aggregation in TopTown Ops.
- Treat interpretive ranking, strongest/weakest framing, executive scoring, and
  downstream-style intelligence as deprecated.
- Do not remove Phase 3 code yet.
- Do not alter outputs yet.

What remains in TopTown Ops for now:
- operator-facing dashboard and analytics infrastructure
- structured record computation helpers such as `analytics/ceo_metrics.py`
- compatibility surfaces needed until later cleanup waves

Out of scope in Wave 1:
- hiding or removing routes
- proxying or cutover work
- automation changes
- dashboard redesign
- file deletions
- runtime behavior changes

Planned later waves:
- Wave 2: decouple callers and hide deprecated CEO/executive surfaces
- Wave 3: remove thin wrappers and executive aggregation layers after callers
  are gone
