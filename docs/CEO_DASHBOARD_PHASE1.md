# CEO Dashboard Phase 1

The active Phase 1 CEO dashboard is a read-only presentation layer over TAOP
operational facts.

## Routes

- HTML: `GET /ceo/vital-few?date=YYYY-MM-DD[&branch=<slug>][&show=all]`
- JSON: `GET /api/ceo/vital-few?date=YYYY-MM-DD[&branch=<slug>]`

The older `/ceo`, `/api/executive/*`, and `/api/ceo/*` routes other than the
new vital-few endpoint remain deprecated and hidden unless their existing
compatibility mode is used.

## Contract

The response uses `contract_version: ceo-kpi.v1`. Every KPI carries:

- current value and unit;
- comparison trend and sample coverage;
- trailing 8-week median target and sample coverage;
- an explicit threshold rule, boundary, and state;
- source system and current source-record paths;
- refresh cadence and data-completeness issues.

Daily trends compare the selected date with the previous seven calendar days.
The stock-release KPI uses a rolling seven-day value, compares it with the
previous seven-day period, and takes its baseline from the previous eight
non-overlapping seven-day periods. Enterprise values are recomputed from summed
numerators and denominators; branch percentages are not averaged.

The initial threshold rules are deliberately visible in the response:

- sales per labour hour, sales per active staff, and staff productivity: more
  than 15% below the trailing baseline;
- conversion: below the trailing baseline for two consecutive calendar days;
- stock release to sales: release value exceeds sales and is more than 15%
  above the trailing baseline;
- attendance: below 90%;
- absence/leave load: more than 15% above the trailing baseline.

## Boundaries

The module reads existing TAOP analytics and structured records. It writes no
state, calls no live endpoint, triggers no action, and does not reply to
WhatsApp. The single priority is a deterministic presentation of the largest
threshold deviation; it is advisory, and the CEO remains the decision-maker.

Financial KPIs remain gated until an accounting source supplies COGS, expenses,
cash, and budget data one-way. TAOP sales are labelled as revenue, not profit.
