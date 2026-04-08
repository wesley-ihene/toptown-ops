# IOI Colony Boundary

## Audit Result

`ioi-colony` is operating as the downstream intelligence and opportunity layer, but it still contains a substantial legacy raw-WhatsApp ingestion stack and several raw-input fallback assumptions.

Its mission and worker model are downstream-oriented:

- normalized signals feed colony memory
- colony workers rank or reinforce opportunities
- reports and blackboards are downstream decision-support outputs

That is consistent with the blueprint. The remaining issue is not role confusion at the mission level. The issue is that some legacy ingestion and fallback parsing still lives inside the downstream repo.

## Confirmed Downstream Role

Current files show that Colony is primarily the intelligence/opportunity system:

- `MISSION.md` defines IOI Colony as an autonomous intelligence system for ranked opportunities.
- `worker_decision_v2.py` consumes normalized signals and writes opportunity-oriented outputs.
- `sales_staff_fusion_summary.py`, `inventory_sales_fusion_summary.py`, `inventory_availability_summary.py`, and `staff_performance_summary.py` aggregate normalized data into downstream reports.
- `colony_cycle.py` orchestrates downstream health, audits, reports, and worker runs.

This confirms the intended downstream role is correct.

## Remaining Direct Raw-WhatsApp Assumptions

The following raw-input dependencies still remain inside `ioi-colony`.

### 1. Legacy raw WhatsApp ingress stack still lives in Colony

These files directly ingest, classify, quarantine, or parse WhatsApp-originated text:

- `scripts/whatsapp_webhook_bridge.py`
- `scripts/whatsapp_gatekeeper.py`
- `scripts/process_accepted_whatsapp.py`
- `scripts/watch_whatsapp.py`
- `scripts/run_whatsapp_batch.sh`
- `scripts/ingest_whatsapp_sales_batch.py`
- `scripts/parse_whatsapp_sales.py`
- `scripts/parse_whatsapp_staff.py`
- `scripts/parse_whatsapp_attendance.py`
- `scripts/parse_bale_summary.py`

This is the largest remaining boundary violation relative to the blueprint.

### 2. Colony cycle health checks still depend on WhatsApp directories and state

`colony_cycle.py` still checks:

- `RAW_INPUT/whatsapp/accepted`
- `DATA/processed_accepted_whatsapp.json`
- `DATA/whatsapp_pipeline_audit.json`

That means Colony runtime health still assumes ownership of raw-ingest operations.

### 3. Some downstream report builders still fall back to raw inputs

Examples:

- `inventory_sales_fusion_summary.py` falls back to `RAW_INPUT/<date>/sales/...` and accepts an explicit raw bale summary file.
- `inventory_availability_summary.py` searches `RAW_INPUT/<date>/operations/...` before or alongside normalized files.

These are direct raw-data assumptions inside downstream analytics code.

### 4. Opportunity logic still references WhatsApp processing state

`worker_decision_v2.py` reads `processed_accepted_whatsapp.json` and treats `RAW_INPUT/whatsapp/` as a valid source linkage prefix.

This is not raw parsing, but it still couples core colony logic to the legacy WhatsApp intake layer.

### 5. Tests still certify the old WhatsApp pipeline inside Colony

`tests/test_whatsapp_pipeline.py` validates the end-to-end raw WhatsApp bridge, gatekeeper, quarantine, and parser workflow inside the Colony repo.

That confirms the old boundary is still active rather than merely archived.

## Boundary Decision

Target boundary:

- `toptown-ops` owns raw WhatsApp intake, routing, specialist parsing, and structured records.
- `ioi-colony` owns normalized-signal consumption, memory, fusion, ranking, and opportunity outputs.
- No Colony worker should need to read raw WhatsApp text or raw-ingest state once the bridge is complete.

The clean rule is:

`toptown-ops records/structured -> explicit adapter -> ioi-colony SIGNALS/normalized -> colony memory / reports / opportunities`

## Handoff Contract

The handoff from `toptown-ops` into `ioi-colony` should be implemented as an explicit adapter, not by moving Colony parsers upstream and not by letting Colony read `records/raw/...`.

### Source Inputs

Authoritative upstream inputs:

- `records/structured/sales_income/<branch>/<YYYY-MM-DD>.json`
- `records/structured/hr_performance/<branch>/<YYYY-MM-DD>.json`
- `records/structured/hr_attendance/<branch>/<YYYY-MM-DD>.json`
- `records/structured/pricing_stock_release/<branch>/<YYYY-MM-DD>.json`

### Bridge Ownership

The bridge should live outside Colony core logic. Practical options:

- `toptown-ops/scripts/export_colony_signals.py`
- a dedicated adapter module under `toptown-ops/packages`
- a separate bridge repo if cross-repo deployment later requires it

It should not be implemented by reusing Colony raw WhatsApp parsers.

### Canonical Output Target

Canonical downstream write target:

- `ioi-colony/SIGNALS/normalized/<branch>/<YYYY-MM-DD>/<signal_file>.json`

This gives Colony one date-and-branch-scoped JSON event layer. Because Colony still has legacy readers, the bridge may temporarily dual-write compatibility files until those consumers are migrated.

### Required Envelope

Every bridged normalized event should include:

- `signal_id`
- `signal_type`
- `event_kind`
- `branch`
- `branch_slug`
- `report_date`
- `source_system`
- `source_record_type`
- `source_record_path`
- `source_record_sha256`
- `contract_version`
- `payload`
- `warnings`

Rules:

- `branch` and `branch_slug` must both resolve to the canonical slug
- `report_date` must be ISO `YYYY-MM-DD`
- `source_system` must be `toptown_ops`
- the bridge must not fabricate missing metrics
- missing upstream families produce partial downstream coverage, not invented zeros

### Record-Type Mapping

Recommended first-pass mapping:

- `sales_income` -> `daily_sales_report`
- `hr_performance` -> `staff_performance_report`
- `hr_attendance` -> `staff_attendance_report`
- `pricing_stock_release` -> `daily_bale_summary_report`

Recommended payload mapping:

#### `sales_income -> daily_sales_report`

Bridge into a JSON event that preserves the Colony-compatible families already used by downstream code:

- `totals`
- `traffic`
- `staffing`
- `control`
- `notes`

At minimum the event must carry:

- total sales inputs
- traffic and served counts when present
- staff on duty when present
- cash variance and supervisor/control fields when present

#### `hr_performance -> staff_performance_report`

Bridge into a JSON event with:

- `staff_records`
- per-staff arrangement/display/performance values when present
- canonical and raw section fields when available

This preserves compatibility with Colony staff summarization without requiring raw WhatsApp parsing.

#### `hr_attendance -> staff_attendance_report`

Bridge into a JSON event with:

- `attendance_records`
- `attendance_totals`
- `declared_totals` when available from upstream records

This keeps attendance as a normalized downstream fact even if current opportunity workers do not consume it heavily yet.

#### `pricing_stock_release -> daily_bale_summary_report`

Bridge into a JSON event with:

- `bales`
- `totals`
- release operator metadata when present

This replaces the current raw bale-summary fallback path.

### Explicit Non-Mappings

The current upstream repo does not yet provide direct structured equivalents for some legacy Colony normalized families, especially:

- `inventory_availability_report`
- `supervisor_control_report`

Those should remain legacy-only until an upstream specialist and structured record contract is defined for them. They should not be silently synthesized from unrelated records.

## Migration Sequence

1. Implement the bridge from `toptown-ops records/structured` into Colony JSON normalized events.
2. Dual-write any legacy compatibility paths only where current Colony consumers still require them.
3. Remove raw fallback reads from downstream analytics.
4. Remove WhatsApp-specific health checks from `colony_cycle.py`.
5. Retire the WhatsApp webhook, gatekeeper, and parser stack from Colony after parity is verified.

## Boundary Status

Aligned:

- Colony mission is downstream intelligence and opportunity ranking
- normalized signals already exist as a downstream operating concept
- downstream report and memory layers are clearly Colony concerns

Misaligned:

- Colony still owns legacy WhatsApp ingress
- some downstream readers still fall back to raw input
- core health and decision logic still reference WhatsApp processing artifacts

Migration-required:

- move all raw-intake ownership to `toptown-ops`
- make the structured-record bridge the only supported upstream handoff
- progressively remove direct raw-WhatsApp assumptions from Colony
