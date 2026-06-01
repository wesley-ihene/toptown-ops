# AGENT CONTROL MATRIX

Scope:
- Sources read: `SYSTEM_CONTROL_MAP.md`, `AGENTS.md`, `README.md`
- Code inspected: all folders under `apps/`, plus proven callers outside `apps/` where needed to confirm runtime wiring

Control rules used:
- `ACTIVE` means a live caller is proven from one of these runtime surfaces: `scripts/whatsapp_webhook_bridge.py`, `apps/orchestrator_agent/worker.py`, `packages/record_store/automation.py`, or `analytics/phase4_portal.py`
- `UNKNOWN` means live runtime wiring is not proven; script-only, test-only, placeholder, or empty modules stay `UNKNOWN`
- `Production path` is judged against the frozen system flow in `SYSTEM_CONTROL_MAP.md`, not just “does some code import it”
- `Allowed caller`, `allowed callee`, `allowed record store`, and `allowed authorities` are conservative control assignments derived from the frozen control map plus proven current wiring. They are not blanket permission to expand usage.

Production path legend:
- `YES`: inside the authoritative operational report path
- `YES (review)`: manual review/rejection branch of the authoritative path
- `YES (documented)`: architecture says it belongs in path, but live wiring is not proven
- `NO`: runtime surface outside the authoritative path
- `NO (compat)`: compatibility or deprecated surface outside the authoritative path

Risk legend:
- `LOW`: limited blast radius; read-only or narrow helper
- `MEDIUM`: can change routing, state, or operator visibility indirectly
- `HIGH`: can change governance, reply behavior, or persisted operational state
- `CRITICAL`: central authority or high-impact path with broad write/decision power

## Intake And Orchestration

| app | layer | runtime status | production path | primary role | allowed caller | allowed callee | allowed record store | allowed authorities | risk level | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `orchestra` | intake/orchestration | `UNKNOWN` | `YES (documented)` | Legacy intake, classify, route, split helpers | WhatsApp ingress only | `orchestra.classifier`, `orchestra.router`, `orchestra.splitter`; no direct specialist writes | none | system control map only | `MEDIUM` | Documented layer, but current live bridge does not prove runtime use. |
| `orchestrator_agent` | orchestration authority | `ACTIVE` | `YES` | Central raw intake, policy guard, routing, split/fallback, validation/acceptance coordination, governed write handoff | upstream ingress only | branch/date/header/family/routing helpers; `report_splitter_agent`; `fallback_extraction_agent`; specialists; specialist record stores; review/provenance writers | raw, rejected, review, provenance, specialist structured stores | orchestrator is central authority | `CRITICAL` | Broadest control surface in `apps/`; current code also owns raw audit and mixed split handling. |
| `pre_ingestion_validator` | ingress guard | `ACTIVE` | `NO` | Early inbound text cleanup/reject gate before orchestration | `whatsapp_webhook_bridge` only | none | none direct | ingress policy only | `HIGH` | Live pre-orchestrator rejection surface; outside the frozen mandatory flow. |
| `header_normalizer_agent` | intake helper | `ACTIVE` | `YES` | Normalize early header lines for routing helpers | orchestrator/routing helpers only | packages only | none | orchestrator/routing layer | `LOW` | Shared helper across branch/date/family detection. |
| `branch_resolver_agent` | intake helper | `ACTIVE` | `YES` | Resolve branch from header lines or metadata hint | orchestrator/routing/split helpers only | packages only | none | orchestrator/routing layer | `MEDIUM` | Branch scope is governance-significant, so mis-resolution has downstream impact. |
| `date_resolver_agent` | intake helper | `ACTIVE` | `YES` | Resolve report date from header lines | orchestrator/routing/split helpers only | packages only | none | orchestrator/routing layer | `MEDIUM` | Report date defines scope and duplicate detection. |
| `report_family_classifier_agent` | intake helper | `ACTIVE` | `YES` | Deterministic report-family classification | orchestrator/routing helpers only | `header_normalizer_agent`; packages only | none | orchestrator/routing layer | `HIGH` | Family decisions drive specialist selection and intelligence routing. |
| `routing_decision_agent` | intake helper | `ACTIVE` | `YES` | Combine branch/date/family signals into explicit routing contract | `orchestrator_agent` only | branch/date/family helpers; `packages.report_registry` | none | orchestrator only | `HIGH` | Final pre-specialist route contract builder. |
| `mixed_content_detector_agent` | intake helper | `ACTIVE` | `YES` | Detect mixed reports and trailing notes | `orchestrator_agent`, `report_splitter_agent` | `header_normalizer_agent`; packages only | none | orchestrator/splitter only | `HIGH` | Mixed detection changes whether a message is rejected, reviewed, or split. |
| `report_splitter_agent` | intake helper | `ACTIVE` | `YES` | Split mixed raw messages into routed child segments | `orchestrator_agent` only | branch/date/header helpers; `mixed_content_detector_agent`; `packages.report_registry` | none | orchestrator only | `HIGH` | Live splitter in current orchestrator path. |
| `mixed_report_splitter_agent` | intake helper | `UNKNOWN` | `YES (documented)` | Alternate mixed-report split planner with child lineage | orchestrator only | branch/date/header helpers; `packages.report_registry` | none | orchestrator only | `MEDIUM` | Code exists, but live runtime imports are not proven; overlaps with `report_splitter_agent`. |
| `fallback_extraction_agent` | fallback parsing | `ACTIVE` | `YES` | Schema-bound fallback extraction after strict specialist failure | `orchestrator_agent` only | packages normalization only | none | orchestrator plus report policy | `HIGH` | Live fallback is in the specialist path; should never self-persist. |

## Specialist And Specialist Support

| app | layer | runtime status | production path | primary role | allowed caller | allowed callee | allowed record store | allowed authorities | risk level | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `sales_income_agent` | specialist | `ACTIVE` | `YES` | Strict sales parsing and structured candidate/result generation | `orchestrator_agent` only | `adaptive_sop_engine`; local `record_store`; internal parser helpers | `records/structured/sales_income`; local outbox | orchestrator + central governance | `HIGH` | Worker can self-persist when not in candidate mode; intended caller should remain orchestrator. |
| `hr_agent` | specialist | `ACTIVE` | `YES` | Strict attendance specialist and HR-family wrapper for staff performance | `orchestrator_agent` only | `adaptive_sop_engine`; `staff_performance_agent`; local `record_store`; internal HR helpers | `records/structured/hr_attendance` and `records/structured/hr_performance`; local outbox | orchestrator + central governance | `HIGH` | Dual ownership of attendance and performance increases entropy. |
| `staff_performance_agent` | specialist | `ACTIVE` | `YES` | Strict staff performance parsing/result builder | `orchestrator_agent`; `hr_agent` internal wrapper | `hr_agent.record_store`; internal parser/support helpers | `records/structured/hr_performance` via HR store | orchestrator + central governance | `HIGH` | Reusable specialist, but persistence still routes through HR record store. |
| `pricing_stock_release_agent` | specialist | `ACTIVE` | `YES` | Strict bale/pricing specialist | `orchestrator_agent` only | `adaptive_sop_engine`; local `record_store`; internal pricing helpers | `records/structured/pricing_stock_release`; local outbox | orchestrator + central governance | `HIGH` | Worker self-persistence remains possible outside candidate mode. |
| `supervisor_control_agent` | specialist/intelligence | `ACTIVE` | `YES` | Strict supervisor control parsing and intelligence record building | `orchestrator_agent` only | `adaptive_sop_engine`; local `record_store`; internal parser/helpers | `records/structured/supervisor_control`; local outbox | orchestrator + governance | `CRITICAL` | Intelligence path is accepted differently from strict transactional reports; highest boundary/governance sensitivity among specialists. |
| `adaptive_sop_engine` | specialist support | `ACTIVE` | `YES` | Apply learned field variations and attach adaptive metadata to structured payloads | specialists only | adaptive SOP submodules; report-specific field mappers | adaptive registry/review artifacts only; never structured business records | specialist owner only; governance stays outside | `HIGH` | Live in specialist path; payload mutation plus learning side effects make it sensitive. |
| `field_canonicalizer_agent` | specialist support | `ACTIVE` | `YES` | Canonicalize noisy staff-performance field lines | `staff_performance_agent` parser only | none | none | specialist parser only | `LOW` | Narrow helper; no persistence. |
| `staff_status_resolver_agent` | specialist support | `ACTIVE` | `YES` | Resolve duty status, role, and grade tokens in performance rows | `staff_performance_agent` parser only | none | none | specialist parser only | `LOW` | Narrow parser helper; no persistence. |
| `income_agent` | placeholder/legacy alias | `UNKNOWN` | `NO` | Importable placeholder for legacy income destination naming | none proven | none | none | none | `LOW` | `orchestra.router` still names `income_agent`, but current live specialist is `sales_income_agent`. |
| `pricing_agent` | placeholder/legacy alias | `UNKNOWN` | `NO` | Importable placeholder for legacy pricing destination naming | none proven | none | none | none | `LOW` | `orchestra.router` still names `pricing_agent`, but current live specialist is `pricing_stock_release_agent`. |

## Governance, Review, And Supervisor Control

| app | layer | runtime status | production path | primary role | allowed caller | allowed callee | allowed record store | allowed authorities | risk level | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `approval_engine` | manual governance | `ACTIVE` | `YES (review)` | Approve, reject, or replay review items | `supervisor_commands` only | `supervisor_auth`; specialist record stores; `scripts/replay_records` | review metadata, rejected copies, specialist structured stores, audit logs | supervisor authorization | `HIGH` | Manual override path writes final structured records after review. |
| `apply_engine` | proposal governance | `ACTIVE` | `NO` | Show, approve, reject, simulate, and apply optimization proposals | `supervisor_commands` only | `proposals_store`; `supervisor_auth`; packages action store | `records/proposals`, `records/actions`, apply/simulate audit artifacts | supervisor authorization; human approval | `HIGH` | Outside core report path, but mutates proposal and action state. |
| `supervisor_auth` | authorization | `ACTIVE` | `YES (review)` | Branch-scoped supervisor authorization lookup | review engines and query engines only | packages/config only | none | `config/supervisors.json` only | `MEDIUM` | Used by both governance and command-query surfaces. |
| `supervisor_commands` | command-control | `ACTIVE` | `NO` | Parse and execute supervisor-only WhatsApp commands | `command_router`, `whatsapp_webhook_bridge` | `approval_engine`; `apply_engine` | none direct | supervisor auth delegated downstream | `HIGH` | Live command entry to manual review and proposal actions; not part of authoritative report path. |
| `rejection_feedback_agent` | rejection support | `UNKNOWN` | `YES (reject)` | Generate rejected-report feedback artifacts without transport | orchestrator/orchestra rejection path only | local formatter; local `record_store` | `records/rejected/feedback` only | governance rejection only | `MEDIUM` | Worker exists, but no proven live caller outside tests and unproven `orchestra` path. |
| `proposals_store` | proposal persistence | `ACTIVE` | `NO` | Persist, load, list, and update optimization proposals | `optimization_engine` write; `apply_engine` mutate; `executive_engine` read | packages proposal/record writers only | `records/proposals` only | human approval workflow | `MEDIUM` | Central proposal store for optimization and executive query surfaces. |
| `consistency_agent` | additive control check | `UNKNOWN` | `NO` | Run deterministic consistency checks over a branch/day record set | unknown/manual only | `consistency_agent.rules`; packages validation only | none | operator/manual only | `MEDIUM` | Not proven live; could become governance-sensitive if later wired into acceptance. |

## Conversation, Commands, And Reply Path

| app | layer | runtime status | production path | primary role | allowed caller | allowed callee | allowed record store | allowed authorities | risk level | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `command_router` | command ingress | `ACTIVE` | `NO` | Exact command classification for WhatsApp text | `whatsapp_webhook_bridge`; `nl_intent_router` | `supervisor_commands`; `cross_branch_router`; `ceo_router` | none | syntax rules only | `MEDIUM` | Central command switch for non-report WhatsApp traffic. |
| `command_handler` | command reply | `ACTIVE` | `NO` | Read-only help/format/status/why-rejected handler | `whatsapp_webhook_bridge` only | `conversation_policy`; record readers only | none; read-only on `records/responses` | response contract only | `MEDIUM` | Live reply surface outside authoritative report flow. |
| `nl_intent_router` | command ingress | `ACTIVE` | `NO` | LLM-backed NL mapping onto existing commands | `whatsapp_webhook_bridge` only | `command_router`; packages LLM adapter/observability | observability only | command surface only | `HIGH` | Live NL-to-command bridge; not safe to treat as authoritative report routing. |
| `analytics_query_engine` | query engine | `ACTIVE` | `NO` | Authorized cross-branch ranking queries over analytics artifacts | `cross_branch_router` only | `supervisor_auth`; analytics loaders | none; read-only | supervisor authorization | `MEDIUM` | Query-only path; no report governance writes. |
| `cross_branch_router` | query router | `ACTIVE` | `NO` | Parse and format cross-branch WhatsApp ranking queries | `command_router`; `whatsapp_webhook_bridge` | `analytics_query_engine` | none | supervisor auth delegated downstream | `MEDIUM` | Live query reply surface. |
| `ceo_router` | query router | `ACTIVE` | `NO` | Parse and format CEO/executive WhatsApp queries | `command_router`; `whatsapp_webhook_bridge` | `executive_engine` | none | supervisor auth delegated downstream | `HIGH` | Executive query surface is live even though downstream ownership is a boundary concern. |
| `executive_engine` | query engine | `ACTIVE` | `NO` | Build executive insight payloads from analytics, alerts, learning, and proposals | `ceo_router` only | `supervisor_auth`; `proposals_store`; analytics/executive loaders | none; read-only | supervisor lookup only | `HIGH` | Active boundary-debt surface kept upstream. |
| `conversation_policy` | reply policy | `ACTIVE` | `NO` | Allowlist response types and reason exposure | `command_handler`; `conversation_router`; `response_engine` | none | none | approved message contract only | `MEDIUM` | Central policy for outbound response semantics. |
| `conversation_context` | reply state | `ACTIVE` | `NO` | Persist and resolve last interaction per sender | `whatsapp_webhook_bridge`; `conversation_router`; post-write automation | none | `records/context/whatsapp` only | response path only | `MEDIUM` | Stateful reply memory; should never drive business governance. |
| `conversation_router` | reply routing | `ACTIVE` | `NO` | Convert outcomes into normalized response contexts | post-write automation only | `conversation_context`; `conversation_policy`; packages validation helpers | none | response contract only | `HIGH` | Determines whether users get acceptance, review, rejection, duplicate, or unknown guidance. |
| `response_engine` | reply rendering | `ACTIVE` | `NO` | Render deterministic WhatsApp-safe reply text | post-write automation only | `conversation_policy`; packages TAOP feedback; optional LLM adapter | none | approved message contract only | `HIGH` | Live text-rendering stage for outbound replies. |
| `outbound_reply_agent` | outbound transport | `ACTIVE` | `NO` | Dispatch persisted WhatsApp replies through the controlled sender | `whatsapp_webhook_bridge` and post-write reply dispatcher only | `packages.whatsapp_outbound` | `records/responses/whatsapp` dispatch updates only | response artifact + outbound allowlist | `HIGH` | Only app-owned live transport dispatcher found in current code. |

## Automation And Learning

| app | layer | runtime status | production path | primary role | allowed caller | allowed callee | allowed record store | allowed authorities | risk level | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `autonomous_control_engine` | post-write automation | `ACTIVE` | `NO` | Generate deterministic control actions from governed structured records | `packages.record_store.automation` only | report-specific constants/helpers only | none direct; returns action decisions | governance-accepted records only | `HIGH` | Live post-write fanout after governed persistence. |
| `action_effectiveness_engine` | learning automation | `ACTIVE` | `NO` | Analyze action outcomes and write learning summary | `packages.record_store.automation` only | packages learning store only | `records/learning/action_effectiveness` | automation only | `MEDIUM` | Learning artifact generator; not part of authoritative report flow. |
| `review_learning_engine` | learning automation | `ACTIVE` | `NO` | Analyze review queue and write review summary | `packages.record_store.automation` only | packages learning store only | `records/learning/review_summary` | automation only | `MEDIUM` | Feeds later proposal/threshold recommendations. |
| `format_drift_analyzer` | learning automation | `ACTIVE` | `NO` | Analyze raw/review/rejected format drift and write summary | `packages.record_store.automation` only | packages learning store only | `records/learning/format_drift` | automation only | `MEDIUM` | Reads broad raw/review/rejected stores; no direct business writes. |
| `optimization_engine` | proposal automation | `ACTIVE` | `NO` | Generate and persist deterministic optimization proposals | `packages.record_store.automation` only | `proposals_store`; analytics/learning readers | `records/proposals` | automation to proposal workflow; human approval required | `HIGH` | Writes operator proposals from learning outputs. |
| `threshold_recommendation_engine` | learning automation | `ACTIVE` | `NO` | Generate threshold-change recommendations without mutating live policy | `packages.record_store.automation` only | packages learning store/report policy only | `records/learning/threshold_recommendations` | automation only; human approval required | `MEDIUM` | Recommendation generator only; no direct config mutation. |

## Analytics Artifact Generators

| app | layer | runtime status | production path | primary role | allowed caller | allowed callee | allowed record store | allowed authorities | risk level | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `branch_daily_analytics_agent` | analytics generation | `UNKNOWN` | `NO` | Generate per-branch daily analytics artifact | operator script only | `analytics.phase3` only | analytics output files only | manual/operator only | `LOW` | Proven script entrypoint exists, but no live runtime wiring. |
| `staff_leaderboard_agent` | analytics generation | `UNKNOWN` | `NO` | Generate per-branch staff leaderboard artifact | operator script only | `analytics.phase3` only | analytics output files only | manual/operator only | `LOW` | Script/test wired only. |
| `section_productivity_agent` | analytics generation | `UNKNOWN` | `NO` | Generate per-branch section productivity artifact | operator script only | `analytics.phase3` only | analytics output files only | manual/operator only | `LOW` | Script/test wired only. |
| `branch_comparison_agent` | analytics generation | `UNKNOWN` | `NO` | Generate daily cross-branch comparison artifact | operator script only | `analytics.phase3` only | analytics output files only | manual/operator only | `LOW` | Script/test wired only. |

## APIs And UI

| app | layer | runtime status | production path | primary role | allowed caller | allowed callee | allowed record store | allowed authorities | risk level | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dashboard_api` | operator API | `ACTIVE` | `NO` | Read-only operational analytics API | `analytics.phase4_portal` only | analytics/observability/learning readers only | none; read-only | HTTP `GET` only | `LOW` | Operator-facing surface; outside authoritative report path. |
| `dashboard_ui` | operator UI | `ACTIVE` | `NO` | Read-only operator dashboard HTML renderer | `analytics.phase4_portal` only | render helpers and read-only loaders only | none; read-only | HTTP `GET` only | `LOW` | UI surface only. |
| `taop_ops_api` | operator export API | `ACTIVE` | `NO` | Read-only ops API plus export endpoints | `analytics.phase4_portal` only | `packages.taop_ops.*` only | export files only; no records mutation | HTTP `GET` only | `MEDIUM` | Route set includes export generation, so blast radius is larger than pure read-only APIs. |
| `ceo_api` | executive compatibility API | `ACTIVE` | `NO (compat)` | Deprecated executive/external compatibility API | `analytics.phase4_portal` only | executive loaders and executive alert readers only | none; read-only | HTTP `GET` only; compatibility mode | `HIGH` | Boundary-debt surface explicitly marked deprecated in code. |
| `ceo_dashboard_ui` | executive compatibility UI | `ACTIVE` | `NO (compat)` | Deprecated executive dashboard renderer | `analytics.phase4_portal` only | render helpers only | none; read-only | HTTP `GET` only; compatibility mode | `MEDIUM` | Compatibility-only executive UI. |

## Empty Or Unproven

| app | layer | runtime status | production path | primary role | allowed caller | allowed callee | allowed record store | allowed authorities | risk level | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `monitoring_agent` | scaffold/empty | `UNKNOWN` | `NO` | Empty directory; no proven behavior | none | none | none | none | `LOW` | Directory exists under `apps/` but contains no files. |
