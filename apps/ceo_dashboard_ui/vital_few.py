"""Server-rendered, exception-first Phase 1 CEO dashboard."""

from __future__ import annotations

from collections.abc import Mapping
from html import escape
from typing import Any
from urllib.parse import urlencode


def render_vital_few_dashboard(
    *,
    dashboard: Mapping[str, Any],
    available_dates: list[str],
    show_all: bool = False,
) -> str:
    """Render the versioned KPI contract without mutating source data."""

    selected_scope_id = str(dashboard.get("selected_scope") or "enterprise")
    enterprise = dashboard.get("enterprise") if isinstance(dashboard.get("enterprise"), Mapping) else {}
    branches = [item for item in dashboard.get("branches") or [] if isinstance(item, Mapping)]
    selected_scope = enterprise
    if selected_scope_id != "enterprise":
        selected_scope = next((item for item in branches if item.get("scope_id") == selected_scope_id), enterprise)
    kpis = list(selected_scope.get("kpis") or [])
    visible_kpis = kpis if show_all else [kpi for kpi in kpis if _nested(kpi, "threshold", "state") == "breach"]
    report_date = str(dashboard.get("report_date") or "")
    priority = dashboard.get("top_priority") if isinstance(dashboard.get("top_priority"), Mapping) else {}
    query_context = {"date": report_date}
    if selected_scope_id != "enterprise":
        query_context["branch"] = selected_scope_id
    all_query = dict(query_context)
    all_query["show"] = "all"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TopTown CEO Dashboard · Vital Few</title>
  <style>
    :root {{
      --ink: #17201f; --muted: #64706d; --line: #dce3df; --paper: #fbfcfa;
      --panel: #fff; --forest: #173f35; --teal: #16725d; --mint: #dff3e9;
      --amber: #9a5a08; --amber-bg: #fff4db; --red: #a3342d; --red-bg: #fff0ed;
      --shadow: 0 18px 48px rgba(24, 56, 47, .10);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; color: var(--ink); background: linear-gradient(145deg, #edf4ef, var(--paper) 45%, #f4efe4); font-family: Inter, "Segoe UI", sans-serif; }}
    main {{ max-width: 1240px; margin: auto; padding: 24px 18px 56px; }}
    header {{ padding: 30px; color: white; border-radius: 24px; background: linear-gradient(130deg, #102f28, #176650); box-shadow: var(--shadow); }}
    header h1 {{ margin: 6px 0; font-family: Georgia, serif; font-size: clamp(2rem, 5vw, 3.5rem); font-weight: 500; }}
    header p {{ margin: 8px 0 0; max-width: 760px; color: #d8eee7; }}
    .eyebrow {{ text-transform: uppercase; letter-spacing: .13em; font-size: .76rem; font-weight: 800; }}
    .toolbar {{ display: grid; grid-template-columns: repeat(3, minmax(180px, 1fr)); gap: 14px; margin: 18px 0; }}
    .panel, .kpi {{ background: rgba(255,255,255,.96); border: 1px solid var(--line); border-radius: 18px; padding: 18px; box-shadow: 0 10px 30px rgba(24, 56, 47, .06); }}
    label, .meta-label {{ display: block; color: var(--muted); font-size: .76rem; font-weight: 800; text-transform: uppercase; letter-spacing: .06em; }}
    select, button {{ width: 100%; margin-top: 8px; border: 1px solid var(--line); border-radius: 11px; padding: 10px 12px; background: white; color: var(--ink); font: inherit; }}
    button {{ border: 0; color: white; background: var(--forest); font-weight: 800; cursor: pointer; }}
    .priority {{ display: grid; grid-template-columns: auto 1fr; gap: 14px; align-items: center; margin: 20px 0; border-left: 6px solid var(--amber); }}
    .priority-icon {{ display: grid; place-items: center; width: 44px; height: 44px; border-radius: 50%; color: var(--amber); background: var(--amber-bg); font-weight: 900; }}
    h2 {{ margin: 0 0 6px; font-family: Georgia, serif; font-weight: 500; }}
    h3 {{ margin: 0; font-size: 1.04rem; }}
    .section-head {{ display: flex; gap: 14px; align-items: end; justify-content: space-between; margin: 28px 0 12px; }}
    .section-head p {{ margin: 0; color: var(--muted); }}
    .section-head a {{ color: var(--teal); font-weight: 800; text-decoration: none; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(310px, 1fr)); gap: 16px; }}
    .kpi {{ border-top: 5px solid var(--red); }}
    .kpi.healthy {{ border-top-color: var(--teal); }}
    .kpi.unavailable {{ border-top-color: #8a918e; }}
    .kpi-head {{ display: flex; justify-content: space-between; gap: 12px; align-items: start; }}
    .badge {{ border-radius: 999px; padding: 5px 9px; font-size: .7rem; font-weight: 900; text-transform: uppercase; letter-spacing: .05em; }}
    .breach .badge, .badge.breach {{ color: var(--red); background: var(--red-bg); }}
    .badge.healthy {{ color: var(--teal); background: var(--mint); }}
    .badge.unavailable {{ color: #59615e; background: #edf0ee; }}
    .value {{ margin: 18px 0 14px; font: 500 clamp(2.2rem, 6vw, 3.3rem)/1 Georgia, serif; }}
    .metric-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
    .metric {{ min-height: 76px; padding: 10px; border-radius: 12px; background: #f5f7f5; }}
    .metric strong {{ display: block; margin-top: 6px; font-size: .94rem; }}
    .wide {{ grid-column: 1 / -1; }}
    .fine {{ color: var(--muted); font-size: .82rem; line-height: 1.45; }}
    details {{ margin-top: 12px; border-top: 1px solid var(--line); padding-top: 10px; }}
    summary {{ cursor: pointer; color: var(--teal); font-weight: 800; }}
    .empty {{ padding: 28px; text-align: center; color: var(--muted); }}
    .branch-list {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 10px; }}
    .branch-row {{ display: flex; justify-content: space-between; gap: 10px; padding: 12px; border-radius: 12px; background: #f5f7f5; color: inherit; text-decoration: none; }}
    .gate {{ margin-top: 24px; background: #faf7ef; }}
    footer {{ margin-top: 24px; color: var(--muted); font-size: .84rem; text-align: center; }}
    @media (max-width: 720px) {{ .toolbar {{ grid-template-columns: 1fr; }} .priority {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <main>
    <header>
      <div class="eyebrow">Phase 1 · Decision support</div>
      <h1>The vital few.</h1>
      <p>Exceptions first. Facts flow one way from TAOP; this board stores nothing authoritative, triggers no action, and leaves every decision with the CEO.</p>
    </header>
    <form class="toolbar" method="get" action="/ceo/vital-few">
      <section class="panel"><label for="scope">Scope</label><select id="scope" name="branch">{_scope_options(branches, selected_scope_id)}</select></section>
      <section class="panel"><label for="date">Reporting date</label><select id="date" name="date">{_date_options(available_dates, report_date)}</select></section>
      <section class="panel"><label for="apply">Refresh view</label><button id="apply" type="submit">Apply filters</button></section>
    </form>
    <section class="panel priority">
      <div class="priority-icon">01</div>
      <div><div class="eyebrow" style="color:var(--amber)">Today’s single priority</div><h2>{escape(str(priority.get('scope_display_name') or 'All branches'))}</h2><p>{escape(str(priority.get('message') or 'No priority is available.'))}</p></div>
    </section>
    <div class="section-head">
      <div><h2>{escape(str(selected_scope.get('display_name') or 'All branches'))}</h2><p>{len(visible_kpis)} exception{'s' if len(visible_kpis) != 1 else ''} shown · data completeness {_completeness_text(selected_scope.get('data_completeness'))}</p></div>
      <a href="/ceo/vital-few?{urlencode(query_context if show_all else all_query)}">{'Show exceptions only' if show_all else 'Show all KPIs'}</a>
    </div>
    <section class="grid">{''.join(_kpi_card(kpi) for kpi in visible_kpis) if visible_kpis else _empty_state(all_query)}</section>
    {_branch_attention(branches, report_date) if selected_scope_id == 'enterprise' else ''}
    <section class="panel gate">
      <h2>Financial clarity is gated</h2>
      <p><strong>Revenue is not profit.</strong> Margin, profit, cash, expense ratio, and budget pacing stay off this board until the accounting system supplies COGS, expenses, bank, and budget data one-way.</p>
    </section>
    <footer>KPI contract {escape(str(dashboard.get('contract_version') or 'unknown'))} · Advisory presentation only · CEO decides</footer>
  </main>
</body>
</html>"""


def _kpi_card(kpi: Mapping[str, Any]) -> str:
    state = str(_nested(kpi, "threshold", "state") or "unavailable")
    trend = kpi.get("trend") if isinstance(kpi.get("trend"), Mapping) else {}
    target = kpi.get("target") if isinstance(kpi.get("target"), Mapping) else {}
    threshold = kpi.get("threshold") if isinstance(kpi.get("threshold"), Mapping) else {}
    source = kpi.get("source") if isinstance(kpi.get("source"), Mapping) else {}
    completeness = kpi.get("data_completeness") if isinstance(kpi.get("data_completeness"), Mapping) else {}
    direction = {"up": "▲", "down": "▼", "flat": "→"}.get(str(trend.get("direction")), "—")
    records = source.get("records") if isinstance(source.get("records"), list) else []
    issues = completeness.get("issues") if isinstance(completeness.get("issues"), list) else []
    inputs = completeness.get("sources_present") if isinstance(completeness.get("sources_present"), Mapping) else {}
    return f"""<article class="kpi {escape(state)}">
      <div class="kpi-head"><div><div class="eyebrow" style="color:var(--muted)">{escape(str(kpi.get('domain') or '').replace('_', ' '))}</div><h3>{escape(str(kpi.get('label') or 'Metric'))}</h3></div><span class="badge {escape(state)}">{escape(state)}</span></div>
      <div class="value">{_format_value(kpi.get('value'), str(kpi.get('unit') or ''))}</div>
      <div class="metric-grid">
        <div class="metric"><span class="meta-label">Trend</span><strong>{direction} {_pct(trend.get('change_pct'))}</strong><span class="fine">vs {escape(str(trend.get('comparison') or 'comparison').replace('_', ' '))}; {trend.get('sample_count', 0)}/{trend.get('expected_sample_count', 0)} samples</span></div>
        <div class="metric"><span class="meta-label">Target / baseline</span><strong>{_format_value(target.get('value'), str(kpi.get('unit') or ''))}</strong><span class="fine">Trailing 8-week median; {target.get('sample_count', 0)}/{target.get('expected_sample_count', 0)} samples</span></div>
        <div class="metric wide"><span class="meta-label">Threshold</span><strong>{escape(state.title())} · boundary {_format_value(threshold.get('boundary'), str(kpi.get('unit') or ''))}</strong><span class="fine">{escape(str(threshold.get('rule') or 'Not defined'))}</span></div>
        <div class="metric"><span class="meta-label">Source</span><strong>{escape(str(source.get('system') or 'TAOP'))}</strong><span class="fine">{escape(str(source.get('description') or ''))}</span></div>
        <div class="metric"><span class="meta-label">Cadence</span><strong>{escape(str(kpi.get('cadence') or '').title())}</strong><span class="fine">Completeness: {escape(str(completeness.get('status') or 'unknown'))}</span></div>
      </div>
      <details><summary>Formula, trust, and traceability</summary><p class="fine"><strong>Formula:</strong> {escape(str(kpi.get('formula') or ''))}</p><p class="fine"><strong>Input coverage:</strong> {escape(', '.join(f'{key}={value}' for key, value in sorted(inputs.items())) if inputs else 'none')}</p><p class="fine"><strong>Completeness issues:</strong> {escape(', '.join(str(item) for item in issues) if issues else 'none')}</p><p class="fine"><strong>Records:</strong> {escape(', '.join(str(item) for item in records) if records else 'no current source record')}</p></details>
    </article>"""


def _scope_options(branches: list[Mapping[str, Any]], selected: str) -> str:
    options = [("", "Enterprise")] + [(str(branch.get("scope_id")), str(branch.get("display_name"))) for branch in branches]
    return "".join(
        f'<option value="{escape(value)}"{" selected" if (selected == "enterprise" and not value) or selected == value else ""}>{escape(label)}</option>'
        for value, label in options
    )


def _date_options(dates: list[str], selected: str) -> str:
    values = list(dict.fromkeys([selected, *dates]))
    return "".join(f'<option value="{escape(value)}"{" selected" if value == selected else ""}>{escape(value)}</option>' for value in values if value)


def _branch_attention(branches: list[Mapping[str, Any]], report_date: str) -> str:
    rows = "".join(
        f'<a class="branch-row" href="/ceo/vital-few?{urlencode({"branch": branch.get("scope_id"), "date": report_date})}"><span>{escape(str(branch.get("display_name") or branch.get("scope_id")))}</span><strong>{int(branch.get("exception_count") or 0)} exception(s)</strong></a>'
        for branch in branches
    )
    return f'<div class="section-head"><div><h2>Branch attention</h2><p>Open a branch to inspect its full KPI contract.</p></div></div><section class="branch-list">{rows}</section>'


def _empty_state(all_query: Mapping[str, str]) -> str:
    return f'<div class="panel empty"><h3>No threshold breaches in this scope.</h3><p>Healthy and unavailable metrics remain one click away.</p><a href="/ceo/vital-few?{urlencode(all_query)}">Show all KPIs</a></div>'


def _format_value(value: Any, unit: str) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    if unit == "PERCENT":
        return f"{number * 100:.1f}%"
    if unit in {"PGK_PER_HOUR", "PGK_PER_STAFF"}:
        return f"K{number:,.2f}"
    if unit == "RATIO":
        return f"{number:.2f}×"
    if unit == "ACTIVITIES_PER_STAFF":
        return f"{number:.2f}"
    return f"{number:,.2f}"


def _pct(value: Any) -> str:
    return "n/a" if value is None else f"{abs(float(value)):.1f}%"


def _completeness_text(value: Any) -> str:
    if not isinstance(value, Mapping):
        return "unknown"
    return f"{value.get('available_kpis', 0)}/{value.get('total_kpis', 0)} KPIs available"


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    value: Any = payload
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


__all__ = ["render_vital_few_dashboard"]
