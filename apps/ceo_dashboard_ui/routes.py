"""HTML rendering for the Phase 5 CEO dashboard.

Wave 1 freeze marker:
- This UI module is frozen for compatibility only.
- It renders duplicated executive/intelligence views that are not part of
  TopTown Ops' final long-term scope.
- Do not extend executive framing here during Wave 1.
- Visibility changes are deferred to Wave 2; runtime behavior stays unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from html import escape
from typing import Any
from urllib.parse import urlencode

from packages.common.analytics_loader import display_branch_name


def render_ceo_dashboard_response(
    *,
    dashboard: Mapping[str, Any],
    selected_branch: str | None,
    selected_date: str,
    available_branches: list[dict[str, Any]],
    available_dates: list[str],
) -> str:
    """Render the deprecated CEO compatibility page."""

    overview = dashboard.get("overview") or {}
    branches = (dashboard.get("branches") or {}).get("branches") or []
    staff = dashboard.get("staff") or {}
    sections = dashboard.get("sections") or {}
    alerts = dashboard.get("alerts") or {}
    selected_scorecard = dashboard.get("selected_branch_scorecard") or {}
    effective_branch = selected_branch or dashboard.get("selected_branch")
    branch_query = urlencode({"branch": effective_branch or "", "date": selected_date})

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TopTown Deprecated Executive Compatibility View</title>
  <style>
    :root {{
      --ink: #111827;
      --muted: #4b5563;
      --line: rgba(17, 24, 39, 0.12);
      --panel: rgba(255, 255, 255, 0.96);
      --bg-a: #eef2ff;
      --bg-b: #f0fdf4;
      --accent: #1d4ed8;
      --critical-bg: #fef2f2;
      --critical-ink: #991b1b;
      --warning-bg: #fff7ed;
      --warning-ink: #9a3412;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top right, rgba(29, 78, 216, 0.08), transparent 24%),
        linear-gradient(155deg, var(--bg-a), var(--bg-b));
    }}
    main {{ max-width: 1320px; margin: 0 auto; padding: 24px 18px 48px; }}
    header {{
      padding: 28px;
      border-radius: 24px;
      color: #fff;
      background: linear-gradient(135deg, #111827, #1d4ed8);
      box-shadow: 0 20px 52px rgba(17, 24, 39, 0.18);
    }}
    .toolbar, .cards, .grid {{ display: grid; gap: 16px; }}
    .toolbar {{
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      margin: 18px 0 24px;
      align-items: end;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 14px 36px rgba(17, 24, 39, 0.08);
    }}
    h1 {{ margin: 0 0 8px; }}
    h2 {{ margin: 0 0 12px; font-size: 1.2rem; }}
    h3 {{ margin: 0 0 10px; font-size: 1rem; }}
    label {{ display: block; margin-bottom: 8px; color: var(--muted); font-size: 0.92rem; }}
    select, button {{
      width: 100%;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      font: inherit;
    }}
    button {{
      background: linear-gradient(135deg, #1d4ed8, #111827);
      color: #fff;
      border: 0;
      font-weight: 700;
      cursor: pointer;
    }}
    .cards {{ grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }}
    .grid {{ grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); }}
    .label {{ color: var(--muted); font-size: 0.84rem; text-transform: uppercase; letter-spacing: 0.05em; }}
    .value {{ margin-top: 6px; font-size: 1.7rem; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 8px 0; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; font-size: 0.94rem; }}
    th {{ color: var(--muted); font-weight: 700; }}
    .alert-critical, .alert-warning {{
      border-radius: 14px;
      padding: 12px 14px;
      margin-bottom: 10px;
      border: 1px solid transparent;
    }}
    .alert-critical {{ background: var(--critical-bg); color: var(--critical-ink); border-color: rgba(153, 27, 27, 0.14); }}
    .alert-warning {{ background: var(--warning-bg); color: var(--warning-ink); border-color: rgba(154, 52, 18, 0.12); }}
    .links a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>TopTown Deprecated Executive Compatibility View</h1>
      <p>This surface is deprecated and hidden from normal TopTown Ops product exposure. Date: {escape(selected_date)}. Focus branch: {escape(display_branch_name(effective_branch)) if effective_branch else "All branches"}.</p>
    </header>
    <section class="panel">
      <h2>Deprecation Notice</h2>
      <p>TopTown Ops now presents the operator dashboard at <a href="/dashboard">/dashboard</a>. Executive interpretation is deprecated here and is scheduled to move downstream to IOI Colony.</p>
    </section>
    <form class="toolbar" method="get" action="/ceo">
      <section class="panel">
        <label for="branch">Branch</label>
        <select id="branch" name="branch">{_render_branch_options(available_branches, effective_branch)}</select>
      </section>
      <section class="panel">
        <label for="date">Date</label>
        <select id="date" name="date">{_render_date_options(available_dates, selected_date)}</select>
      </section>
      <section class="panel">
        <input type="hidden" name="compat" value="1">
        <label for="apply">Filters</label>
        <button id="apply" type="submit">Load Deprecated View</button>
      </section>
    </form>
    <section class="panel">
      <h2>Compatibility Summary Cards</h2>
      <div class="cards">
        {_metric_card("Total Sales", overview.get("total_gross_sales"))}
        {_metric_card("Active Staff", overview.get("total_active_staff"))}
        {_metric_card("Traffic", overview.get("total_traffic"))}
        {_metric_card("Top Branch", _nested(overview, "top_branch_by_operational_score", "branch"))}
        {_metric_card("Weakest Branch", _nested(overview, "weakest_branch_by_operational_score", "branch"))}
        {_metric_card("Critical Alerts", _nested(overview, "summary_warning_counts", "critical_alert_count"))}
      </div>
    </section>
    <section class="grid">
      <article class="panel">
        <h2>Branch Compatibility View</h2>
        <table>
          <thead><tr><th>Branch</th><th>Sales</th><th>Ops</th><th>Conversion</th><th>Warnings</th><th>Reporting</th></tr></thead>
          <tbody>{_render_branch_rows(branches, effective_branch)}</tbody>
        </table>
      </article>
      <article class="panel">
        <h2>Staff Compatibility View</h2>
        <table>
          <tbody>
            {_summary_row("Best Staff", _nested(staff, "best_staff", "staff_name"))}
            {_summary_row("Weakest Staff", _nested(staff, "weakest_staff", "staff_name"))}
            {_summary_row("Top Items Staff", _first_name(staff, "top_items_staff"))}
            {_summary_row("Top Assisting Staff", _first_name(staff, "top_assisting_staff"))}
            {_summary_row("Idle On-Duty Staff", len(staff.get("idle_on_duty_staff") or []))}
          </tbody>
        </table>
        <h3>Idle On-Duty Staff</h3>
        <table><thead><tr><th>Staff</th><th>Branch</th><th>Score</th></tr></thead><tbody>{_render_staff_rows(staff.get("idle_on_duty_staff") or [])}</tbody></table>
      </article>
      <article class="panel">
        <h2>Section Compatibility View</h2>
        <table>
          <tbody>
            {_summary_row("Strongest Section", _nested(sections, "strongest_section", "section"))}
            {_summary_row("Weakest Section", _nested(sections, "weakest_section", "section"))}
            {_summary_row("Unresolved Hotspots", len(sections.get("unresolved_section_hotspots") or []))}
          </tbody>
        </table>
        <h3>Strongest Sections</h3>
        <table><thead><tr><th>Section</th><th>Branch</th><th>Index</th></tr></thead><tbody>{_render_section_rows(sections.get("strongest_sections") or [])}</tbody></table>
        <h3>Weakest Sections</h3>
        <table><thead><tr><th>Section</th><th>Branch</th><th>Index</th></tr></thead><tbody>{_render_section_rows(sections.get("weakest_sections") or [])}</tbody></table>
        <h3>Unresolved Hotspots</h3>
        <table><thead><tr><th>Branch</th><th>Count</th><th>Examples</th></tr></thead><tbody>{_render_hotspot_rows(sections.get("unresolved_section_hotspots") or [])}</tbody></table>
      </article>
      <article class="panel">
        <h2>Deprecated Alerts Panel</h2>
        <h3>Critical Alerts</h3>
        {_render_alerts(alerts.get("critical_alerts") or [], "critical")}
        <h3>Warning Alerts</h3>
        {_render_alerts(alerts.get("warning_alerts") or [], "warning")}
      </article>
      <article class="panel">
        <h2>Selected Branch Detail</h2>
        <table>
          <tbody>
            {_summary_row("Branch", _get(selected_scorecard, "branch"))}
            {_summary_row("Sales", _get(selected_scorecard, "gross_sales"))}
            {_summary_row("Operational Score", _get(selected_scorecard, "operational_score"))}
            {_summary_row("Conversion", _get(selected_scorecard, "conversion_rate"))}
            {_summary_row("Reporting Status", _format_reporting_status(_get(selected_scorecard, "readiness_status")))}
            {_summary_row("Reporting Incomplete", _format_missing(_get(selected_scorecard, "missing_input_indicators")))}
          </tbody>
        </table>
      </article>
      <article class="panel links">
        <h2>Compatibility Routing</h2>
        <p>Direct CEO/executive API routes remain available only for rollback safety and automation compatibility.</p>
        <p>Normal TopTown Ops usage should stay on the operator dashboard and operator analytics API.</p>
      </article>
    </section>
  </main>
</body>
</html>"""


def _metric_card(label: str, value: Any) -> str:
    rendered = "n/a" if value is None else escape(str(value))
    return f'<section class="panel"><div class="label">{escape(label)}</div><div class="value">{rendered}</div></section>'


def _render_branch_options(options: list[dict[str, Any]], selected: str | None) -> str:
    return "".join(
        f'<option value="{escape(str(option["slug"]))}"{" selected" if option["slug"] == selected else ""}>{escape(str(option["display_name"]))}</option>'
        for option in options
    )


def _render_date_options(options: list[str], selected: str) -> str:
    return "".join(
        f'<option value="{escape(option)}"{" selected" if option == selected else ""}>{escape(option)}</option>'
        for option in options
    )


def _render_branch_rows(rows: list[dict[str, Any]], selected_branch: str | None) -> str:
    if not rows:
        return '<tr><td colspan="6">No branch scorecards available.</td></tr>'
    rendered = []
    for row in rows:
        data_label = "complete"
        if row.get("readiness_status") == "data_gap":
            data_label = "reporting incomplete"
        elif row.get("readiness_status") not in {"ready", "accepted"}:
            data_label = "confidence reduced"
        branch = str(row.get("branch") or "unknown")
        label = display_branch_name(branch)
        if selected_branch == branch:
            label = f"{label} *"
        rendered.append(
            f"<tr><td>{escape(label)}</td><td>{escape(str(row.get('gross_sales')))}</td><td>{escape(str(row.get('operational_score')))}</td><td>{escape(str(row.get('conversion_rate')))}</td><td>{escape(str(row.get('warning_count')))}</td><td>{escape(data_label)}</td></tr>"
        )
    return "".join(rendered)


def _render_staff_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="3">No idle on-duty staff detected.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('staff_name')))}</td><td>{escape(display_branch_name(str(row.get('branch') or 'unknown')))}</td><td>{escape(str(row.get('activity_score')))}</td></tr>"
        for row in rows[:10]
    )


def _render_section_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="3">No section summaries available.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('section')))}</td><td>{escape(display_branch_name(str(row.get('branch') or 'unknown')))}</td><td>{escape(str(row.get('productivity_index')))}</td></tr>"
        for row in rows[:10]
    )


def _render_hotspot_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="3">No unresolved section issues.</td></tr>'
    return "".join(
        f"<tr><td>{escape(display_branch_name(str(row.get('branch') or 'unknown')))}</td><td>{escape(str(row.get('count')))}</td><td>{escape(', '.join(row.get('examples') or []))}</td></tr>"
        for row in rows
    )


def _render_alerts(rows: list[dict[str, Any]], severity: str) -> str:
    if not rows:
        return f"<p>No {severity} alerts.</p>"
    klass = "alert-critical" if severity == "critical" else "alert-warning"
    return "".join(
        f'<div class="{klass}"><strong>{escape(display_branch_name(str(row.get("branch") or "unknown")))}</strong>: {escape(str(row.get("message")))}</div>'
        for row in rows
    )


def _summary_row(label: str, value: Any) -> str:
    rendered = "n/a" if value is None else escape(str(value))
    return f"<tr><th>{escape(label)}</th><td>{rendered}</td></tr>"


def _nested(mapping: Mapping[str, Any] | None, key: str, child: str) -> Any:
    if isinstance(mapping, Mapping):
        block = mapping.get(key)
        if isinstance(block, Mapping):
            return block.get(child)
    return None


def _get(mapping: Mapping[str, Any] | None, key: str) -> Any:
    if isinstance(mapping, Mapping):
        return mapping.get(key)
    return None


def _first_name(mapping: Mapping[str, Any] | None, key: str) -> str | None:
    rows = _get(mapping, key)
    if not isinstance(rows, list) or not rows:
        return None
    first = rows[0]
    if isinstance(first, Mapping):
        return first.get("staff_name")
    return None


def _format_missing(mapping: Mapping[str, Any] | None) -> str:
    if not isinstance(mapping, Mapping):
        return "n/a"
    labels = {
        "sales_input_missing": "sales reporting",
        "staff_input_missing": "staff reporting",
    }
    missing = [labels.get(name, name) for name, value in mapping.items() if value]
    return ", ".join(missing) if missing else "none"


def _format_reporting_status(value: Any) -> str:
    if value in {"ready", "accepted"}:
        return "current"
    if value == "data_gap":
        return "reporting incomplete"
    if value == "needs_review":
        return "confidence reduced"
    if value is None:
        return "n/a"
    return str(value)
