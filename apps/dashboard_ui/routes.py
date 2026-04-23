"""Dashboard HTML rendering for Phase 4 operational analytics.

Wave 1 operator vs executive boundary note:
- The operator dashboard remains in TopTown Ops for operational visibility.
- Its long-term scope is upstream factual operations: what happened, what was
  recorded, and what operator-facing KPI facts are available.
- Executive intelligence and interpretive downstream framing belong in IOI
  Colony and should not expand here.
- Wave 1 makes no route or rendering behavior changes.
"""

from __future__ import annotations

from collections.abc import Mapping
from html import escape
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from packages.common.analytics_loader import display_branch_name
from packages.learning_store import get_learning_artifact_path, read_latest_learning_artifact


def render_dashboard_response(
    *,
    bundle: Mapping[str, Any],
    catalog: Mapping[str, Any],
    selected_branch: str,
    selected_date: str,
    warnings: list[str],
) -> str:
    """Render the operational dashboard page for one branch/date selection."""

    branch_daily = bundle.get("branch_daily") or {}
    staff_daily = bundle.get("staff_daily") or {}
    section_daily = bundle.get("section_daily") or {}
    branch_comparison = bundle.get("branch_comparison") or {}
    operator_action_state = bundle.get("operator_action_state") or {}
    action_summary = operator_action_state.get("summary") or {}
    pending_actions = operator_action_state.get("pending_actions") or []
    branch_options = catalog.get("available_branches") or []
    date_options = catalog.get("available_dates") or []
    branch_query = urlencode({"branch": selected_branch, "date": selected_date})
    learning = _load_learning_bundle(selected_date)

    top_branch = _top_by_operational_score(branch_comparison, highest=True)
    weak_branch = _top_by_operational_score(branch_comparison, highest=False)
    operational_summary = {
        "total_branches": len(branch_options),
        "available_dates": len(date_options),
        "top_branch": top_branch["branch"] if top_branch else None,
        "weakest_branch": weak_branch["branch"] if weak_branch else None,
    }

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TopTown Ops Dashboard</title>
  <style>
    :root {{
      --ink: #132238;
      --muted: #56657a;
      --line: rgba(19, 34, 56, 0.12);
      --panel: rgba(255, 255, 255, 0.94);
      --accent: #005f73;
      --bg-a: #f5f3ff;
      --bg-b: #ecfeff;
      --warn-bg: #fff7ed;
      --warn-ink: #9a3412;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(0, 95, 115, 0.08), transparent 24%),
        linear-gradient(155deg, var(--bg-a), var(--bg-b));
    }}
    main {{ max-width: 1240px; margin: 0 auto; padding: 24px 18px 48px; }}
    header {{
      padding: 24px;
      border-radius: 24px;
      color: #fff;
      background: linear-gradient(130deg, #132238, #005f73);
      box-shadow: 0 20px 50px rgba(19, 34, 56, 0.16);
    }}
    header h1 {{ margin: 0 0 8px; font-size: 2rem; }}
    header p {{ margin: 0; color: rgba(255, 255, 255, 0.84); }}
    .toolbar, .card-grid, .section-grid {{ display: grid; gap: 16px; }}
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
      box-shadow: 0 14px 36px rgba(19, 34, 56, 0.08);
      backdrop-filter: blur(8px);
    }}
    h2 {{ margin: 0 0 14px; font-size: 1.2rem; }}
    h3 {{ margin: 0 0 10px; font-size: 1rem; }}
    label {{ display: block; margin-bottom: 8px; color: var(--muted); font-size: 0.92rem; }}
    select, button {{
      width: 100%;
      border-radius: 12px;
      border: 1px solid var(--line);
      padding: 10px 12px;
      font: inherit;
    }}
    button {{
      background: linear-gradient(135deg, #005f73, #132238);
      color: #fff;
      border: 0;
      font-weight: 700;
      cursor: pointer;
    }}
    .card-grid {{ grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); }}
    .card-label {{ color: var(--muted); font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.06em; }}
    .card-value {{ margin-top: 6px; font-size: 1.7rem; font-weight: 700; }}
    .section-grid {{ grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); margin-top: 18px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 8px 0; border-bottom: 1px solid var(--line); text-align: left; font-size: 0.94rem; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 700; }}
    .warning {{
      margin-bottom: 18px;
      border-radius: 16px;
      padding: 14px 16px;
      background: var(--warn-bg);
      color: var(--warn-ink);
      border: 1px solid rgba(154, 52, 18, 0.14);
    }}
    .subtle {{ color: var(--muted); font-size: 0.92rem; }}
    .links a {{ color: var(--accent); text-decoration: none; font-weight: 700; }}
    .list-note {{ margin: 0; padding-left: 18px; color: var(--ink); }}
    .list-note li {{ margin: 0 0 8px; }}
    .kicker {{ color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.08em; }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>TopTown Operational Dashboard</h1>
      <p>Read-only operator interface over analytics JSON. Branch: {escape(display_branch_name(selected_branch))}. Date: {escape(selected_date)}.</p>
    </header>
    <form class="toolbar" method="get" action="/dashboard">
      <section class="panel">
        <label for="branch">Branch</label>
        <select id="branch" name="branch">{_render_branch_options(branch_options, selected_branch)}</select>
      </section>
      <section class="panel">
        <label for="date">Date</label>
        <select id="date" name="date">{_render_date_options(date_options, selected_date)}</select>
      </section>
      <section class="panel">
        <label for="apply">Filters</label>
        <button id="apply" type="submit">Apply Filters</button>
      </section>
    </form>
    {_render_warnings(warnings)}
    <section class="panel">
      <h2>Operational Overview</h2>
      <div class="card-grid">
        {_metric_card("Total Branches Available", operational_summary["total_branches"])}
        {_metric_card("Available Dates", operational_summary["available_dates"])}
        {_metric_card("Highest Ops Score Branch", display_branch_name(operational_summary["top_branch"]) if operational_summary["top_branch"] else None)}
        {_metric_card("Lowest Ops Score Branch", display_branch_name(operational_summary["weakest_branch"]) if operational_summary["weakest_branch"] else None)}
      </div>
      <div class="section-grid">
        <article class="panel">
          <h3>Branch Comparison Table</h3>
          <table>
            <thead><tr><th>Branch</th><th>Sales</th><th>Ops Score</th><th>Conversion</th></tr></thead>
            <tbody>{_render_branch_scorecards(branch_comparison.get("branch_scorecards") or [], selected_branch)}</tbody>
          </table>
        </article>
        <article class="panel links">
          <h3>Operator API Routes</h3>
          <p><a href="/api/analytics/staff?{escape(branch_query)}">/api/analytics/staff</a></p>
          <p><a href="/api/analytics/branch_daily?{escape(branch_query)}">/api/analytics/branch_daily</a></p>
          <p><a href="/api/analytics/section?{escape(branch_query)}">/api/analytics/section</a></p>
          <p><a href="/api/analytics/branch_comparison?date={escape(selected_date)}">/api/analytics/branch_comparison</a></p>
          <p><a href="/api/actions/pending?{escape(branch_query)}">/api/actions/pending</a></p>
          <p><a href="/api/actions/summary?{escape(branch_query)}">/api/actions/summary</a></p>
          <p><a href="/api/feedback/summary?{escape(branch_query)}">/api/feedback/summary</a></p>
          <p><a href="/api/learning/summary">/api/learning/summary</a></p>
          <p><a href="/api/learning/recommendations">/api/learning/recommendations</a></p>
        </article>
      </div>
    </section>
    <section class="panel">
      <h2>Operator Action Loop</h2>
      <div class="card-grid">
        {_metric_card("Pending Actions", action_summary.get("pending_actions"))}
        {_metric_card("Acknowledged", action_summary.get("actions_acknowledged"))}
        {_metric_card("In Progress", action_summary.get("actions_in_progress"))}
        {_metric_card("Resolved", action_summary.get("actions_resolved"))}
        {_metric_card("Dismissed", action_summary.get("actions_dismissed"))}
        {_metric_card("Stale Pending", action_summary.get("stale_pending_actions"))}
        {_metric_card("Review-Linked", action_summary.get("review_linked_actions"))}
      </div>
      <div class="section-grid">
        <article class="panel">
          <h3>Pending Actions</h3>
          <table>
            <thead><tr><th>Rule</th><th>Status</th><th>Priority</th><th>Assigned</th><th>Review</th></tr></thead>
            <tbody>{_render_pending_actions(pending_actions)}</tbody>
          </table>
        </article>
        <article class="panel">
          <h3>Feedback State</h3>
          <table>
            <tbody>
              {_summary_row("Feedback Records", action_summary.get("feedback_records"))}
              {_summary_row("Feedback Events", action_summary.get("feedback_history_events"))}
              {_summary_row("Pending Requires Ack", _count_requires_ack(pending_actions))}
              {_summary_row("Latest Date", selected_date)}
            </tbody>
          </table>
          <p class="subtle">Action files remain the source of truth for generated recommendations. Feedback is recorded separately and displayed here read-only.</p>
        </article>
      </div>
    </section>
    <section class="panel">
      <h2>Daily Learning Summary</h2>
      <p class="subtle">Concise read-only learning highlights from persisted artifacts. No analysis is triggered from the dashboard.</p>
      <div class="card-grid">
        {_metric_card("Learning Artifact Date", learning.get("artifact_date"))}
        {_metric_card("Top Review Hotspot", _top_review_hotspot_label(learning.get("review")))}
        {_metric_card("Top Recommendation", _top_recommendation_label(learning.get("recommendations")))}
        {_metric_card("Top Drift Note", _top_drift_note_label(learning.get("format_drift")))}
      </div>
      <div class="section-grid">
        <article class="panel">
          <div class="kicker">Top Issues</div>
          {_render_learning_notes(_daily_issue_notes(learning))}
        </article>
        <article class="panel">
          <div class="kicker">Top Recommendations</div>
          {_render_learning_notes(_daily_recommendation_notes(learning.get("recommendations")))}
        </article>
        <article class="panel">
          <div class="kicker">Risk Notes</div>
          {_render_learning_notes(_daily_risk_notes(learning))}
        </article>
      </div>
    </section>
    <section class="section-grid">
      <article class="panel">
        <h2>Staff Performance View</h2>
        <div class="card-grid">
          {_metric_card("Total Staff", _nested(staff_daily, "summary_counts", "total_staff_count"))}
          {_metric_card("Active Staff", _nested(staff_daily, "summary_counts", "active_staff_count"))}
          {_metric_card("Items Moved", _nested(staff_daily, "summary_counts", "total_items_moved"))}
          {_metric_card("Assisting Count", _nested(staff_daily, "summary_counts", "total_assisting_count"))}
        </div>
        <h3>Top Items Moved</h3>
        <table><thead><tr><th>Staff</th><th>Items</th><th>Section</th></tr></thead><tbody>{_render_staff_metric_rows(staff_daily.get("top_items_moved") or [], "items_moved")}</tbody></table>
        <h3>Top Assisting</h3>
        <table><thead><tr><th>Staff</th><th>Assists</th><th>Section</th></tr></thead><tbody>{_render_staff_metric_rows(staff_daily.get("top_assisting") or [], "assisting_count")}</tbody></table>
        <h3>Top Activity Score</h3>
        <table><thead><tr><th>Staff</th><th>Score</th><th>Section</th></tr></thead><tbody>{_render_staff_metric_rows(staff_daily.get("top_activity_score") or [], "activity_score")}</tbody></table>
        <h3>Lowest Productivity</h3>
        <table><thead><tr><th>Staff</th><th>Score</th><th>Duty Status</th></tr></thead><tbody>{_render_staff_low_rows(staff_daily.get("lowest_productivity") or [])}</tbody></table>
        <h3>Role Summary</h3>
        <table><thead><tr><th>Role</th><th>Staff</th><th>Avg Score</th></tr></thead><tbody>{_render_role_rows(staff_daily.get("role_summaries") or [])}</tbody></table>
        <h3>Duty Status Summary</h3>
        <table><thead><tr><th>Status</th><th>Staff</th><th>Items</th></tr></thead><tbody>{_render_duty_rows(staff_daily.get("duty_status_summaries") or [])}</tbody></table>
      </article>
      <article class="panel">
        <h2>Sales vs Staffing Efficiency View</h2>
        <div class="card-grid">
          {_metric_card("Gross Sales", branch_daily.get("gross_sales"))}
          {_metric_card("Traffic", branch_daily.get("traffic"))}
          {_metric_card("Served", branch_daily.get("served"))}
          {_metric_card("Labor Hours", branch_daily.get("labor_hours"))}
          {_metric_card("Active Staff Count", branch_daily.get("active_staff_count"))}
          {_metric_card("Sales per Active Staff", branch_daily.get("sales_per_active_staff"))}
          {_metric_card("Items per Active Staff", branch_daily.get("items_per_active_staff"))}
          {_metric_card("Assists per Active Staff", branch_daily.get("assists_per_active_staff"))}
        </div>
        <h3>Operational Flags</h3>
        <table><thead><tr><th>Code</th><th>Severity</th><th>Message</th></tr></thead><tbody>{_render_flags(branch_daily.get("operational_flags") or [])}</tbody></table>
        <p class="subtle">This view is file-backed directly from branch daily analytics. No calculations are recomputed in the dashboard.</p>
      </article>
      <article class="panel">
        <h2>Section Productivity View</h2>
        <h3>Section Table</h3>
        <table><thead><tr><th>Section</th><th>Productivity Index</th><th>Staff</th><th>Items</th></tr></thead><tbody>{_render_section_rows(section_daily.get("sections") or [])}</tbody></table>
        <h3>Unresolved Sections Indicator</h3>
        <div class="card-grid">
          {_metric_card("Unresolved Count", _nested(section_daily, "unresolved_section_tracking", "count"))}
          {_metric_card("Examples", ", ".join(_nested(section_daily, "unresolved_section_tracking", "examples") or []) or None)}
        </div>
      </article>
      <article class="panel">
        <h2>Branch Comparison Metrics</h2>
        <h3>Ranked by Sales</h3>
        <table><thead><tr><th>Rank</th><th>Branch</th><th>Sales</th></tr></thead><tbody>{_render_rank_rows(branch_comparison.get("ranked_branches_by_sales") or [], "gross_sales")}</tbody></table>
        <h3>Ranked by Conversion</h3>
        <table><thead><tr><th>Rank</th><th>Branch</th><th>Conversion</th></tr></thead><tbody>{_render_rank_rows(branch_comparison.get("ranked_branches_by_conversion") or [], "conversion_rate")}</tbody></table>
        <h3>Ranked by Staff Productivity</h3>
        <table><thead><tr><th>Rank</th><th>Branch</th><th>Productivity</th></tr></thead><tbody>{_render_rank_rows(branch_comparison.get("ranked_branches_by_staff_productivity") or [], "staff_productivity_index")}</tbody></table>
        <h3>Ranked by Operational Score</h3>
        <table><thead><tr><th>Rank</th><th>Branch</th><th>Ops Score</th></tr></thead><tbody>{_render_rank_rows(branch_comparison.get("ranked_branches_by_operational_score") or [], "operational_score")}</tbody></table>
      </article>
      <article class="panel">
        <h2>Review Hotspots</h2>
        <table><thead><tr><th>Branch</th><th>Reviews</th><th>Leading Cause</th></tr></thead><tbody>{_render_review_hotspots(learning.get("review"))}</tbody></table>
      </article>
      <article class="panel">
        <h2>Noisy Rules</h2>
        <table><thead><tr><th>Rule</th><th>Total</th><th>Dismissed</th><th>Stale</th></tr></thead><tbody>{_render_noisy_rules(learning.get("actions"))}</tbody></table>
      </article>
      <article class="panel">
        <h2>Stale Action Patterns</h2>
        <table><thead><tr><th>Rule</th><th>Branch</th><th>Status</th><th>Expiry</th></tr></thead><tbody>{_render_stale_action_patterns(learning.get("actions"))}</tbody></table>
      </article>
      <article class="panel">
        <h2>Threshold Recommendations</h2>
        <table><thead><tr><th>Target</th><th>Current</th><th>Proposed</th><th>Priority</th></tr></thead><tbody>{_render_threshold_recommendations(learning.get("recommendations"))}</tbody></table>
      </article>
      <article class="panel">
        <h2>Formatting Drift Notes</h2>
        {_render_format_drift_notes(learning.get("format_drift"))}
      </article>
    </section>
  </main>
</body>
</html>"""


def render_not_found_page(*, path: str, branch: str | None, report_date: str | None) -> str:
    """Render one helpful not-found page for dashboard requests."""

    filters = []
    if branch is not None:
        filters.append(f"branch={branch}")
    if report_date is not None:
        filters.append(f"date={report_date}")
    filter_text = ", ".join(filters) if filters else "no filters supplied"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Analytics Not Found</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: linear-gradient(155deg, #fff7ed, #fee2e2);
      color: #7c2d12;
    }}
    article {{
      max-width: 700px;
      background: rgba(255,255,255,0.95);
      border-radius: 20px;
      padding: 28px;
      box-shadow: 0 20px 48px rgba(124, 45, 18, 0.12);
    }}
    a {{ color: #9a3412; font-weight: 700; }}
  </style>
</head>
<body>
  <article>
    <h1>Analytics Not Found</h1>
    <p>No analytics outputs matched the requested route and filters.</p>
    <p><strong>Route:</strong> {escape(path)}</p>
    <p><strong>Filters:</strong> {escape(filter_text)}</p>
    <p><a href="/dashboard">Back to dashboard</a></p>
  </article>
</body>
</html>"""


def _metric_card(label: str, value: Any) -> str:
    rendered = "n/a" if value is None else escape(str(value))
    return f'<section class="panel"><div class="card-label">{escape(label)}</div><div class="card-value">{rendered}</div></section>'


def _nested(payload: Mapping[str, Any], key: str, child: str) -> Any:
    block = payload.get(key)
    if isinstance(block, Mapping):
        return block.get(child)
    return None


def _render_branch_options(options: list[dict[str, Any]], selected_branch: str) -> str:
    return "".join(
        f'<option value="{escape(str(option["slug"]))}"{" selected" if option["slug"] == selected_branch else ""}>{escape(str(option["display_name"]))}</option>'
        for option in options
    )


def _render_date_options(options: list[str], selected_date: str) -> str:
    return "".join(
        f'<option value="{escape(option)}"{" selected" if option == selected_date else ""}>{escape(option)}</option>'
        for option in options
    )


def _render_warnings(warnings: list[str]) -> str:
    if not warnings:
        return ""
    return '<section class="warning">' + "".join(f"<p>{escape(warning)}</p>" for warning in warnings) + "</section>"


def _render_pending_actions(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="5">No pending operator actions.</td></tr>'
    rendered: list[str] = []
    for row in rows[:8]:
        review_label = "linked" if row.get("linked_review_queue_path") else "none"
        rendered.append(
            "<tr>"
            f"<td>{escape(str(row.get('rule_code') or row.get('action_type') or 'unknown'))}</td>"
            f"<td>{escape(str(row.get('effective_status') or 'pending'))}</td>"
            f"<td>{escape(str(row.get('priority') or 'n/a'))}</td>"
            f"<td>{escape(str(row.get('assigned_to') or 'unassigned'))}</td>"
            f"<td>{escape(review_label)}</td>"
            "</tr>"
        )
    return "".join(rendered)


def _load_learning_bundle(report_date: str) -> dict[str, Any]:
    review = _read_learning_artifact_for_date_or_latest("review_summary", report_date)
    actions = _read_learning_artifact_for_date_or_latest("action_effectiveness", report_date)
    recommendations = _read_learning_artifact_for_date_or_latest("threshold_recommendations", report_date)
    format_drift = _read_learning_artifact_for_date_or_latest("format_drift", report_date)
    artifact_dates = [
        _artifact_date(payload)
        for payload in (review, actions, recommendations, format_drift)
        if isinstance(payload, Mapping)
    ]
    return {
        "artifact_date": max(artifact_dates) if artifact_dates else None,
        "review": review,
        "actions": actions,
        "recommendations": recommendations,
        "format_drift": format_drift,
    }


def _read_learning_artifact_for_date_or_latest(category: str, report_date: str) -> dict[str, Any] | None:
    try:
        dated_path = get_learning_artifact_path(category, report_date)
    except ValueError:
        return None
    payload = _read_json_path(dated_path)
    if payload is not None:
        return payload
    return read_latest_learning_artifact(category)


def _read_json_path(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _artifact_date(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("date")
    if isinstance(value, str) and value.strip():
        return value.strip()
    value = payload.get("report_date")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _top_review_hotspot_label(payload: Any) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    hotspots = payload.get("branch_review_heatmap")
    if not isinstance(hotspots, list) or not hotspots:
        return None
    top = hotspots[0]
    if not isinstance(top, Mapping):
        return None
    return f"{top.get('branch') or 'unknown'} ({top.get('total_reviews') or 0})"


def _top_recommendation_label(payload: Any) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    rows = payload.get("recommendations")
    if not isinstance(rows, list) or not rows:
        return None
    first = rows[0]
    if not isinstance(first, Mapping):
        return None
    return str(first.get("recommendation_id") or first.get("target_area") or "n/a")


def _top_drift_note_label(payload: Any) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    rows = payload.get("frequent_format_issues")
    if not isinstance(rows, list) or not rows:
        return None
    first = rows[0]
    if not isinstance(first, Mapping):
        return None
    return str(first.get("issue_code") or "n/a")


def _daily_issue_notes(learning: Mapping[str, Any]) -> list[str]:
    notes: list[str] = []
    review = learning.get("review")
    if isinstance(review, Mapping):
        hotspots = review.get("branch_review_heatmap")
        if isinstance(hotspots, list) and hotspots:
            top = hotspots[0]
            if isinstance(top, Mapping):
                notes.append(
                    f"Review hotspot: {top.get('branch') or 'unknown'} with {top.get('total_reviews') or 0} review items."
                )
    actions = learning.get("actions")
    if isinstance(actions, Mapping):
        noisy = actions.get("noisy_rules_candidates")
        if isinstance(noisy, list) and noisy:
            top = noisy[0]
            if isinstance(top, Mapping):
                notes.append(
                    f"Noisy rule candidate: {top.get('rule_code') or 'unknown'} with {top.get('total_actions') or 0} actions."
                )
    format_drift = learning.get("format_drift")
    if isinstance(format_drift, Mapping):
        issues = format_drift.get("frequent_format_issues")
        if isinstance(issues, list) and issues:
            top = issues[0]
            if isinstance(top, Mapping):
                notes.append(
                    f"Formatting drift: {top.get('issue_code') or 'unknown'} seen across {top.get('count') or 0} records."
                )
    return notes[:3]


def _daily_recommendation_notes(payload: Any) -> list[str]:
    if not isinstance(payload, Mapping):
        return []
    rows = payload.get("recommendations")
    if not isinstance(rows, list) or not rows:
        return []
    notes: list[str] = []
    for row in rows[:3]:
        if not isinstance(row, Mapping):
            continue
        notes.append(
            f"{row.get('priority') or 'n/a'} priority: {row.get('target_area') or 'unknown'} to {row.get('proposed_threshold') or 'n/a'}."
        )
    return notes


def _daily_risk_notes(learning: Mapping[str, Any]) -> list[str]:
    notes: list[str] = []
    actions = learning.get("actions")
    if isinstance(actions, Mapping):
        stale = actions.get("stale_actions_summary")
        if isinstance(stale, Mapping) and stale.get("count"):
            notes.append(f"Stale action risk: {stale.get('count')} pending actions are past expiry.")
    recommendations = learning.get("recommendations")
    if isinstance(recommendations, Mapping) and recommendations.get("recommendation_count"):
        notes.append(
            f"Threshold review required: {recommendations.get('recommendation_count')} human-approved recommendations are pending."
        )
    review = learning.get("review")
    if isinstance(review, Mapping):
        outcomes = review.get("review_outcomes")
        if isinstance(outcomes, Mapping) and outcomes.get("still_pending"):
            notes.append(f"Review backlog: {outcomes.get('still_pending')} items still pending manual resolution.")
    return notes[:3]


def _render_learning_notes(notes: list[str]) -> str:
    if not notes:
        return '<p class="subtle">No learning highlights available yet.</p>'
    return '<ul class="list-note">' + "".join(f"<li>{escape(note)}</li>" for note in notes) + "</ul>"


def _render_review_hotspots(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return '<tr><td colspan="3">No review learning artifact available yet.</td></tr>'
    rows = payload.get("branch_review_heatmap")
    if not isinstance(rows, list) or not rows:
        return '<tr><td colspan="3">No review hotspots detected.</td></tr>'
    rendered: list[str] = []
    for row in rows[:5]:
        if not isinstance(row, Mapping):
            continue
        reasons = row.get("by_reason")
        leading_reason = "n/a"
        if isinstance(reasons, list) and reasons and isinstance(reasons[0], Mapping):
            leading_reason = str(reasons[0].get("reason") or "n/a")
        rendered.append(
            f"<tr><td>{escape(str(row.get('branch') or 'unknown'))}</td><td>{escape(str(row.get('total_reviews') or 0))}</td><td>{escape(leading_reason)}</td></tr>"
        )
    return "".join(rendered) or '<tr><td colspan="3">No review hotspots detected.</td></tr>'


def _render_noisy_rules(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return '<tr><td colspan="4">No action learning artifact available yet.</td></tr>'
    rows = payload.get("noisy_rules_candidates")
    if not isinstance(rows, list) or not rows:
        return '<tr><td colspan="4">No noisy rules flagged.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('rule_code') or 'unknown'))}</td><td>{escape(str(row.get('total_actions') or 0))}</td><td>{escape(str(row.get('dismissed_rate') or 0))}</td><td>{escape(str(row.get('stale_pending_rate') or 0))}</td></tr>"
        for row in rows[:5]
        if isinstance(row, Mapping)
    )


def _render_stale_action_patterns(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return '<tr><td colspan="4">No action learning artifact available yet.</td></tr>'
    summary = payload.get("stale_actions_summary")
    if not isinstance(summary, Mapping):
        return '<tr><td colspan="4">No stale action patterns available.</td></tr>'
    rows = summary.get("items")
    if not isinstance(rows, list) or not rows:
        return '<tr><td colspan="4">No stale action patterns detected.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('rule_code') or 'unknown'))}</td><td>{escape(str(row.get('branch') or 'unknown'))}</td><td>{escape(str(row.get('effective_status') or 'pending'))}</td><td>{escape(str(row.get('expires_at') or 'n/a'))}</td></tr>"
        for row in rows[:5]
        if isinstance(row, Mapping)
    )


def _render_threshold_recommendations(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return '<tr><td colspan="4">No threshold recommendation artifact available yet.</td></tr>'
    rows = payload.get("recommendations")
    if not isinstance(rows, list) or not rows:
        return '<tr><td colspan="4">No threshold recommendations pending.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('target_area') or 'unknown'))}</td><td>{escape(str(row.get('current_threshold') or 'n/a'))}</td><td>{escape(str(row.get('proposed_threshold') or 'n/a'))}</td><td>{escape(str(row.get('priority') or 'n/a'))}</td></tr>"
        for row in rows[:5]
        if isinstance(row, Mapping)
    )


def _render_format_drift_notes(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return '<p class="subtle">No format drift artifact available yet.</p>'
    issues = payload.get("frequent_format_issues")
    notes = payload.get("branch_training_notes")
    output: list[str] = []
    if isinstance(issues, list) and issues:
        output.append("<h3>Frequent Issues</h3><table><thead><tr><th>Issue</th><th>Count</th><th>Branches</th></tr></thead><tbody>")
        for row in issues[:4]:
            if not isinstance(row, Mapping):
                continue
            output.append(
                f"<tr><td>{escape(str(row.get('issue_code') or 'unknown'))}</td><td>{escape(str(row.get('count') or 0))}</td><td>{escape(', '.join(str(item) for item in (row.get('branches') or [])) or 'n/a')}</td></tr>"
            )
        output.append("</tbody></table>")
    else:
        output.append('<p class="subtle">No recurring formatting drift detected.</p>')
    if isinstance(notes, list) and notes:
        output.append("<h3>Branch Notes</h3>")
        output.append(
            '<ul class="list-note">'
            + "".join(
                f"<li><strong>{escape(str(row.get('branch') or 'unknown'))}:</strong> {escape(str(row.get('note') or 'No note'))}</li>"
                for row in notes[:3]
                if isinstance(row, Mapping)
            )
            + "</ul>"
        )
    return "".join(output)


def _summary_row(label: str, value: Any) -> str:
    rendered = "n/a" if value is None else escape(str(value))
    return f"<tr><th>{escape(label)}</th><td>{rendered}</td></tr>"


def _count_requires_ack(rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in rows if row.get("requires_ack") is True)


def _render_staff_metric_rows(rows: list[dict[str, Any]], metric: str) -> str:
    if not rows:
        return '<tr><td colspan="3">No staff analytics available.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('staff_name') or 'Unknown'))}</td><td>{escape(str(row.get(metric)))}</td><td>{escape(str(row.get('section') or 'unresolved'))}</td></tr>"
        for row in rows[:5]
    )


def _render_staff_low_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="3">No productivity data available.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('staff_name') or 'Unknown'))}</td><td>{escape(str(row.get('activity_score')))}</td><td>{escape(str(row.get('duty_status') or 'unknown'))}</td></tr>"
        for row in rows[:5]
    )


def _render_role_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="3">No role summary available.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('role') or 'unassigned'))}</td><td>{escape(str(row.get('staff_count')))}</td><td>{escape(str(row.get('avg_activity_score')))}</td></tr>"
        for row in rows
    )


def _render_duty_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="3">No duty summary available.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('duty_status') or 'unknown'))}</td><td>{escape(str(row.get('staff_count')))}</td><td>{escape(str(row.get('total_items_moved')))}</td></tr>"
        for row in rows
    )


def _render_flags(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="3">No operational flags.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('code')))}</td><td>{escape(str(row.get('severity')))}</td><td>{escape(str(row.get('message')))}</td></tr>"
        for row in rows
    )


def _render_section_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<tr><td colspan="4">No section analytics available.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('section') or 'unknown'))}</td><td>{escape(str(row.get('productivity_index')))}</td><td>{escape(str(row.get('staff_count')))}</td><td>{escape(str(row.get('items_moved')))}</td></tr>"
        for row in rows[:12]
    )


def _render_rank_rows(rows: list[dict[str, Any]], metric: str) -> str:
    if not rows:
        return '<tr><td colspan="3">No ranking available.</td></tr>'
    return "".join(
        f"<tr><td>{escape(str(row.get('rank')))}</td><td>{escape(display_branch_name(str(row.get('branch') or 'unknown')))}</td><td>{escape(str(row.get(metric)))}</td></tr>"
        for row in rows
    )


def _render_branch_scorecards(rows: list[dict[str, Any]], selected_branch: str) -> str:
    if not rows:
        return '<tr><td colspan="4">No branch comparison available.</td></tr>'
    rendered = []
    for row in rows:
        branch = str(row.get("branch") or "unknown")
        label = display_branch_name(branch)
        if branch == selected_branch:
            label = f"{label} *"
        rendered.append(
            f"<tr><td>{escape(label)}</td><td>{escape(str(row.get('gross_sales')))}</td><td>{escape(str(row.get('operational_score')))}</td><td>{escape(str(row.get('conversion_rate')))}</td></tr>"
        )
    return "".join(rendered)


def _top_by_operational_score(comparison: Mapping[str, Any], *, highest: bool) -> Mapping[str, Any] | None:
    rows = comparison.get("branch_scorecards")
    if not isinstance(rows, list):
        return None
    candidates = [row for row in rows if isinstance(row, Mapping) and row.get("operational_score") is not None]
    if not candidates:
        return None
    ordered = sorted(
        candidates,
        key=lambda row: (
            float(row["operational_score"]),
            str(row.get("branch") or ""),
        ),
        reverse=highest,
    )
    return ordered[0]
