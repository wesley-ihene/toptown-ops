"""Flask application for authenticated financial-grade daily capture."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
import os
from pathlib import Path
import secrets
from typing import Any, Callable

import click
from flask import Flask, Response, abort, g, jsonify, make_response, redirect, render_template, request, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from packages.common.paths import REPO_ROOT

from .db import CaptureStore
from .domain import (
    STAFF_ROLES,
    SubmissionError,
    validate_attendance,
    validate_bales,
    validate_sales,
    validate_staff_performance,
)
from .exporter import csv_zip, tabular_reports, xlsx_workbook
from .master_data import MasterData
from .pipeline import PipelineDispatcher

FORM_PERMISSIONS = {
    "sales": {"supervisor", "acting_supervisor"},
    "staff-performance": {"supervisor", "acting_supervisor"},
    "attendance": {"supervisor", "acting_supervisor"},
    "bales": {"pricing_clerk"},
}
REPORT_TO_FORM = {
    "sales_income": "sales",
    "supervisor_control": "sales",
    "hr_performance": "staff-performance",
    "hr_attendance": "attendance",
    "pricing_stock_release": "bales",
}
SESSION_COOKIE = "toptown_capture_session"


def create_app(config: dict[str, Any] | None = None) -> Flask:
    """Create an isolated capture service; no Colony connection is made."""

    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_mapping(
        SECRET_KEY=os.environ.get("TOPTOWN_CAPTURE_SECRET"),
        DATABASE=os.environ.get("TOPTOWN_CAPTURE_DB", str(REPO_ROOT / "data" / "capture.sqlite3")),
        REPO_ROOT=str(REPO_ROOT),
        SESSION_LIFETIME_MINUTES=30,
        SESSION_COOKIE_SECURE=True,
        VARIANCE_THRESHOLD="5.00",
        TESTING=False,
    )
    if config:
        app.config.update(config)
    if not app.config.get("SECRET_KEY"):
        if app.config["TESTING"]:
            app.config["SECRET_KEY"] = "test-only-secret"
        else:
            raise RuntimeError("TOPTOWN_CAPTURE_SECRET must be set to a long random value.")

    store = CaptureStore(app.config["DATABASE"])
    store.initialize()
    masters = MasterData(Path(app.config["REPO_ROOT"]))
    dispatcher = PipelineDispatcher(store, repo_root=app.config["REPO_ROOT"])
    app.extensions["capture_store"] = store
    app.extensions["capture_masters"] = masters
    app.extensions["capture_dispatcher"] = dispatcher

    @app.cli.command("create-admin")
    @click.option("--full-name", prompt=True)
    @click.option("--username", prompt=True)
    @click.password_option(confirmation_prompt=True)
    def create_admin(full_name: str, username: str, password: str) -> None:
        """Create a named administrator for initial service bootstrap."""

        user_id = store.create_user(
            full_name=full_name,
            username=username,
            password=password,
            role="admin",
            branch=None,
            actor_user_id=None,
        )
        click.echo(f"Created administrator {username} ({user_id}).")

    @app.cli.command("set-password")
    @click.option("--username", required=True)
    @click.password_option(confirmation_prompt=True)
    def set_password(username: str, password: str) -> None:
        """Set a temporary password for account recovery."""

        store.set_password_from_cli(username, new_password=password)
        click.echo(f"Set a temporary password for {username}.")

    @app.cli.command("retry-outbox")
    def retry_outbox() -> None:
        """Retry failed or pending structured-record materialization."""

        summary = dispatcher.dispatch_pending()
        click.echo(
            f"Scanned {summary['scanned']}; dispatched {summary['dispatched']}; failed {summary['failed']}."
        )
        if summary["failed"]:
            raise click.ClickException("One or more outbox records remain failed.")

    @app.before_request
    def load_identity_and_check_csrf() -> Response | None:
        session = store.get_session(request.cookies.get(SESSION_COOKIE))
        g.user, g.csrf_token = session if session else (None, None)
        if request.method in {"POST", "PUT", "PATCH", "DELETE"} and request.endpoint != "login":
            if g.user is None:
                abort(401)
            supplied = request.headers.get("X-CSRF-Token") or request.form.get("_csrf")
            if not supplied or not secrets.compare_digest(str(supplied), str(g.csrf_token)):
                abort(400, "Invalid CSRF token.")
        if (
            g.user is not None
            and g.user["must_change_password"]
            and request.endpoint not in {"account_password", "logout", "health", "static"}
        ):
            return redirect(url_for("account_password"))
        return None

    @app.after_request
    def security_headers(response: Response) -> Response:
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self'; script-src 'self'; form-action 'self'; frame-ancestors 'none'"
        if app.config["SESSION_COOKIE_SECURE"]:
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        return response

    @app.get("/login")
    def login_page() -> str | Response:
        if g.user:
            return redirect(url_for("home"))
        return render_template("login.html", login_csrf=_login_token(app))

    @app.post("/login")
    def login() -> Response:
        try:
            _verify_login_token(app, request.form.get("_csrf", ""))
        except (BadSignature, SignatureExpired):
            abort(400, "Invalid or expired CSRF token.")
        user = store.authenticate(request.form.get("username", ""), request.form.get("password", ""))
        if user is None:
            return make_response(render_template("login.html", login_csrf=_login_token(app), error="Invalid username or password."), 401)
        token, _ = store.create_session(user["user_id"], lifetime=timedelta(minutes=int(app.config["SESSION_LIFETIME_MINUTES"])))
        destination = "account_password" if user["must_change_password"] else "home"
        response = make_response(redirect(url_for(destination)))
        response.set_cookie(
            SESSION_COOKIE, token, max_age=int(app.config["SESSION_LIFETIME_MINUTES"]) * 60,
            secure=bool(app.config["SESSION_COOKIE_SECURE"]), httponly=True, samesite="Strict", path="/",
        )
        return response

    @app.route("/account/password", methods=["GET", "POST"])
    @_login_required
    def account_password() -> str | Response:
        error = None
        if request.method == "POST":
            new_password = request.form.get("new_password", "")
            if new_password != request.form.get("confirm_password", ""):
                error = "New passwords do not match."
            else:
                try:
                    store.change_password(
                        g.user["user_id"],
                        current_password=request.form.get("current_password", ""),
                        new_password=new_password,
                    )
                except ValueError as exc:
                    error = str(exc)
                else:
                    token, _ = store.create_session(
                        g.user["user_id"],
                        lifetime=timedelta(minutes=int(app.config["SESSION_LIFETIME_MINUTES"])),
                    )
                    response = make_response(redirect(url_for("home")))
                    response.set_cookie(
                        SESSION_COOKIE, token,
                        max_age=int(app.config["SESSION_LIFETIME_MINUTES"]) * 60,
                        secure=bool(app.config["SESSION_COOKIE_SECURE"]), httponly=True,
                        samesite="Strict", path="/",
                    )
                    return response
        status = 400 if error else 200
        return make_response(
            render_template(
                "password.html", error=error,
                password_change_required=bool(g.user["must_change_password"]),
            ),
            status,
        )

    @app.post("/logout")
    def logout() -> Response:
        store.delete_session(request.cookies.get(SESSION_COOKIE), actor_user_id=g.user["user_id"])
        response = make_response(redirect(url_for("login_page")))
        response.delete_cookie(SESSION_COOKIE, path="/")
        return response

    @app.get("/")
    @_login_required
    def home() -> str:
        branch = g.user["branch"]
        readiness = masters.readiness(branch) if branch else []
        return render_template("home.html", readiness=readiness, permissions=FORM_PERMISSIONS)

    @app.route("/forms/<form_type>", methods=["GET", "POST"])
    @_login_required
    def form_route(form_type: str) -> str | Response:
        if form_type not in FORM_PERMISSIONS:
            abort(404)
        correction_id = request.args.get("correct") or request.form.get("correct_report_id")
        correction = store.get_report(correction_id, viewer=g.user) if correction_id else None
        branch = correction["branch"] if correction else g.user["branch"]
        if not _can_use_form(g.user, form_type, correction=correction):
            abort(403)
        if correction and REPORT_TO_FORM.get(correction["report_type"]) != form_type:
            abort(400, "Correction target does not match this form.")
        if not branch:
            abort(400, "A branch-bound account or correction target is required.")
        roster = masters.staff(branch)
        sections = masters.sections(branch)
        products = masters.products()
        supervisors = store.active_supervisors(branch, include_user=g.user["user_id"])
        context = {
            "form_type": form_type,
            "branch": branch,
            "roster": roster,
            "sections": sections,
            "products": products,
            "supervisors": supervisors,
            "staff_roles": sorted(STAFF_ROLES),
            "today": date.today().isoformat(),
            "correct_report_id": correction_id,
            "correction": correction,
            "errors": [],
            "warnings": [],
        }
        if request.method == "GET":
            return render_template("form.html", **context)
        try:
            data = _request_data(form_type)
            reports = _validate(form_type, data, roster=roster, sections=sections, products=products, supervisors=supervisors, store=store, branch=branch, app=app)
            supersedes: dict[str, str] = {}
            if correction:
                if form_type == "sales":
                    supersedes = store.group_report_ids(correction["submission_group_id"])
                else:
                    supersedes = {correction["report_type"]: correction["report_id"]}
            report_ids = store.submit(
                reports,
                branch=branch,
                submitted_by=g.user["user_id"],
                supersedes=supersedes,
                change_reason=data.get("change_reason"),
            )
            dispatch = dispatcher.dispatch_pending()
        except SubmissionError as error:
            context["errors"] = error.errors
            context["warnings"] = error.warnings
            return make_response(render_template("form.html", **context), 400)
        except (ValueError, LookupError) as error:
            context["errors"] = [str(error)]
            return make_response(render_template("form.html", **context), 400)
        return redirect(url_for("submission_confirmation", report_id=report_ids[0], pipeline_failed=dispatch["failed"]))

    @app.get("/submissions/<report_id>/confirmed")
    @_login_required
    def submission_confirmation(report_id: str) -> str:
        report = store.get_report(report_id, viewer=g.user)
        if report is None:
            abort(404)
        return render_template(
            "confirmed.html",
            report=report,
            pipeline_failed=request.args.get("pipeline_failed") not in {None, "0"},
        )

    @app.get("/history")
    @_login_required
    def history() -> str:
        branch = request.args.get("branch") if g.user["role"] == "admin" else g.user["branch"]
        return render_template(
            "history.html", reports=store.history(viewer=g.user, branch=branch, report_type=request.args.get("report_type")),
            branches=masters.branches(), selected_branch=branch,
        )

    @app.get("/reports/<report_id>")
    @_login_required
    def report_detail(report_id: str) -> str:
        report = store.get_report(report_id, viewer=g.user)
        if report is None:
            abort(404)
        return render_template("report.html", report=report, form_type=REPORT_TO_FORM[report["report_type"]])

    @app.route("/admin/users", methods=["GET", "POST"])
    @_role_required("admin")
    def users() -> str | Response:
        error = None
        if request.method == "POST":
            try:
                action = request.form.get("action", "create_user")
                if action == "deactivate":
                    store.deactivate_user(request.form.get("user_id", ""), actor_user_id=g.user["user_id"])
                elif action == "reset_password":
                    store.reset_password(
                        request.form.get("user_id", ""),
                        new_password=request.form.get("password", ""),
                        actor_user_id=g.user["user_id"],
                    )
                elif action == "create_user":
                    role = request.form.get("role", "")
                    branch = None if role == "admin" else request.form.get("branch")
                    if branch is not None and branch not in masters.branches():
                        raise ValueError("Unknown branch.")
                    store.create_user(
                        full_name=request.form.get("full_name", ""), username=request.form.get("username", ""),
                        password=request.form.get("password", ""), role=role, branch=branch, actor_user_id=g.user["user_id"],
                    )
                else:
                    abort(400, "Unknown user action.")
                return redirect(url_for("users"))
            except PermissionError:
                abort(403)
            except (LookupError, ValueError) as exc:
                error = str(exc)
        return render_template("users.html", users=store.list_users(), branches=masters.branches(), error=error)

    @app.get("/export")
    @_role_required("admin")
    def export() -> Response | str:
        branch = request.args.get("branch", "")
        start = request.args.get("start", "")
        end = request.args.get("end", "")
        output_format = request.args.get("format", "")
        if not output_format:
            return render_template("export.html", branches=masters.branches(), today=date.today().isoformat())
        if branch not in masters.branches():
            abort(400, "Unknown branch.")
        try:
            if date.fromisoformat(start) > date.fromisoformat(end):
                raise ValueError
        except ValueError:
            abort(400, "A valid start/end date range is required.")
        include_history = request.args.get("include_history") == "1"
        tables = tabular_reports(store.current_reports(branch=branch, start=start, end=end, include_history=include_history))
        if output_format == "csv":
            content, mime, extension = csv_zip(tables), "application/zip", "zip"
        elif output_format == "xlsx":
            content, mime, extension = xlsx_workbook(tables), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xlsx"
        else:
            abort(400, "Format must be csv or xlsx.")
        store.log_export(actor_user_id=g.user["user_id"], detail={
            "branch": branch, "start": start, "end": end, "format": output_format, "include_history": include_history,
        })
        response = make_response(content)
        response.headers["Content-Type"] = mime
        response.headers["Content-Disposition"] = f'attachment; filename="toptown_{branch}_{start}_{end}.{extension}"'
        return response

    @app.get("/health")
    def health() -> Response:
        return jsonify({"status": "ok", "service": "toptown-data-capture"})

    return app


def _validate(
    form_type: str, data: dict[str, Any], *, roster: list[str], sections: list[str], products: list[str],
    supervisors: list[str], store: CaptureStore, branch: str, app: Flask,
) -> list[Any]:
    if form_type == "sales":
        return list(validate_sales(data, roster=roster, supervisors=supervisors, variance_threshold=Decimal(app.config["VARIANCE_THRESHOLD"])))
    if form_type == "staff-performance":
        attendance = store.attendance_for(branch, str(data.get("report_date", "")))
        return [validate_staff_performance(data, roster=roster, sections=sections, attendance=attendance)]
    if form_type == "attendance":
        return [validate_attendance(data, roster=roster)]
    if form_type == "bales":
        return [validate_bales(data, product_master=products)]
    raise ValueError("Unknown form type.")


def _request_data(form_type: str) -> dict[str, Any]:
    if request.is_json:
        payload = request.get_json(silent=False)
        if not isinstance(payload, dict):
            raise SubmissionError(["Request body must be an object."])
        return payload
    data = dict(request.form)
    if form_type == "sales":
        data["tills"] = _parallel_rows({
            "label": "till_label", "cashier": "cashier", "assistant": "assistant", "cash": "cash",
            "card": "card", "z_reading": "z_reading", "reason": "over_short_reason",
        })
    elif form_type == "staff-performance":
        data["staff"] = _parallel_rows({
            "staff_name": "staff_name", "section": "section", "role": "staff_role",
            "items_moved": "items_moved", "assisting": "assisting",
        })
    elif form_type == "attendance":
        data["statuses"] = {
            name: status for name, status in zip(request.form.getlist("attendance_name"), request.form.getlist("attendance_status"))
        }
    elif form_type == "bales":
        data["bales"] = _parallel_rows({"item": "item", "weight": "weight", "qty": "qty", "amount": "amount"})
    return data


def _parallel_rows(fields: dict[str, str]) -> list[dict[str, str]]:
    columns = {key: request.form.getlist(form_name) for key, form_name in fields.items()}
    size = max((len(values) for values in columns.values()), default=0)
    return [{key: values[index] if index < len(values) else "" for key, values in columns.items()} for index in range(size)]


def _can_use_form(user: dict[str, Any], form_type: str, *, correction: dict[str, Any] | None) -> bool:
    if user["role"] == "admin":
        return correction is not None
    if user["role"] not in FORM_PERMISSIONS[form_type]:
        return False
    return correction is None or correction["branch"] == user["branch"]


def _login_required(view: Callable[..., Any]) -> Callable[..., Any]:
    from functools import wraps

    @wraps(view)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if g.user is None:
            if request.is_json:
                abort(401)
            return redirect(url_for("login_page", next=request.path))
        return view(*args, **kwargs)

    return wrapped


def _role_required(role: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(view: Callable[..., Any]) -> Callable[..., Any]:
        from functools import wraps

        @wraps(view)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if g.user is None:
                return redirect(url_for("login_page"))
            if g.user["role"] != role:
                abort(403)
            return view(*args, **kwargs)

        return wrapped
    return decorator


def _login_token(app: Flask) -> str:
    return URLSafeTimedSerializer(app.config["SECRET_KEY"], salt="capture-login-csrf").dumps({"purpose": "login"})


def _verify_login_token(app: Flask, token: str) -> None:
    payload = URLSafeTimedSerializer(app.config["SECRET_KEY"], salt="capture-login-csrf").loads(token, max_age=600)
    if payload != {"purpose": "login"}:
        raise BadSignature("Wrong token purpose")
