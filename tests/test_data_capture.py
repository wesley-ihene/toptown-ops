from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
import re
import sqlite3
from zipfile import ZipFile
from io import BytesIO

import pytest

from apps.data_capture.app import create_app
from apps.data_capture.db import CaptureStore
from apps.data_capture.domain import (
    SubmissionError,
    validate_attendance,
    validate_bales,
    validate_sales,
)
from apps.data_capture.exporter import tabular_reports, xlsx_workbook


@pytest.fixture()
def capture_app(tmp_path):
    (tmp_path / "config" / "sections").mkdir(parents=True)
    (tmp_path / "STAFF").mkdir()
    (tmp_path / "config" / "branches.yaml").write_text(
        "branches:\n  - slug: waigani\n    name: Waigani\n", encoding="utf-8"
    )
    (tmp_path / "config" / "products.yaml").write_text(
        "products:\n  - name: Premium Bale\n", encoding="utf-8"
    )
    (tmp_path / "config" / "sections" / "waigani.yaml").write_text(
        "sections:\n  hr_performance:\n    - Ladies\n    - Mens\n", encoding="utf-8"
    )
    (tmp_path / "STAFF" / "master_staff_list.md").write_text(
        "# Staff\n\n## waigani\n\n| Staff Name | Active |\n| --- | --- |\n"
        "| Alice Supervisor | yes |\n| Bob Cashier | yes |\n| Former Worker | no |\n",
        encoding="utf-8",
    )
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "capture-test-secret",
            "DATABASE": str(tmp_path / "capture.sqlite3"),
            "REPO_ROOT": str(tmp_path),
            "SESSION_COOKIE_SECURE": False,
        }
    )
    store: CaptureStore = app.extensions["capture_store"]
    store.create_user(
        full_name="Alice Supervisor",
        username="alice",
        password="safe-password-123",
        role="supervisor",
        branch="waigani",
        actor_user_id=None,
    )
    store.create_user(
        full_name="Peter Pricing",
        username="peter",
        password="safe-password-456",
        role="pricing_clerk",
        branch="waigani",
        actor_user_id=None,
    )
    # These fixture accounts represent users that predate the additive migration.
    with store.connect() as connection:
        connection.execute("UPDATE users SET must_change_password=0")
    return app


def _login(client, username="alice", password="safe-password-123"):
    page = client.get("/login")
    token = re.search(rb'name="_csrf" value="([^"]+)"', page.data).group(1).decode()
    return client.post(
        "/login",
        data={"_csrf": token, "username": username, "password": password},
        follow_redirects=False,
    )


def _csrf(client, path="/"):
    page = client.get(path)
    return re.search(rb'name="_csrf" value="([^"]+)"', page.data).group(1).decode()


def test_sales_validation_reconciles_totals_and_control_record():
    sales, control = validate_sales(
        {
            "report_date": date.today().isoformat(),
            "tills": [
                {
                    "label": "Till 1",
                    "cashier": "Bob Cashier",
                    "assistant": "",
                    "cash": "100.00",
                    "card": "25.25",
                    "z_reading": "125.25",
                    "reason": "",
                }
            ],
            "main_door": "20",
            "served": "10",
            "staff_on_duty": "2",
            "labour_hours": "16",
            "balanced_by": "Alice Supervisor",
            "staffing_issue": "none",
            "stock_issue": "no",
            "pricing_issue": "no",
            "exceptions_escalated": "None",
            "supervisor_confirmation": "yes",
        },
        roster=["Alice Supervisor", "Bob Cashier"],
        supervisors=["Alice Supervisor"],
        variance_threshold=Decimal("5.00"),
    )

    assert sales.metrics["total_sales"] == "125.25"
    assert sales.metrics["conversion_rate"] == "0.5000"
    assert sales.lines[0]["balanced"] is True
    assert control.report_type == "supervisor_control"
    assert control.metrics["cash_variance"] == "0.00"


def test_sales_validation_rejects_unexplained_variance():
    with pytest.raises(SubmissionError) as raised:
        validate_sales(
            {
                "report_date": date.today().isoformat(),
                "tills": [{"label": "T1", "cashier": "Bob", "cash": "10", "card": "0", "z_reading": "0"}],
                "main_door": "1",
                "served": "1",
                "staff_on_duty": "1",
                "labour_hours": "1",
                "balanced_by": "Alice",
                "staffing_issue": "none",
                "stock_issue": "no",
                "pricing_issue": "no",
                "exceptions_escalated": "None",
                "supervisor_confirmation": "yes",
            },
            roster=["Bob"],
            supervisors=["Alice"],
        )

    assert any("Over/Short Reason" in error for error in raised.value.errors)
    assert any("Cash Variance explanation" in error for error in raised.value.errors)


def test_attendance_is_roster_complete_and_bales_flag_unknown_products():
    attendance = validate_attendance(
        {"report_date": date.today().isoformat(), "statuses": {"Alice": "present", "Bob": "off"}},
        roster=["Alice", "Bob"],
    )
    assert attendance.metrics["total_staff_records"] == 2
    with pytest.raises(SubmissionError):
        validate_attendance(
            {"report_date": date.today().isoformat(), "statuses": {"Alice": "present"}},
            roster=["Alice", "Bob"],
        )

    bales = validate_bales(
        {
            "report_date": date.today().isoformat(),
            "bales": [{"item": "Unlisted Bale", "weight": "10.500", "qty": "5", "amount": "50.00"}],
        },
        product_master=["Premium Bale"],
    )
    assert bales.lines[0]["item_resolved"] is False
    assert "not resolved" in bales.warnings[0]


def test_store_versions_reports_and_preserves_zero_quantity(capture_app):
    store: CaptureStore = capture_app.extensions["capture_store"]
    user = store.authenticate("peter", "safe-password-456")
    report = validate_bales(
        {
            "report_date": date.today().isoformat(),
            "bales": [{"item": "Premium Bale", "weight": "0", "qty": "0", "amount": "0"}],
        },
        product_master=["Premium Bale"],
    )
    first_id = store.submit([report], branch="waigani", submitted_by=user["user_id"])[0]
    with store.connect() as connection:
        indexed = connection.execute(
            "SELECT quantity,weight_grams,amount_minor FROM report_lines WHERE report_id=?", (first_id,)
        ).fetchone()
    assert tuple(indexed) == (0, 0, 0)

    second_id = store.submit(
        [report],
        branch="waigani",
        submitted_by=user["user_id"],
        supersedes={"pricing_stock_release": first_id},
        change_reason="Confirmed zero-release day.",
    )[0]
    current = store.get_report(second_id, viewer=user)
    old = store.get_report(first_id, viewer=user)
    assert current["version"] == 2 and current["status"] == "current"
    assert old["status"] == "superseded"
    assert len(store.pending_outbox()) == 2


def test_authenticated_form_submission_materializes_via_outbox(capture_app, tmp_path):
    client = capture_app.test_client()
    assert _login(client).status_code == 302
    token = _csrf(client, "/forms/sales")
    response = client.post(
        "/forms/sales",
        data={
            "_csrf": token,
            "report_date": date.today().isoformat(),
            "till_label": "Till 1",
            "cashier": "Bob Cashier",
            "assistant": "",
            "cash": "100.00",
            "card": "25.00",
            "z_reading": "125.00",
            "over_short_reason": "",
            "main_door": "20",
            "served": "10",
            "staff_on_duty": "2",
            "labour_hours": "16",
            "balanced_by": "Alice Supervisor",
            "notes": "",
            "staffing_issue": "none",
            "stock_issue": "no",
            "pricing_issue": "no",
            "exceptions_escalated": "None",
            "supervisor_confirmation": "on",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302
    store: CaptureStore = capture_app.extensions["capture_store"]
    assert store.pending_outbox() == []
    records = list((tmp_path / "records" / "structured").rglob("*.json"))
    assert any(path.name == f"{date.today().isoformat()}.json" for path in records)

    client.post("/logout", data={"_csrf": _csrf(client)})
    assert _login(client, "peter", "safe-password-456").status_code == 302
    assert client.get("/forms/sales").status_code == 403


def test_csrf_duplicate_user_cli_and_xlsx(capture_app):
    client = capture_app.test_client()
    _login(client)
    assert client.post("/logout", data={"_csrf": "wrong"}).status_code == 400

    runner = capture_app.test_cli_runner()
    created = runner.invoke(
        args=["create-admin", "--full-name", "Ops Admin", "--username", "ops", "--password", "admin-password-123"]
    )
    assert created.exit_code == 0
    duplicate = runner.invoke(
        args=["create-admin", "--full-name", "Other Admin", "--username", "OPS", "--password", "admin-password-456"]
    )
    assert duplicate.exit_code != 0
    assert "already in use" in str(duplicate.exception)

    content = xlsx_workbook(tabular_reports([]))
    with ZipFile(BytesIO(content)) as archive:
        assert "xl/workbook.xml" in archive.namelist()
        assert len([name for name in archive.namelist() if name.startswith("xl/worksheets/")]) == 5


def test_new_user_is_forced_to_change_password_and_old_sessions_die(capture_app):
    store: CaptureStore = capture_app.extensions["capture_store"]
    user_id = store.create_user(
        full_name="New Pricing Clerk",
        username="new-clerk",
        password="temporary-pass-123",
        role="pricing_clerk",
        branch="waigani",
        actor_user_id=None,
    )
    client = capture_app.test_client()
    other_client = capture_app.test_client()
    login = _login(client, "new-clerk", "temporary-pass-123")
    assert login.headers["Location"].endswith("/account/password")
    assert _login(other_client, "new-clerk", "temporary-pass-123").status_code == 302

    blocked = client.get("/forms/bales")
    assert blocked.status_code == 302
    assert blocked.headers["Location"].endswith("/account/password")

    token = _csrf(client, "/account/password")
    wrong_current = client.post(
        "/account/password",
        data={
            "_csrf": token,
            "current_password": "incorrect-current-password",
            "new_password": "replacement-pass-123",
            "confirm_password": "replacement-pass-123",
        },
    )
    assert wrong_current.status_code == 400
    short = client.post(
        "/account/password",
        data={
            "_csrf": token,
            "current_password": "temporary-pass-123",
            "new_password": "too-short",
            "confirm_password": "too-short",
        },
    )
    assert short.status_code == 400

    changed = client.post(
        "/account/password",
        data={
            "_csrf": token,
            "current_password": "temporary-pass-123",
            "new_password": "replacement-pass-123",
            "confirm_password": "replacement-pass-123",
        },
        follow_redirects=False,
    )
    assert changed.status_code == 302
    assert client.get("/forms/bales").status_code == 200
    assert other_client.get("/").headers["Location"].startswith("/login")
    assert store.authenticate("new-clerk", "temporary-pass-123") is None
    assert store.authenticate("new-clerk", "replacement-pass-123")["must_change_password"] == 0
    with store.connect() as connection:
        audit = connection.execute(
            "SELECT actor_user_id,detail_json FROM audit_log WHERE action='password_changed' ORDER BY at DESC LIMIT 1"
        ).fetchone()
    assert audit["actor_user_id"] == user_id
    assert json.loads(audit["detail_json"])["target_user_id"] == user_id


def test_admin_reset_is_authorized_temporary_and_audited(capture_app):
    store: CaptureStore = capture_app.extensions["capture_store"]
    admin_id = store.create_user(
        full_name="Ops Admin",
        username="ops-admin",
        password="admin-initial-pass",
        role="admin",
        branch=None,
        actor_user_id=None,
    )
    with store.connect() as connection:
        connection.execute("UPDATE users SET must_change_password=0 WHERE user_id=?", (admin_id,))
        target_id = connection.execute("SELECT user_id FROM users WHERE username='peter'").fetchone()["user_id"]

    target_client = capture_app.test_client()
    assert _login(target_client, "peter", "safe-password-456").status_code == 302
    non_admin = capture_app.test_client()
    assert _login(non_admin).status_code == 302
    rejected = non_admin.post(
        "/admin/users",
        data={
            "_csrf": _csrf(non_admin),
            "action": "reset_password",
            "user_id": target_id,
            "password": "unauthorized-pass-123",
        },
    )
    assert rejected.status_code == 403

    admin = capture_app.test_client()
    assert _login(admin, "ops-admin", "admin-initial-pass").status_code == 302
    token = _csrf(admin, "/admin/users")
    short = admin.post(
        "/admin/users",
        data={"_csrf": token, "action": "reset_password", "user_id": target_id, "password": "short"},
    )
    assert short.status_code == 200
    reset = admin.post(
        "/admin/users",
        data={
            "_csrf": token,
            "action": "reset_password",
            "user_id": target_id,
            "password": "temporary-reset-123",
        },
        follow_redirects=False,
    )
    assert reset.status_code == 302
    assert target_client.get("/").headers["Location"].startswith("/login")
    assert store.authenticate("peter", "safe-password-456") is None
    assert store.authenticate("peter", "temporary-reset-123")["must_change_password"] == 1
    with store.connect() as connection:
        audit = connection.execute(
            "SELECT actor_user_id,detail_json FROM audit_log WHERE action='password_reset' ORDER BY at DESC LIMIT 1"
        ).fetchone()
    assert audit["actor_user_id"] == admin_id
    assert json.loads(audit["detail_json"])["target_user_id"] == target_id


def test_cli_set_password_invalidates_sessions_and_identifies_cli_actor(capture_app):
    store: CaptureStore = capture_app.extensions["capture_store"]
    client = capture_app.test_client()
    assert _login(client, "peter", "safe-password-456").status_code == 302
    runner = capture_app.test_cli_runner()
    short = runner.invoke(args=["set-password", "--username", "peter"], input="short\nshort\n")
    assert short.exit_code != 0
    changed = runner.invoke(
        args=["set-password", "--username", "peter"],
        input="recovery-password-123\nrecovery-password-123\n",
    )
    assert changed.exit_code == 0
    assert client.get("/").headers["Location"].startswith("/login")
    assert store.authenticate("peter", "safe-password-456") is None
    user = store.authenticate("peter", "recovery-password-123")
    assert user["must_change_password"] == 1
    with store.connect() as connection:
        audit = connection.execute(
            "SELECT actor_user_id,detail_json FROM audit_log WHERE action='password_reset' ORDER BY at DESC LIMIT 1"
        ).fetchone()
    detail = json.loads(audit["detail_json"])
    assert audit["actor_user_id"] is None
    assert detail == {"actor": "cli", "target_user_id": user["user_id"]}


def test_password_flag_migration_is_idempotent_and_preserves_existing_users(tmp_path):
    database = tmp_path / "legacy.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE users (user_id TEXT PRIMARY KEY,full_name TEXT NOT NULL,username TEXT NOT NULL UNIQUE,"
        "password_hash TEXT NOT NULL,role TEXT NOT NULL,branch TEXT,is_active INTEGER NOT NULL DEFAULT 1,"
        "created_at TEXT NOT NULL,deactivated_at TEXT)"
    )
    connection.execute(
        "INSERT INTO users (user_id,full_name,username,password_hash,role,branch,is_active,created_at,deactivated_at) "
        "VALUES ('legacy-admin','Legacy Admin','admin@taop','not-used','admin',NULL,1,'2026-01-01',NULL)"
    )
    connection.commit()
    connection.close()

    store = CaptureStore(database)
    store.initialize()
    store.initialize()
    with store.connect() as migrated:
        columns = [row["name"] for row in migrated.execute("PRAGMA table_info(users)")]
        existing = migrated.execute(
            "SELECT username,must_change_password FROM users WHERE user_id='legacy-admin'"
        ).fetchone()
    assert columns.count("must_change_password") == 1
    assert tuple(existing) == ("admin@taop", 0)
