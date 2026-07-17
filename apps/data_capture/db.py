"""SQLite authoritative store with append-only report and audit records."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
import secrets
import sqlite3
from typing import Any, Iterator, Sequence
from uuid import uuid4

from werkzeug.security import check_password_hash, generate_password_hash

from .domain import ValidatedReport

SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    username TEXT NOT NULL UNIQUE COLLATE NOCASE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('supervisor','acting_supervisor','pricing_clerk','admin')),
    branch TEXT,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
    created_at TEXT NOT NULL,
    deactivated_at TEXT,
    CHECK ((role = 'admin' AND branch IS NULL) OR (role <> 'admin' AND branch IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    submission_group_id TEXT NOT NULL,
    report_type TEXT NOT NULL,
    branch TEXT NOT NULL,
    report_date TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    supersedes_report_id TEXT REFERENCES reports(report_id),
    submitted_by TEXT NOT NULL REFERENCES users(user_id),
    submitted_at TEXT NOT NULL,
    change_reason TEXT,
    metrics_json TEXT NOT NULL,
    details_json TEXT NOT NULL,
    warnings_json TEXT NOT NULL,
    UNIQUE(report_type, branch, report_date, version),
    CHECK ((version = 1 AND supersedes_report_id IS NULL AND change_reason IS NULL)
        OR (version > 1 AND supersedes_report_id IS NOT NULL AND length(trim(change_reason)) > 0))
);
CREATE TABLE IF NOT EXISTS report_lines (
    line_id TEXT PRIMARY KEY,
    report_id TEXT NOT NULL REFERENCES reports(report_id),
    line_type TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    amount_minor INTEGER,
    quantity INTEGER,
    weight_grams INTEGER,
    variance_minor INTEGER,
    UNIQUE(report_id, sequence_no)
);
CREATE TABLE IF NOT EXISTS report_status_events (
    status_event_id TEXT PRIMARY KEY,
    report_id TEXT NOT NULL REFERENCES reports(report_id),
    status TEXT NOT NULL CHECK (status IN ('submitted','superseded')),
    at TEXT NOT NULL,
    actor_user_id TEXT NOT NULL REFERENCES users(user_id),
    detail_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id TEXT PRIMARY KEY,
    at TEXT NOT NULL,
    actor_user_id TEXT REFERENCES users(user_id),
    action TEXT NOT NULL,
    report_id TEXT REFERENCES reports(report_id),
    detail_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS signal_outbox (
    outbox_id TEXT PRIMARY KEY,
    report_id TEXT NOT NULL UNIQUE REFERENCES reports(report_id),
    signal_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','dispatched','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    dispatched_at TEXT,
    last_error TEXT
);
CREATE TABLE IF NOT EXISTS sessions (
    session_id_hash TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(user_id),
    csrf_token TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reports_lookup ON reports(report_type, branch, report_date, version DESC);
CREATE INDEX IF NOT EXISTS reports_submitter ON reports(submitted_by, submitted_at DESC);
CREATE INDEX IF NOT EXISTS audit_at ON audit_log(at DESC);
CREATE INDEX IF NOT EXISTS outbox_state ON signal_outbox(state, created_at);
CREATE TRIGGER IF NOT EXISTS reports_no_update BEFORE UPDATE ON reports BEGIN SELECT RAISE(ABORT, 'reports are append-only'); END;
CREATE TRIGGER IF NOT EXISTS reports_no_delete BEFORE DELETE ON reports BEGIN SELECT RAISE(ABORT, 'reports are retained'); END;
CREATE TRIGGER IF NOT EXISTS lines_no_update BEFORE UPDATE ON report_lines BEGIN SELECT RAISE(ABORT, 'report lines are append-only'); END;
CREATE TRIGGER IF NOT EXISTS lines_no_delete BEFORE DELETE ON report_lines BEGIN SELECT RAISE(ABORT, 'report lines are retained'); END;
CREATE TRIGGER IF NOT EXISTS audit_no_update BEFORE UPDATE ON audit_log BEGIN SELECT RAISE(ABORT, 'audit log is append-only'); END;
CREATE TRIGGER IF NOT EXISTS audit_no_delete BEFORE DELETE ON audit_log BEGIN SELECT RAISE(ABORT, 'audit log is retained'); END;
CREATE TRIGGER IF NOT EXISTS status_no_update BEFORE UPDATE ON report_status_events BEGIN SELECT RAISE(ABORT, 'status events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS status_no_delete BEFORE DELETE ON report_status_events BEGIN SELECT RAISE(ABORT, 'status events are retained'); END;
CREATE TRIGGER IF NOT EXISTS users_no_delete BEFORE DELETE ON users BEGIN SELECT RAISE(ABORT, 'users must be deactivated, not deleted'); END;
"""


class CaptureStore:
    """Database operations for the capture service."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def create_user(
        self, *, full_name: str, username: str, password: str, role: str, branch: str | None, actor_user_id: str | None
    ) -> str:
        full_name = full_name.strip()
        username = username.strip()
        if not full_name or not username or len(password) < 12:
            raise ValueError("Full name, username, and a password of at least 12 characters are required.")
        if role not in {"supervisor", "acting_supervisor", "pricing_clerk", "admin"}:
            raise ValueError("Invalid role.")
        if (role == "admin") != (branch is None):
            raise ValueError("Admin must have no branch; shop accounts require a branch.")
        user_id = str(uuid4())
        now = utc_now()
        with self.transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM users WHERE username = ? COLLATE NOCASE", (username,)
            ).fetchone():
                raise ValueError("Username is already in use.")
            connection.execute(
                "INSERT INTO users VALUES (?,?,?,?,?,?,1,?,NULL)",
                (user_id, full_name, username, generate_password_hash(password, method="scrypt"), role, branch, now),
            )
            self._audit(connection, actor_user_id, "user_created", None, {"user_id": user_id, "role": role, "branch": branch})
        return user_id

    def deactivate_user(self, user_id: str, *, actor_user_id: str) -> None:
        with self.transaction() as connection:
            row = connection.execute("SELECT is_active FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if row is None:
                raise LookupError("User not found.")
            if not row["is_active"]:
                return
            connection.execute("UPDATE users SET is_active = 0, deactivated_at = ? WHERE user_id = ?", (utc_now(), user_id))
            connection.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            self._audit(connection, actor_user_id, "user_deactivated", None, {"user_id": user_id})

    def list_users(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT user_id,full_name,username,role,branch,is_active,created_at,deactivated_at FROM users ORDER BY full_name"
            )]

    def active_supervisors(self, branch: str, *, include_user: str | None = None) -> list[str]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT full_name FROM users WHERE branch = ? AND role IN ('supervisor','acting_supervisor') AND (is_active = 1 OR user_id = ?) ORDER BY full_name",
                (branch, include_user),
            )
            return [row["full_name"] for row in rows]

    def authenticate(self, username: str, password: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute("SELECT * FROM users WHERE username = ? COLLATE NOCASE", (username.strip(),)).fetchone()
            if row is None or not row["is_active"] or not check_password_hash(row["password_hash"], password):
                self._audit(connection, row["user_id"] if row else None, "login_failed", None, {"username": username.strip()})
                connection.commit()
                return None
            self._audit(connection, row["user_id"], "login_succeeded", None, {})
            connection.commit()
            return _public_user(row)

    def create_session(self, user_id: str, *, lifetime: timedelta) -> tuple[str, str]:
        token = secrets.token_urlsafe(32)
        csrf = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO sessions VALUES (?,?,?,?,?,?)",
                (_hash_token(token), user_id, csrf, now.isoformat(), (now + lifetime).isoformat(), now.isoformat()),
            )
        return token, csrf

    def get_session(self, token: str | None) -> tuple[dict[str, Any], str] | None:
        if not token:
            return None
        now = utc_now()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT s.*,u.full_name,u.username,u.role,u.branch,u.is_active FROM sessions s JOIN users u ON u.user_id=s.user_id WHERE s.session_id_hash=?",
                (_hash_token(token),),
            ).fetchone()
            if row is None or not row["is_active"] or row["expires_at"] <= now:
                if row is not None:
                    connection.execute("DELETE FROM sessions WHERE session_id_hash=?", (_hash_token(token),))
                return None
            connection.execute("UPDATE sessions SET last_seen_at=? WHERE session_id_hash=?", (now, _hash_token(token)))
            return _public_user(row), row["csrf_token"]

    def delete_session(self, token: str | None, *, actor_user_id: str | None) -> None:
        with self.transaction() as connection:
            if token:
                connection.execute("DELETE FROM sessions WHERE session_id_hash=?", (_hash_token(token),))
            self._audit(connection, actor_user_id, "logout", None, {})

    def submit(
        self,
        reports: Sequence[ValidatedReport],
        *,
        branch: str,
        submitted_by: str,
        supersedes: dict[str, str] | None = None,
        change_reason: str | None = None,
    ) -> list[str]:
        """Atomically persist report(s), lines, audit, status events, and outbox."""

        supersedes = supersedes or {}
        if supersedes and not str(change_reason or "").strip():
            raise ValueError("A correction reason is required.")
        group_id = str(uuid4())
        report_ids: list[str] = []
        with self.transaction() as connection:
            actor = connection.execute(
                "SELECT full_name,is_active FROM users WHERE user_id=?", (submitted_by,)
            ).fetchone()
            if actor is None or not actor["is_active"]:
                raise ValueError("Submitting user is not active.")
            for report in reports:
                previous_id = supersedes.get(report.report_type)
                version = 1
                if previous_id:
                    previous = connection.execute("SELECT * FROM reports WHERE report_id=?", (previous_id,)).fetchone()
                    if previous is None:
                        raise LookupError("The report being corrected does not exist.")
                    if previous["report_type"] != report.report_type or previous["branch"] != branch or previous["report_date"] != report.report_date:
                        raise ValueError("A correction must keep the original report type, branch, and date.")
                    if self._is_superseded(connection, previous_id):
                        raise ValueError("Only the current report version can be corrected.")
                    version = int(previous["version"]) + 1
                else:
                    current = connection.execute(
                        "SELECT report_id FROM reports r WHERE report_type=? AND branch=? AND report_date=? AND NOT EXISTS (SELECT 1 FROM reports n WHERE n.supersedes_report_id=r.report_id)",
                        (report.report_type, branch, report.report_date),
                    ).fetchone()
                    if current is not None:
                        raise ValueError("A report already exists for this type, branch, and date; submit a correction instead.")

                report_id = str(uuid4())
                now = utc_now()
                connection.execute(
                    "INSERT INTO reports VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        report_id,
                        group_id,
                        report.report_type,
                        branch,
                        report.report_date,
                        version,
                        previous_id,
                        submitted_by,
                        now,
                        str(change_reason).strip() if previous_id else None,
                        _json(report.metrics),
                        _json(report.details),
                        _json(list(report.warnings)),
                    ),
                )
                for sequence, line in enumerate(report.lines, 1):
                    connection.execute(
                        "INSERT INTO report_lines VALUES (?,?,?,?,?,?,?,?,?)",
                        (
                            str(uuid4()), report_id, report.report_type, sequence, _json(line),
                            _minor_units(_first_present(line, "amount", "cash")),
                            _optional_int(_first_present(line, "qty", "items_moved")),
                            _weight_grams(line.get("weight_kg")),
                            _minor_units(line.get("variance")),
                        ),
                    )
                connection.execute(
                    "INSERT INTO report_status_events VALUES (?,?,?,?,?,?)",
                    (str(uuid4()), report_id, "submitted", now, submitted_by, "{}"),
                )
                if previous_id:
                    connection.execute(
                        "INSERT INTO report_status_events VALUES (?,?,?,?,?,?)",
                        (str(uuid4()), previous_id, "superseded", now, submitted_by, _json({"by_report_id": report_id})),
                    )
                action = "report_corrected" if previous_id else "report_submitted"
                self._audit(connection, submitted_by, action, report_id, {"version": version, "supersedes_report_id": previous_id})
                outbox_payload = {
                    "report_id": report_id,
                    "report_type": report.report_type,
                    "branch": branch,
                    "report_date": report.report_date,
                    "version": version,
                    "submitted_by": submitted_by,
                    "submitted_by_name": actor["full_name"],
                    "submitted_at": now,
                    "change_reason": str(change_reason).strip() if previous_id else None,
                    "lines": list(report.lines),
                    "metrics": report.metrics,
                    "details": report.details,
                    "warnings": list(report.warnings),
                }
                connection.execute(
                    "INSERT INTO signal_outbox(outbox_id,report_id,signal_type,payload_json,created_at) VALUES (?,?,?,?,?)",
                    (str(uuid4()), report_id, report.report_type, _json(outbox_payload), now),
                )
                report_ids.append(report_id)
        return report_ids

    def attendance_for(self, branch: str, report_date: str) -> dict[str, str]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT report_id FROM reports r WHERE report_type='hr_attendance' AND branch=? AND report_date=? AND NOT EXISTS (SELECT 1 FROM reports n WHERE n.supersedes_report_id=r.report_id) ORDER BY version DESC LIMIT 1",
                (branch, report_date),
            ).fetchone()
            if row is None:
                return {}
            lines = connection.execute("SELECT payload_json FROM report_lines WHERE report_id=? ORDER BY sequence_no", (row["report_id"],))
            return {item["staff_name"].casefold(): item["status"] for item in (json.loads(line["payload_json"]) for line in lines)}

    def get_report(self, report_id: str, *, viewer: dict[str, Any]) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT r.*,u.full_name submitted_by_name FROM reports r JOIN users u ON u.user_id=r.submitted_by WHERE r.report_id=?", (report_id,)
            ).fetchone()
            if row is None or (viewer["role"] != "admin" and row["branch"] != viewer["branch"]):
                return None
            payload = _report_payload(row)
            payload["lines"] = [json.loads(item["payload_json"]) for item in connection.execute(
                "SELECT payload_json FROM report_lines WHERE report_id=? ORDER BY sequence_no", (report_id,)
            )]
            payload["status"] = "superseded" if self._is_superseded(connection, report_id) else "current"
            return payload

    def history(self, *, viewer: dict[str, Any], branch: str | None = None, report_type: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if viewer["role"] != "admin":
            clauses.append("r.branch=?")
            params.append(viewer["branch"])
        elif branch:
            clauses.append("r.branch=?")
            params.append(branch)
        if report_type:
            clauses.append("r.report_type=?")
            params.append(report_type)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT r.*,u.full_name submitted_by_name,EXISTS(SELECT 1 FROM reports n WHERE n.supersedes_report_id=r.report_id) superseded FROM reports r JOIN users u ON u.user_id=r.submitted_by"
                + where + " ORDER BY r.report_date DESC,r.report_type,r.version DESC LIMIT 500", params,
            )
            return [{**_report_payload(row), "status": "superseded" if row["superseded"] else "current"} for row in rows]

    def group_report_ids(self, submission_group_id: str) -> dict[str, str]:
        """Return report-type IDs for one atomic combined submission."""

        with self.connect() as connection:
            rows = connection.execute(
                "SELECT report_type,report_id FROM reports WHERE submission_group_id=?", (submission_group_id,)
            )
            return {row["report_type"]: row["report_id"] for row in rows}

    def current_reports(self, *, branch: str, start: str, end: str, include_history: bool = False) -> list[dict[str, Any]]:
        condition = "" if include_history else " AND NOT EXISTS (SELECT 1 FROM reports n WHERE n.supersedes_report_id=r.report_id)"
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT r.*,u.full_name submitted_by_name FROM reports r JOIN users u ON u.user_id=r.submitted_by WHERE r.branch=? AND r.report_date BETWEEN ? AND ?"
                + condition + " ORDER BY r.report_date,r.report_type,r.version", (branch, start, end),
            )
            result = []
            for row in rows:
                report = _report_payload(row)
                report["lines"] = [json.loads(item["payload_json"]) for item in connection.execute(
                    "SELECT payload_json FROM report_lines WHERE report_id=? ORDER BY sequence_no", (row["report_id"],)
                )]
                result.append(report)
            return result

    def log_export(self, *, actor_user_id: str, detail: dict[str, Any]) -> None:
        with self.transaction() as connection:
            self._audit(connection, actor_user_id, "export_created", None, detail)

    def pending_outbox(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM signal_outbox WHERE state IN ('pending','failed') ORDER BY created_at LIMIT ?", (limit,)
            )]

    def mark_outbox(self, outbox_id: str, *, success: bool, error: str | None = None) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE signal_outbox SET state=?,attempts=attempts+1,dispatched_at=?,last_error=? WHERE outbox_id=?",
                ("dispatched" if success else "failed", utc_now() if success else None, None if success else str(error)[:1000], outbox_id),
            )

    def _audit(
        self, connection: sqlite3.Connection, actor_user_id: str | None, action: str, report_id: str | None, detail: dict[str, Any]
    ) -> None:
        connection.execute(
            "INSERT INTO audit_log VALUES (?,?,?,?,?,?)", (str(uuid4()), utc_now(), actor_user_id, action, report_id, _json(detail))
        )

    @staticmethod
    def _is_superseded(connection: sqlite3.Connection, report_id: str) -> bool:
        return connection.execute("SELECT 1 FROM reports WHERE supersedes_report_id=?", (report_id,)).fetchone() is not None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash_token(token: str) -> str:
    import hashlib

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _public_user(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in ("user_id", "full_name", "username", "role", "branch", "is_active")}


def _report_payload(row: sqlite3.Row) -> dict[str, Any]:
    result = {key: row[key] for key in (
        "report_id", "submission_group_id", "report_type", "branch", "report_date", "version", "supersedes_report_id",
        "submitted_by", "submitted_at", "change_reason",
    )}
    result["submitted_by_name"] = row["submitted_by_name"]
    result["metrics"] = json.loads(row["metrics_json"])
    result["details"] = json.loads(row["details_json"])
    result["warnings"] = json.loads(row["warnings_json"])
    return result


def _minor_units(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int((Decimal(str(value)) * 100).to_integral_exact())


def _weight_grams(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int((Decimal(str(value)) * 1000).to_integral_exact())


def _optional_int(value: Any) -> int | None:
    return int(value) if value not in (None, "") else None


def _first_present(value: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in value and value[key] not in (None, ""):
            return value[key]
    return None
