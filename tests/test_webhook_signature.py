"""Tests for Meta webhook authentication."""

from __future__ import annotations

import hashlib
import hmac
import json

from scripts import whatsapp_webhook_bridge as bridge
from scripts.whatsapp_webhook_signature import verify_meta_signature


APP_SECRET = b"test-app-secret"
RAW_BODY = b'{"object":"whatsapp_business_account","entry":[]}'


def _signature(body: bytes = RAW_BODY, secret: bytes = APP_SECRET) -> str:
    digest = hmac.new(secret, body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def test_valid_signature_is_accepted() -> None:
    assert verify_meta_signature(RAW_BODY, _signature(), app_secret=APP_SECRET) is True


def test_tampered_body_is_rejected() -> None:
    assert verify_meta_signature(RAW_BODY + b" ", _signature(), app_secret=APP_SECRET) is False


def test_missing_signature_header_is_rejected() -> None:
    assert verify_meta_signature(RAW_BODY, None, app_secret=APP_SECRET) is False


def test_wrong_signature_prefix_is_rejected() -> None:
    assert verify_meta_signature(
        RAW_BODY,
        _signature().replace("sha256=", "sha1="),
        app_secret=APP_SECRET,
    ) is False


def test_malformed_signature_digest_is_rejected() -> None:
    assert verify_meta_signature(
        RAW_BODY,
        "sha256=" + ("z" * 64),
        app_secret=APP_SECRET,
    ) is False


def test_wrong_secret_is_rejected() -> None:
    assert verify_meta_signature(RAW_BODY, _signature(), app_secret=b"wrong-secret") is False


def test_unset_secret_fails_closed(monkeypatch) -> None:
    monkeypatch.delenv("WHATSAPP_APP_SECRET", raising=False)

    assert verify_meta_signature(RAW_BODY, _signature()) is False


def test_post_rejects_invalid_signature_before_parsing(monkeypatch) -> None:
    monkeypatch.setenv("WHATSAPP_APP_SECRET", APP_SECRET.decode("utf-8"))

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=b"not-json",
        headers={"X-Hub-Signature-256": _signature()},
    )

    payload = json.loads(response.body)
    assert response.status_code == 403
    assert payload["ok"] is False
    assert payload["error_stage"] == "signature_verification"


def test_post_with_valid_signature_reaches_existing_processing(monkeypatch) -> None:
    monkeypatch.setenv("WHATSAPP_APP_SECRET", APP_SECRET.decode("utf-8"))

    response = bridge.dispatch_http_request(
        method="POST",
        target="/webhook",
        body=RAW_BODY,
        headers={"X-Hub-Signature-256": _signature()},
    )

    payload = json.loads(response.body)
    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["reason"] == "no_supported_messages"


def test_verification_accepts_correct_token_with_constant_time_compare(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []
    compare_digest = hmac.compare_digest

    def capture_compare_digest(supplied: str, expected: str) -> bool:
        calls.append((supplied, expected))
        return compare_digest(supplied, expected)

    monkeypatch.setenv("WHATSAPP_VERIFY_TOKEN", "expected-token")
    monkeypatch.setattr(bridge.hmac, "compare_digest", capture_compare_digest)

    response = bridge._handle_verification(
        "hub.mode=subscribe&hub.challenge=challenge&hub.verify_token=expected-token"
    )

    assert response.status_code == 200
    assert response.body == b"challenge"
    assert calls == [("expected-token", "expected-token")]


def test_verification_rejects_wrong_token_with_constant_time_compare(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []
    compare_digest = hmac.compare_digest

    def capture_compare_digest(supplied: str, expected: str) -> bool:
        calls.append((supplied, expected))
        return compare_digest(supplied, expected)

    monkeypatch.setenv("WHATSAPP_VERIFY_TOKEN", "expected-token")
    monkeypatch.setattr(bridge.hmac, "compare_digest", capture_compare_digest)

    response = bridge._handle_verification(
        "hub.mode=subscribe&hub.challenge=challenge&hub.verify_token=wrong-token"
    )

    assert response.status_code == 403
    assert calls == [("wrong-token", "expected-token")]
