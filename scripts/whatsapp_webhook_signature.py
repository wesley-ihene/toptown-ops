"""Meta webhook signature verification."""

from __future__ import annotations

import hashlib
import hmac
import os


def verify_meta_signature(
    raw_body: bytes,
    signature_header: str | None,
    *,
    app_secret: bytes | None = None,
) -> bool:
    """Return whether a raw webhook body has a valid Meta HMAC signature."""

    secret = app_secret
    if secret is None:
        configured_secret = os.getenv("WHATSAPP_APP_SECRET")
        if configured_secret is None:
            return False
        secret = configured_secret.encode("utf-8")

    if not secret or not signature_header or not signature_header.startswith("sha256="):
        return False

    supplied_digest = signature_header.removeprefix("sha256=")
    if len(supplied_digest) != hashlib.sha256().digest_size * 2:
        return False

    try:
        bytes.fromhex(supplied_digest)
    except ValueError:
        return False

    expected_digest = hmac.new(secret, raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(supplied_digest, expected_digest)
