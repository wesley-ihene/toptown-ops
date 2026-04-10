"""Thin Orchestra-owned WhatsApp intake helpers."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
from typing import Any, Final

from scripts.whatsapp_webhook_bridge import dispatch_http_request

INGEST_SOURCE: Final[str] = "whatsapp"
DEFAULT_ROUTE: Final[str] = "/webhook"


def ingest_payload(
    payload: Mapping[str, Any],
    *,
    headers: Mapping[str, str] | None = None,
    route: str = DEFAULT_ROUTE,
) -> dict[str, Any]:
    """Submit one inbound WhatsApp payload through the Orchestra-first bridge."""

    response = dispatch_http_request(
        method="POST",
        target=route,
        body=json.dumps(dict(payload)).encode("utf-8"),
        headers=dict(headers or {}),
    )
    return {
        "status_code": int(response.status_code),
        "body": json.loads(response.body.decode("utf-8")),
    }


def ingest_payload_file(
    payload_file: str | Path,
    *,
    headers: Mapping[str, str] | None = None,
    route: str = DEFAULT_ROUTE,
) -> dict[str, Any]:
    """Load one JSON payload from disk and submit it through the intake bridge."""

    payload = json.loads(Path(payload_file).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("WhatsApp intake payload file must contain a JSON object.")
    return ingest_payload(payload, headers=headers, route=route)
