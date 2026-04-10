"""CLI entrypoint for Orchestra-first WhatsApp ingestion."""

from __future__ import annotations

import argparse
import json
import sys

from packages.whatsapp_ingest import ingest_payload, ingest_payload_file


def main(argv: list[str] | None = None) -> int:
    """Submit one WhatsApp payload through the upstream intake bridge."""

    parser = argparse.ArgumentParser(description="Ingest one WhatsApp payload through Orchestra.")
    parser.add_argument("--payload-file", help="Path to a JSON payload file. If omitted, read JSON from stdin.")
    parser.add_argument("--route", default="/webhook", help="Ingress route to target. Defaults to /webhook.")
    parser.add_argument("--header", action="append", default=[], help="Optional HTTP header in Name:Value form.")
    args = parser.parse_args(argv)

    headers = _parse_headers(args.header)
    if args.payload_file:
        result = ingest_payload_file(args.payload_file, headers=headers, route=args.route)
    else:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict):
            raise ValueError("stdin payload must be a JSON object")
        result = ingest_payload(payload, headers=headers, route=args.route)

    print(json.dumps(result, indent=2, sort_keys=True))
    body = result.get("body")
    if isinstance(body, dict) and body.get("ok") is True:
        return 0
    return 1


def _parse_headers(values: list[str]) -> dict[str, str]:
    """Parse CLI header arguments into a simple mapping."""

    headers: dict[str, str] = {}
    for value in values:
        name, separator, header_value = value.partition(":")
        if not separator or not name.strip() or not header_value.strip():
            raise ValueError(f"Invalid --header value: {value!r}")
        headers[name.strip()] = header_value.strip()
    return headers


if __name__ == "__main__":
    raise SystemExit(main())
