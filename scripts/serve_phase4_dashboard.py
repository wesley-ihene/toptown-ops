"""Serve the read-only Phase 4 dashboard and analytics API."""

from __future__ import annotations

import argparse

from analytics.phase4_portal import DEFAULT_HOST, DEFAULT_PORT, serve


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve the Phase 4 read-only dashboard and analytics API.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Bind host. Defaults to 0.0.0.0.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Bind port. Defaults to 8010.")
    parser.add_argument("--root", help="Repository root override.")
    args = parser.parse_args(argv)

    server = serve(host=args.host, port=args.port, root=args.root)
    print(f"http://{args.host}:{args.port}/dashboard")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
