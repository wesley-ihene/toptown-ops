"""Authenticated, append-only operational data-capture service."""

from .app import create_app

__all__ = ["create_app"]
