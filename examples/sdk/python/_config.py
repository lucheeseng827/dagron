"""Shared connection config for the SDK examples.

Reads the gateway URL and credentials from the environment, with defaults that
match the local `podman compose up` / `docker compose up` stack
(`dagron/compose.yaml`):

    DAGRON_API_URL   default http://localhost:8080
    DAGRON_TOKEN     a session JWT (skips login if set)
    DAGRON_EMAIL     default admin@local      (compose seeded admin)
    DAGRON_PASSWORD  default dagron-admin      (compose seeded admin)

Override any of them to point at a real deployment, e.g.

    DAGRON_API_URL=https://dagron.example.com \
    DAGRON_EMAIL=me@example.com DAGRON_PASSWORD=... \
    python 01_quickstart.py
"""
from __future__ import annotations

import os

import _bootstrap  # noqa: F401  (side effect: puts the SDK on sys.path)
from dagron import Client

API_URL = os.environ.get("DAGRON_API_URL", "http://localhost:8080")
TOKEN = os.environ.get("DAGRON_TOKEN")
EMAIL = os.environ.get("DAGRON_EMAIL", "admin@local")
PASSWORD = os.environ.get("DAGRON_PASSWORD", "dagron-admin")


def connect() -> Client:
    """Return an authenticated :class:`dagron.Client`.

    Uses ``DAGRON_TOKEN`` when present, otherwise logs in with
    ``DAGRON_EMAIL`` / ``DAGRON_PASSWORD``.
    """
    if TOKEN:
        return Client(API_URL, token=TOKEN)
    api = Client(API_URL)
    api.login(EMAIL, PASSWORD)
    return api
