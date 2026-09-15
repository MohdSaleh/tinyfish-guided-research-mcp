"""Durable persistence for research state and fetched source content.

SQLite is the zero-config local/development backend. Set DATABASE_URL to a
PostgreSQL URL for shared, multi-replica production deployments.
"""

from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
import time
from enum import Enum
from pathlib import Path
from typing import Any

from .config import DATABASE_URL, DB_PATH

_SESSION_LOCKS: dict[str, asyncio.Lock] = {}


class ConcurrentStateUpdateError(RuntimeError):
    """Raised when another worker updated a research session first."""


def _using_postgres() -> bool:
    return DATABASE_URL.startswith(("postgres://", "postgresql://"))


def storage_backend() -> str:
    return "postgresql" if _using_postgres() else "sqlite"


def _json_default(obj: Any) -> Any:
    if isinstance(obj, set):
        return sorted(obj)
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _serialize_state(state: dict[str, Any]) -> str:
    serializable = copy.deepcopy(state)
    serializable.pop("_storage_version", None)
    serializable["executed_queries_set"] = sorted(state.get("executed_queries_set", set()))
    for source in serializable.get("sources", {}).values():
        source.pop("content", None)
    return json.dumps(serializable, default=_json_default)


def _prepare_loaded_state(payload: str, version: int) -> dict[str, Any]:
    state = json.loads(payload)
    state["executed_queries_set"] = set(state.get("executed_queries_set", []))
    state["_storage_version"] = int(version)
    return state


def _get_sqlite() -> sqlite3.Connection:
    db_path = Path(DB_PATH)
    if db_path.parent != Path("."):
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS research_state ("
        "research_id TEXT PRIMARY KEY, state_json TEXT NOT NULL, "
        "updated_at REAL NOT NULL, version INTEGER NOT NULL DEFAULT 1)"
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(research_state)").fetchall()}
    if "version" not in columns:
        conn.execute("ALTER TABLE research_state ADD COLUMN version INTEGER NOT NULL DEFAULT 1")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS source_content ("
        "research_id TEXT NOT NULL, source_id TEXT NOT NULL, content TEXT NOT NULL, "
        "content_hash TEXT, PRIMARY KEY(research_id, source_id))"
    )
    return conn


def _get_postgres() -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - packaging guarantees this in production
        raise RuntimeError(
            "DATABASE_URL is PostgreSQL but psycopg is not installed. Install the package dependencies."
        ) from exc

    conn = psycopg.connect(DATABASE_URL, connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS research_state ("
            "research_id TEXT PRIMARY KEY, state_json TEXT NOT NULL, "
            "updated_at DOUBLE PRECISION NOT NULL, version BIGINT NOT NULL DEFAULT 1)"
        )
        cur.execute("ALTER TABLE research_state ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS source_content ("
            "research_id TEXT NOT NULL, source_id TEXT NOT NULL, content TEXT NOT NULL, "
            "content_hash TEXT, PRIMARY KEY(research_id, source_id))"
        )
    conn.commit()
    return conn


def store_source_content(research_id: str, source_id: str, content: str, content_hash: str) -> None:
    if _using_postgres():
        conn = _get_postgres()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO source_content (research_id, source_id, content, content_hash) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT(research_id, source_id) DO UPDATE SET "
                    "content=excluded.content, content_hash=excluded.content_hash",
                    (research_id, source_id, content, content_hash),
                )
            conn.commit()
        finally:
            conn.close()
        return

    conn = _get_sqlite()
    try:
        with conn:
            conn.execute(
                "INSERT INTO source_content (research_id, source_id, content, content_hash) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(research_id, source_id) DO UPDATE SET content=excluded.content, content_hash=excluded.content_hash",
                (research_id, source_id, content, content_hash),
            )
    finally:
        conn.close()


def load_source_content(research_id: str, source_id: str) -> str:
    if _using_postgres():
        conn = _get_postgres()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT content FROM source_content WHERE research_id=%s AND source_id=%s",
                    (research_id, source_id),
                )
                row = cur.fetchone()
        finally:
            conn.close()
        return str(row[0]) if row else ""

    conn = _get_sqlite()
    try:
        row = conn.execute(
            "SELECT content FROM source_content WHERE research_id=? AND source_id=?",
            (research_id, source_id),
        ).fetchone()
    finally:
        conn.close()
    return str(row[0]) if row else ""


def get_source_content(state: dict[str, Any], source_id: str) -> str:
    source = state.get("sources", {}).get(source_id, {})
    content = source.get("content")
    if content is None:
        content = load_source_content(state["research_id"], source_id)
        source["content"] = content
    return content or ""


def persist(state: dict[str, Any]) -> None:
    """Persist a session using optimistic concurrency control.

    Each load carries a storage version. Updates succeed only when that version
    is still current, preventing two Horizon replicas from silently overwriting
    one another. Clients can safely retry after a conflict.
    """

    research_id = state["research_id"]
    payload = _serialize_state(state)
    now = time.time()
    expected_version = state.get("_storage_version")

    if _using_postgres():
        conn = _get_postgres()
        try:
            with conn.cursor() as cur:
                if expected_version is None:
                    cur.execute(
                        "INSERT INTO research_state (research_id, state_json, updated_at, version) "
                        "VALUES (%s, %s, %s, 1) ON CONFLICT(research_id) DO NOTHING RETURNING version",
                        (research_id, payload, now),
                    )
                else:
                    cur.execute(
                        "UPDATE research_state SET state_json=%s, updated_at=%s, version=version+1 "
                        "WHERE research_id=%s AND version=%s RETURNING version",
                        (payload, now, research_id, int(expected_version)),
                    )
                row = cur.fetchone()
                if row is None:
                    conn.rollback()
                    raise ConcurrentStateUpdateError(
                        f"Research session {research_id!r} changed concurrently; reload and retry the tool call."
                    )
                new_version = int(row[0])
            conn.commit()
        finally:
            conn.close()
        state["_storage_version"] = new_version
        return

    conn = _get_sqlite()
    try:
        with conn:
            if expected_version is None:
                try:
                    conn.execute(
                        "INSERT INTO research_state (research_id, state_json, updated_at, version) VALUES (?, ?, ?, 1)",
                        (research_id, payload, now),
                    )
                    new_version = 1
                except sqlite3.IntegrityError as exc:
                    raise ConcurrentStateUpdateError(
                        f"Research session {research_id!r} already exists; reload before updating it."
                    ) from exc
            else:
                cursor = conn.execute(
                    "UPDATE research_state SET state_json=?, updated_at=?, version=version+1 "
                    "WHERE research_id=? AND version=?",
                    (payload, now, research_id, int(expected_version)),
                )
                if cursor.rowcount != 1:
                    raise ConcurrentStateUpdateError(
                        f"Research session {research_id!r} changed concurrently; reload and retry the tool call."
                    )
                new_version = int(expected_version) + 1
        state["_storage_version"] = new_version
    finally:
        conn.close()


def load_state_raw(research_id: str) -> dict[str, Any]:
    """Load fresh state from durable storage.

    There is intentionally no process-local state cache: remote replicas must
    never serve a stale research session after another replica commits a tool.
    """

    if _using_postgres():
        conn = _get_postgres()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT state_json, version FROM research_state WHERE research_id=%s",
                    (research_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()
    else:
        conn = _get_sqlite()
        try:
            row = conn.execute(
                "SELECT state_json, version FROM research_state WHERE research_id=?", (research_id,)
            ).fetchone()
        finally:
            conn.close()

    if row is None:
        raise KeyError(f"Unknown research_id={research_id!r}")
    return _prepare_loaded_state(str(row[0]), int(row[1]))


def session_lock(research_id: str) -> asyncio.Lock:
    """Local optimization only; database version checks provide cross-replica safety."""

    return _SESSION_LOCKS.setdefault(research_id, asyncio.Lock())


def clear_process_cache() -> None:
    """Testing/maintenance helper. Persistent database state is untouched."""

    _SESSION_LOCKS.clear()
