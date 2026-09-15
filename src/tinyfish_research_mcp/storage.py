"""SQLite persistence for research state and fetched source content."""
from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
import time
from enum import Enum
from pathlib import Path
from typing import Any

from .config import DB_PATH

_STATE_CACHE: dict[str, dict[str, Any]] = {}
_SESSION_LOCKS: dict[str, asyncio.Lock] = {}

def _get_db() -> sqlite3.Connection:
    db_path = Path(DB_PATH)
    if db_path.parent != Path("."):
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS research_state ("
        "research_id TEXT PRIMARY KEY, state_json TEXT NOT NULL, updated_at REAL NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS source_content ("
        "research_id TEXT NOT NULL, source_id TEXT NOT NULL, content TEXT NOT NULL, "
        "content_hash TEXT, PRIMARY KEY(research_id, source_id))"
    )
    return conn

def _json_default(obj: Any) -> Any:
    if isinstance(obj, set):
        return sorted(obj)
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def store_source_content(research_id: str, source_id: str, content: str, content_hash: str) -> None:
    conn = _get_db()
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
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT content FROM source_content WHERE research_id=? AND source_id=?",
            (research_id, source_id),
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else ""

def get_source_content(state: dict[str, Any], source_id: str) -> str:
    source = state.get("sources", {}).get(source_id, {})
    content = source.get("content")
    if content is None:
        content = load_source_content(state["research_id"], source_id)
        source["content"] = content
    return content or ""

def persist(state: dict[str, Any]) -> None:
    research_id = state["research_id"]
    _STATE_CACHE[research_id] = state
    serializable = copy.deepcopy(state)
    serializable["executed_queries_set"] = sorted(state.get("executed_queries_set", set()))
    for source in serializable.get("sources", {}).values():
        source.pop("content", None)
    conn = _get_db()
    try:
        with conn:
            conn.execute(
                "INSERT INTO research_state (research_id, state_json, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(research_id) DO UPDATE SET state_json=excluded.state_json, updated_at=excluded.updated_at",
                (research_id, json.dumps(serializable, default=_json_default), time.time()),
            )
    finally:
        conn.close()

def load_state_raw(research_id: str) -> dict[str, Any]:
    if research_id in _STATE_CACHE:
        return _STATE_CACHE[research_id]
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT state_json FROM research_state WHERE research_id = ?", (research_id,)
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise KeyError(f"Unknown research_id={research_id!r}")
    state = json.loads(row[0])
    state["executed_queries_set"] = set(state.get("executed_queries_set", []))
    _STATE_CACHE[research_id] = state
    return state

def session_lock(research_id: str) -> asyncio.Lock:
    return _SESSION_LOCKS.setdefault(research_id, asyncio.Lock())

def clear_process_cache() -> None:
    """Testing/maintenance helper. Persistent SQLite state is untouched."""
    _STATE_CACHE.clear()
    _SESSION_LOCKS.clear()
