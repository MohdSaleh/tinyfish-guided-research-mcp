import copy
import os
import uuid

import pytest

from tinyfish_research_mcp import storage


TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is not configured")


def _configure_postgres(monkeypatch):
    monkeypatch.setattr(storage, "DATABASE_URL", TEST_DATABASE_URL)
    storage.clear_process_cache()


def test_postgres_roundtrip_and_optimistic_concurrency(monkeypatch):
    _configure_postgres(monkeypatch)
    research_id = f"res_pg_{uuid.uuid4().hex}"
    source_id = f"src_{uuid.uuid4().hex}"

    state = {
        "research_id": research_id,
        "executed_queries_set": {"query"},
        "sources": {},
        "value": 1,
    }
    storage.persist(state)
    assert state["_storage_version"] == 1

    writer_a = storage.load_state_raw(research_id)
    writer_b = copy.deepcopy(writer_a)
    assert writer_a["executed_queries_set"] == {"query"}

    writer_a["value"] = 2
    storage.persist(writer_a)
    assert writer_a["_storage_version"] == 2

    writer_b["value"] = 3
    with pytest.raises(storage.ConcurrentStateUpdateError):
        storage.persist(writer_b)

    loaded = storage.load_state_raw(research_id)
    assert loaded["value"] == 2
    assert loaded["_storage_version"] == 2

    storage.store_source_content(research_id, source_id, "verified evidence", "hash")
    assert storage.load_source_content(research_id, source_id) == "verified evidence"
