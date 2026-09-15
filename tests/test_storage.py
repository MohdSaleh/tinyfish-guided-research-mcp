import copy

import pytest

from tinyfish_research_mcp import storage


def _use_sqlite(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "DATABASE_URL", "")
    monkeypatch.setattr(storage, "DB_PATH", str(tmp_path / "state.db"))
    storage.clear_process_cache()


def test_state_roundtrip(tmp_path, monkeypatch):
    _use_sqlite(tmp_path, monkeypatch)
    state = {"research_id": "res_test", "executed_queries_set": {"q"}, "sources": {}, "value": 7}
    storage.persist(state)
    assert state["_storage_version"] == 1

    loaded = storage.load_state_raw("res_test")
    assert loaded["value"] == 7
    assert loaded["executed_queries_set"] == {"q"}
    assert loaded["_storage_version"] == 1


def test_state_updates_increment_version(tmp_path, monkeypatch):
    _use_sqlite(tmp_path, monkeypatch)
    state = {"research_id": "res_version", "executed_queries_set": set(), "sources": {}, "value": 1}
    storage.persist(state)
    state["value"] = 2
    storage.persist(state)

    loaded = storage.load_state_raw("res_version")
    assert loaded["value"] == 2
    assert loaded["_storage_version"] == 2


def test_stale_writer_is_rejected(tmp_path, monkeypatch):
    _use_sqlite(tmp_path, monkeypatch)
    state = {"research_id": "res_conflict", "executed_queries_set": set(), "sources": {}, "value": 1}
    storage.persist(state)

    writer_a = storage.load_state_raw("res_conflict")
    writer_b = copy.deepcopy(writer_a)

    writer_a["value"] = 2
    storage.persist(writer_a)

    writer_b["value"] = 3
    with pytest.raises(storage.ConcurrentStateUpdateError):
        storage.persist(writer_b)

    loaded = storage.load_state_raw("res_conflict")
    assert loaded["value"] == 2
    assert loaded["_storage_version"] == 2


def test_source_content_roundtrip(tmp_path, monkeypatch):
    _use_sqlite(tmp_path, monkeypatch)
    storage.store_source_content("res_content", "src_1", "evidence text", "hash")
    assert storage.load_source_content("res_content", "src_1") == "evidence text"
