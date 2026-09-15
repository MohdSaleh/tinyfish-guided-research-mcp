from tinyfish_research_mcp import storage


def test_state_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "DB_PATH", str(tmp_path / "state.db"))
    storage.clear_process_cache()
    state = {"research_id": "res_test", "executed_queries_set": {"q"}, "sources": {}, "value": 7}
    storage.persist(state)
    storage.clear_process_cache()
    loaded = storage.load_state_raw("res_test")
    assert loaded["value"] == 7
    assert loaded["executed_queries_set"] == {"q"}
