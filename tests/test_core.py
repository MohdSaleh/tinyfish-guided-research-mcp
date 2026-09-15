from tinyfish_research_mcp.core import _attach_source_to_work


def test_same_doi_collapses_to_same_work():
    state = {"works": {}, "work_alias_map": {}, "sources": {}}
    a = {"source_id": "s1", "doi": "10.1234/example.1", "title": "A Paper", "authors": ["A Author"]}
    b = {"source_id": "s2", "url": "https://doi.org/10.1234/example.1", "title": "A Paper", "authors": ["A Author"]}
    assert _attach_source_to_work(state, a) == _attach_source_to_work(state, b)
    assert len(state["works"]) == 1
