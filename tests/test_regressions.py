from tinyfish_research_mcp.core import _quote_match


def test_quote_integrity_normalizes_whitespace():
    content = "Process supervision makes   credit assignment easier."
    ok, match_type = _quote_match(content, "Process supervision makes credit assignment easier.")
    assert ok is True
    assert match_type == "exact_normalized"
