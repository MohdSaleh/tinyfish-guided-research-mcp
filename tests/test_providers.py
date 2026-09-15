from tinyfish_research_mcp.providers import _canonicalize_url, _registrable_domain


def test_canonicalize_url_removes_tracking_and_fragment():
    url = "https://Example.com/paper?a=1&utm_source=x#section"
    assert _canonicalize_url(url) == "https://example.com/paper?a=1"


def test_registrable_domain():
    assert _registrable_domain("www.example.com") == "example.com"
