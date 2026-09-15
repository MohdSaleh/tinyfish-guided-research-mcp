import pytest

mcp_module = pytest.importorskip("mcp")
from mcp import Client
from tinyfish_research_mcp.server import mcp


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_tools_are_exposed():
    async with Client(mcp, raise_exceptions=True) as client:
        result = await client.list_tools()
        names = {tool.name for tool in result.tools}
        assert "init_research" in names
        assert "review_candidates" in names
        assert "verify_citations" in names
        assert "finalize_research" in names
