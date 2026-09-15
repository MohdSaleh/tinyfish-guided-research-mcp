# TinyFish Guided Research MCP

<!-- mcp-name: io.github.MohdSaleh/tinyfish-guided-research -->

An MCP server that adds a simple research workflow on top of TinyFish Search and Fetch.

The client model does the reasoning. The server keeps track of the research run, handles the search and fetch flow, stores the state, checks evidence and citations, and tells the client what should happen next.

There is no LLM running inside the server.

## How it works

A research run usually follows this flow:

```text
Research request
      ↓
Plan
      ↓
Search
      ↓
Review sources
      ↓
Fetch useful content
      ↓
Track claims and evidence
      ↓
Check what is supported or still missing
      ↓
Research the gaps
      ↓
Verify citations
      ↓
Finalize
```

Search results are treated as candidates first, not as evidence by default.

The server also keeps duplicate sources from being counted more than once. This includes cases where the same paper or source appears through different URLs or mirrors.

Quotes are checked against the fetched source content, while semantic decisions such as whether a passage actually supports a claim are left to the client model.

Conflicting evidence is kept in the research state instead of being ignored, and citations are checked before the research is finalized.

## What it handles

- Research state across multiple steps
- TinyFish Search and Fetch calls
- Source screening and duplicate handling
- Claim and evidence tracking
- Quote checks against fetched content
- Conflicting evidence
- Research gaps and follow-up searches
- Citation verification
- Research budgets and stopping conditions
- SQLite and PostgreSQL persistence

## Quick start

### Hosted

The hosted MCP endpoint is:

```text
https://tinyfish-guided-research-mcp.fastmcp.app/mcp
```

Use it as a Streamable HTTP MCP server.

### Local

Requires Python 3.11+ and a TinyFish API key.

```bash
TINYFISH_API_KEY="your-api-key" uvx tinyfish-guided-research-mcp
```

Example MCP client config:

```json
{
  "mcpServers": {
    "tinyfish-research": {
      "command": "uvx",
      "args": ["tinyfish-guided-research-mcp"],
      "env": {
        "TINYFISH_API_KEY": "your-api-key"
      }
    }
  }
}
```

The compatibility entrypoint is also available:

```bash
uvx --from tinyfish-guided-research-mcp tinyfish-research-mcp
```

## Storage

SQLite is fine for local or single-instance use:

```bash
export RESEARCH_DB_PATH="research_state.db"
```

For hosted or multi-instance deployments, use PostgreSQL:

```bash
export DATABASE_URL="postgresql://user:password@host:5432/database?sslmode=require"
```

PostgreSQL is the better option when more than one server instance can access the same research state.

## Distribution

The server is available through PyPI, the official MCP Registry, and the hosted Horizon endpoint.

PyPI:

```text
tinyfish-guided-research-mcp
```

MCP Registry:

```text
io.github.MohdSaleh/tinyfish-guided-research
```

Hosted MCP:

```text
https://tinyfish-guided-research-mcp.fastmcp.app/mcp
```

## Development

Clone the repo and install the dependencies:

```bash
git clone https://github.com/MohdSaleh/tinyfish-guided-research-mcp.git
cd tinyfish-guided-research-mcp
uv sync --all-extras
```

Run it locally:

```bash
TINYFISH_API_KEY="your-api-key" uv run tinyfish-guided-research-mcp
```

Run the checks:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
uv run python evals/run_evals.py
uv run pip-audit
uv build
```

The regular tests cover the implementation and storage layer. The research evals cover cases such as duplicate sources, weak evidence, quote mismatches, superseded claims, and citation coverage.

You can also inspect the MCP tools with:

```bash
npx @modelcontextprotocol/inspector \
  --cli uv run tinyfish-guided-research-mcp \
  --method tools/list
```

## Deploying on Horizon

If you want to deploy your own instance with Prefect Horizon, use:

```text
Entrypoint:
src/tinyfish_research_mcp/server.py:mcp

Dependencies:
pyproject.toml
```

Set:

```text
TINYFISH_API_KEY
DATABASE_URL
```

Use PostgreSQL for hosted deployments instead of the local SQLite fallback.

## License

MIT
