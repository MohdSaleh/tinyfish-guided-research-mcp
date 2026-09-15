# TinyFish Guided Research MCP

<!-- mcp-name: io.github.MohdSaleh/tinyfish-guided-research -->

A latency-conscious, auditable deep-research MCP server built on TinyFish Search + Fetch.
The server contains **no LLM**. The calling client performs bounded semantic judgments while
the server owns retrieval, persistence, evidence integrity, work-level deduplication, quality gates,
citation auditing and protocol state.

## Requirements

- Python 3.11+
- TinyFish API key
- `uv` recommended
- PostgreSQL for shared production deployments; SQLite is supported for local/single-instance use

## Install for development

```bash
uv sync --all-extras
cp .env.example .env
export TINYFISH_API_KEY="..."
```

Run over stdio:

```bash
uv run tinyfish-research-mcp
```

Inspect the tool contract:

```bash
npx @modelcontextprotocol/inspector --cli uv run tinyfish-research-mcp --method tools/list
```

## Persistence

Local development defaults to SQLite:

```bash
export RESEARCH_DB_PATH=research_state.db
```

For remote hosting, autoscaling or multiple replicas, use shared PostgreSQL:

```bash
export DATABASE_URL='postgresql://user:password@host:5432/database?sslmode=require'
```

The storage layer reloads state from durable storage for every tool call and uses optimistic
version checks. Concurrent workers therefore cannot silently overwrite the same research session;
a stale writer fails and can be retried by the client.

## Production structure

```text
src/tinyfish_research_mcp/
  server.py          MCP adapter and entrypoint
  config.py          environment and quality thresholds
  models.py          Pydantic tool schemas
  core.py            research protocol and scoring engine
  providers.py       TinyFish / Crossref / OpenAlex / HTTP reliability
  storage.py         SQLite/PostgreSQL durable state + source-content persistence
  observability.py   structured stderr logging + optional OpenTelemetry helpers
```

`tests/` protects implementation and MCP contracts. `evals/` protects research quality.

## Production checks

```bash
uv run ruff check .
uv run pyright
uv run pytest
uv run python evals/run_evals.py
uv run pip-audit
uv build
```

## Observability

The MCP Python SDK v2 emits protocol OpenTelemetry spans. Install the optional observability extra
when exporting traces through OTLP:

```bash
uv sync --extra observability
```

Application logs are structured JSON on **stderr**, preserving stdout for stdio MCP traffic.
