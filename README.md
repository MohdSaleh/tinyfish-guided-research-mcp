# TinyFish Guided Research MCP

<!-- mcp-name: io.github.MohdSaleh/tinyfish-guided-research -->

A simple research workflow for AI agents using TinyFish Search and Fetch.

TinyFish provides free web search and page fetching APIs. They are useful on their own, but getting consistently good research results can be difficult — especially when the agent is powered by a small or medium-sized model.

The main problem usually isn't search itself.

It's deciding:

- what to search for
- which results are worth opening
- what information matters
- when more research is needed
- which sources actually support a claim
- when the research is good enough to stop

This MCP adds a structured research workflow on top of TinyFish so the AI model doesn't have to figure out that entire process by itself.

## Why I built this

TinyFish offers Search and Fetch APIs that can be used freely, while its more advanced research services are paid.

I wanted to see how far the free APIs could go with a better workflow around them.

Instead of asking the AI model to manage the whole research process, this MCP handles the repeatable parts for it.

The model still reads, reasons, and makes decisions.

The MCP handles the workflow around those decisions.

The result is a more reliable way for agents — especially smaller models — to search the web, collect useful information, and build answers from real sources.

## How it works

The basic flow looks like this:

```text
Question
   ↓
Plan what needs to be researched
   ↓
Search with TinyFish
   ↓
Filter weak or duplicate results
   ↓
Fetch useful pages
   ↓
Extract evidence
   ↓
Check whether the evidence supports the claim
   ↓
Search again if something is missing
   ↓
Verify citations
   ↓
Finish
```

There is **no LLM running inside the MCP server**.

Your client model does the language reasoning.

The MCP manages the research process, keeps track of the state, and makes sure important steps are not skipped.

## What it helps with

- Breaking a research task into smaller parts
- Running focused TinyFish searches
- Filtering weak and duplicate sources
- Fetching the most useful pages
- Keeping research state between steps
- Connecting evidence to claims
- Finding gaps that need more research
- Checking quotes against fetched source content
- Tracking conflicting evidence
- Verifying citations before the research is finished
- Preventing weak evidence from being treated as strong proof

The goal is not to make the model smarter.

The goal is to give it a better process.

## Requirements

- Python 3.11+
- A TinyFish API key
- `uv` recommended
- PostgreSQL for remote or multi-instance deployments

SQLite works fine for local development.

## Installation

Clone the repository:

```bash
git clone https://github.com/MohdSaleh/tinyfish-guided-research-mcp.git
cd tinyfish-guided-research-mcp
```

Install the dependencies:

```bash
uv sync --all-extras
```

Set your TinyFish API key:

```bash
export TINYFISH_API_KEY="your-api-key"
```

Then start the MCP server:

```bash
uv run tinyfish-research-mcp
```

## Test it with MCP Inspector

You can inspect the available tools using the official MCP Inspector:

```bash
npx @modelcontextprotocol/inspector \
  --cli uv run tinyfish-research-mcp \
  --method tools/list
```

## Storage

For local development, the MCP uses SQLite.

```bash
export RESEARCH_DB_PATH=research_state.db
```

For a hosted deployment, use PostgreSQL:

```bash
export DATABASE_URL="postgresql://user:password@host:5432/database?sslmode=require"
```

PostgreSQL is recommended when more than one server instance may be running at the same time.

## Project structure

```text
src/tinyfish_research_mcp/

  server.py
  MCP server and tool definitions

  core.py
  Research workflow and quality checks

  providers.py
  TinyFish and external data providers

  storage.py
  Research state and source storage

  models.py
  Tool input/output models

  config.py
  Configuration

  observability.py
  Logging and tracing
```

There are also two important directories:

```text
tests/
```

Tests the MCP implementation.

```text
evals/
```

Tests research-quality behavior such as citation coverage, duplicate sources, weak evidence, and quote verification.

## Development

Run the main checks with:

```bash
uv run ruff check .
uv run pyright
uv run pytest
uv run python evals/run_evals.py
```

Security check:

```bash
uv run pip-audit
```

Build the package:

```bash
uv build
```

## Design idea

This project follows one simple rule:

> Let the model do the reasoning. Let the MCP manage the research process.

Smaller models can often understand a source perfectly well once the right information is in front of them.

What they struggle with more is managing a long research process consistently.

This MCP tries to solve that part.

TinyFish handles search and page fetching.

The AI model handles understanding and reasoning.

The MCP sits between them and keeps the research moving through a predictable workflow.

## Status

The project is still evolving.

The current focus is improving:

- research quality
- source selection
- citation accuracy
- smaller-model performance
- search efficiency
- fewer unnecessary tool calls

Feedback, issues, and experiments are welcome.

## License

MIT
