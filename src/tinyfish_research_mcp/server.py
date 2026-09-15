"""MCP protocol adapter and process entrypoint."""
from __future__ import annotations

from mcp.server import MCPServer

from . import core
from .observability import configure_observability

SERVER_INSTRUCTIONS = """
TinyFish Guided Research MCP v7.2 - work-aware latency-first auditable protocol

The server contains NO LLM. The client performs bounded semantic judgments;
the MCP owns retrieval, state, IDs, budgets, quote integrity, confidence,
phase transitions, citation coverage, and final synthesis constraints.

Mandatory rules:
1. Never invent server IDs and never inspect MCP source/database to infer protocol state. Follow next_action.
2. Retrieval returns candidates, not evidence.
3. Atomicity warnings are hard gates.
4. Quote existence is server-verified; semantic entailment is a bounded client judgment.
5. Strong resolution requires independent underlying works, authority, full-fetch evidence and quality thresholds.
6. Different hosts/copies of the same paper never count as independent studies.
7. Contested sessions require an explicit disconfirmation attempt before strong resolution.
8. Stop researching claims already returned as SUPPORTED or CONTRADICTED.
9. Citation verification is server-owned and work-independent.
10. Final synthesis must obey synthesis_manifest and distinguish DIRECTLY_SUPPORTED,
    DERIVED_INFERENCE, and SPECULATIVE_RECOMMENDATION.
"""

mcp = MCPServer("tinyfish-guided-research", instructions=SERVER_INSTRUCTIONS)

_TOOL_FUNCTIONS = (
    core.check_server_config,
    core.init_research,
    core.plan_research,
    core.dispatch_parallel_subagents,
    core.discovery_search,
    core.screen_sources,
    core.register_claims,
    core.split_claim,
    core.override_atomicity,
    core.revise_claim,
    core.bind_evidence,
    core.judge_evidence,
    core.assess_claims,
    core.research_unknowns,
    core.review_candidates,
    core.get_source_context,
    core.review_claim_tensions,
    core.verify_citations,
    core.finalize_research,
    core.get_research_state,
)
for _tool in _TOOL_FUNCTIONS:
    mcp.tool()(_tool)

def main() -> None:
    configure_observability()
    mcp.run()

if __name__ == "__main__":
    main()
