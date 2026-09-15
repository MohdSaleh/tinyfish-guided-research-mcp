"""Deterministic release evaluator for the research integrity gates."""
from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

from tinyfish_research_mcp import storage
from tinyfish_research_mcp.core import (
    _active_claims,
    _attach_source_to_work,
    _quote_match,
    assess_one_claim,
    verify_citations,
)


def _base_source(source_id: str, doi: str, domain: str, quote: str) -> dict:
    return {
        "source_id": source_id,
        "doi": doi,
        "title": f"Paper {source_id}",
        "authors": ["A Researcher"],
        "registered_domain": domain,
        "host_domain": domain,
        "screen_status": "ACCEPTED",
        "screen_verdict": "RELEVANT",
        "authority_score": 0.95,
        "mechanical_quality_score": 0.95,
        "content_origin": "fetch",
        "fetch_failed": False,
        "content": f"Introduction. {quote} Conclusion.",
        "retraction_check": {},
        "author_track_record": {},
    }


def _claim(claim_id: str = "claim_1") -> dict:
    return {
        "claim_id": claim_id,
        "client_ref": "c1",
        "text": "Independent evidence supports the evaluated research claim.",
        "temporal_mode": "TIMELESS",
        "atomicity_status": "RESOLVED",
        "disconfirmation_attempted": False,
    }


def evaluate_duplicate_work_mirrors() -> bool:
    state = {"works": {}, "work_alias_map": {}, "sources": {}}
    a = {"source_id": "a", "doi": "10.1000/test", "title": "Paper", "authors": ["A"]}
    b = {"source_id": "b", "url": "https://doi.org/10.1000/test", "title": "Paper", "authors": ["A"]}
    return _attach_source_to_work(state, a) == _attach_source_to_work(state, b) and len(state["works"]) == 1


def evaluate_quote_mismatch() -> bool:
    ok, _ = _quote_match("alpha beta gamma", "different quote")
    return not ok


def evaluate_weak_single_source() -> bool:
    quote = "The experiment provides support for the evaluated research claim."
    state = {
        "contested": False,
        "works": {},
        "work_alias_map": {},
        "sources": {},
        "evidence": [],
    }
    source = _base_source("s1", "10.1000/one", "one.example", quote)
    state["sources"]["s1"] = source
    _attach_source_to_work(state, source)
    state["evidence"].append({
        "evidence_id": "e1", "claim_id": "claim_1", "source_id": "s1",
        "quote": quote, "quote_verified": True, "relation": "SUPPORTS",
        "strength": 0.95, "source_relevance": 1.0,
    })
    assessment = assess_one_claim(state, _claim())
    return assessment.status == "PROVISIONAL_SUPPORTED" and "insufficient_independent_works" in assessment.metrics["quality_flags"]


def evaluate_superseded_claim() -> bool:
    state = {
        "claims": [_claim("old"), _claim("new")],
        "superseded_claim_ids": ["old"],
    }
    return [c["claim_id"] for c in _active_claims(state)] == ["new"]


async def evaluate_citation_coverage() -> bool:
    q1 = "Study one directly supports the evaluated research claim."
    q2 = "Study two independently supports the evaluated research claim."
    with tempfile.TemporaryDirectory() as td:
        storage.DB_PATH = str(Path(td) / "eval.db")
        storage.clear_process_cache()
        state = {
            "research_id": "res_eval",
            "topic": "evaluation",
            "phase": "CLAIM_GRAPH_STABLE",
            "contested": False,
            "started_at": 0.0,
            "policy": {"max_gap_rounds": 0, "max_followup_rounds": 0},
            "gap_rounds": 0,
            "followup_rounds": 0,
            "executed_queries_set": set(),
            "claims": [_claim()],
            "superseded_claim_ids": [],
            "sources": {},
            "works": {},
            "work_alias_map": {},
            "evidence": [],
            "verified_citations": [],
            "metrics": {},
            "claim_tensions": [],
            "tension_review_done": False,
        }
        for sid, doi, domain, quote in [
            ("s1", "10.1000/a", "a.example", q1),
            ("s2", "10.1000/b", "b.example", q2),
        ]:
            source = _base_source(sid, doi, domain, quote)
            state["sources"][sid] = source
            _attach_source_to_work(state, source)
            storage.store_source_content(state["research_id"], sid, source["content"], "hash-" + sid)
            state["evidence"].append({
                "evidence_id": "e-" + sid,
                "claim_id": "claim_1",
                "source_id": sid,
                "quote": quote,
                "quote_verified": True,
                "relation": "SUPPORTS",
                "strength": 0.95,
                "source_relevance": 1.0,
            })
        storage.persist(state)
        result = await verify_citations("res_eval")
        return (
            result.status == "ALL_CITATIONS_VERIFIED_AND_COVERED"
            and result.quality_gate is not None
            and result.quality_gate.get("citation_coverage") == "PASS"
            and len(result.data.get("verified_citations", [])) == 2
        )


async def main_async() -> int:
    cases = json.loads((Path(__file__).parent / "cases.json").read_text())["cases"]
    results = {
        "duplicate_work_mirrors": evaluate_duplicate_work_mirrors(),
        "quote_mismatch": evaluate_quote_mismatch(),
        "weak_single_source": evaluate_weak_single_source(),
        "superseded_claim": evaluate_superseded_claim(),
        "citation_coverage": await evaluate_citation_coverage(),
    }
    expected = {case["id"] for case in cases}
    missing = sorted(expected - results.keys())
    failures = sorted(key for key, passed in results.items() if not passed)
    print(json.dumps({"results": results, "missing": missing, "failures": failures}, indent=2))
    return 1 if failures or missing else 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
