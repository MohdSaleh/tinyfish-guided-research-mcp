"""Research protocol engine.

This module intentionally contains the cohesive research state machine and scoring
logic. It has no MCP SDK dependency; server.py is the protocol adapter.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import math
import re
import socket
import time
import unicodedata
import uuid
from collections import defaultdict
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Optional
from urllib.parse import urlsplit

from .config import (
    CONTESTED_MARGIN_THRESHOLD,
    CURRENT_EVIDENCE_HALF_LIFE_DAYS,
    ENABLE_CROSSREF_CHECKS,
    ENABLE_OPENALEX_CHECKS,
    FETCH_TOP_N_PER_QUERY,
    GENERIC_HOME_SIGNALS,
    MAX_ATOMICITY_SPLIT_DEPTH,
    MAX_CANDIDATE_CONTEXTS,
    MAX_FETCH_PER_RETRIEVAL,
    MIN_INDEPENDENT_WORKS,
    MAX_INFLIGHT_FETCH,
    MAX_INFLIGHT_SEARCH,
    MAX_PARALLEL_SUBAGENTS,
    MIN_AUTHORITY_FOR_STRONG_RESOLUTION,
    MIN_CITATION_WORKS_PER_CLAIM,
    MIN_DISSENTING_WORKS_FOR_OVERRIDE,
    MIN_EVIDENCE_QUALITY,
    MIN_QUOTE_CHARS,
    MIN_STANCE_CONFIDENCE,
    MODE_POLICIES,
    PROTOCOL_VERSION,
    ResearchMode,
    RRF_K,
    SEARCH_INTERMEDIARY_PATTERNS,
    SOCIAL_DOMAINS,
    SOURCE_MECHANICAL_ACCEPT_THRESHOLD,
    STOPWORDS,
    TINYFISH_API_KEY,
    TINYFISH_FETCH_URL,
    TINYFISH_SEARCH_URL,
)
from .models import (
    CandidateEvidenceReview,
    CitationCheck,
    ClaimAssessment,
    ClaimDraft,
    ClaimTension,
    EvidenceBinding,
    EvidenceRelationJudgment,
    GapPlan,
    NextAction,
    ResearchPlan,
    ResearchToolResponse,
    RetrievalSpec,
    SourceScreening,
    SubagentTask,
    TemporalMode,
)
from .providers import (
    _canonicalize_url,
    _check_author,
    _check_retraction,
    _do_search,
    _domain_of,
    _fetch_many,
    _registrable_domain,
    get_http_client,
)
from .storage import (
    get_source_content as _get_source_content,
    load_source_content as _load_source_content,
    load_state_raw,
    persist,
    store_source_content as _store_source_content,
)

_WORK_DOI_RE = re.compile(r"10\.\d{4,9}/[^\s&?#\"\']+", re.I)
_ARXIV_RE = re.compile(r"(?:arxiv(?:\.org)?(?:/abs/|/pdf/|:)?\s*)(\d{4}\.\d{4,5})(?:v\d+)?", re.I)
_WORK_ALIAS_PRIORITY = {
    "doi": 0,
    "arxiv": 1,
    "openalex": 2,
    "title_authors": 3,
    "title": 4,
    "source_fallback": 5,
}
PHASES = {
    "INITIALIZED",
    "PLANNED",
    "RETRIEVING",
    "SOURCE_SCREENING",
    "CLAIM_REGISTRATION",
    "ATOMICITY_REVIEW",
    "EVIDENCE_BINDING",
    "ASSESSED",
    "GAP_RESEARCH",
    "CANDIDATE_REVIEW",
    "CLAIM_GRAPH_STABLE",
    "TENSION_REVIEWED",
    "CITATION_AUDIT",
    "READY_TO_FINALIZE",
    "COMPLETE",
}


def _mode_policy(state: dict[str, Any]) -> dict[str, int]:
    mode = state.get("mode", ResearchMode.FAST.value)
    return MODE_POLICIES.get(mode, MODE_POLICIES[ResearchMode.FAST.value])


def load_state(research_id: str) -> dict[str, Any]:
    state = load_state_raw(research_id)
    _ensure_work_lineage(state)
    return state


def _is_root_url(url: str) -> bool:
    try:
        p = urlsplit(url)
        return (p.path or "/") in ("", "/") and not p.query
    except Exception:
        return False


def _keyword_set(text: str) -> set[str]:
    return {
        w.lower()
        for w in re.findall(r"[A-Za-z0-9][A-Za-z0-9_\-]+", text or "")
        if len(w) > 2 and w.lower() not in STOPWORDS
    }


def _token_overlap_score(reference: str, candidate: str) -> float:
    ref = _keyword_set(reference)
    cand = _keyword_set(candidate)
    if not ref or not cand:
        return 0.0
    hits = len(ref & cand)
    return min(1.0, hits / max(1, min(10, len(ref))))


def _parse_date_to_ts(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        pass
    try:
        return parsedate_to_datetime(s).timestamp()
    except Exception:
        return None


def _content_hash(text: str) -> str:
    norm = re.sub(r"\s+", " ", text or "").strip().lower()
    return hashlib.sha256(norm.encode("utf-8", errors="ignore")).hexdigest()


def _normalize_title_key(title: Optional[str]) -> str:
    text = unicodedata.normalize("NFKC", title or "").lower()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_author_key(authors: list[str]) -> str:
    cleaned = []
    for author in authors[:3]:
        a = unicodedata.normalize("NFKC", author or "").lower()
        a = re.sub(r"[^a-z0-9]+", " ", a)
        a = re.sub(r"\s+", " ", a).strip()
        if a:
            cleaned.append(a)
    return "|".join(cleaned)


def _extract_doi_from_source(source: dict[str, Any]) -> Optional[str]:
    fields = [
        source.get("doi"),
        source.get("url"),
        source.get("fetch_url"),
        source.get("final_url"),
        source.get("pdf_url"),
        source.get("description"),
        source.get("title"),
        (source.get("content") or "")[:5000],
    ]
    for field in fields:
        if not field:
            continue
        m = _WORK_DOI_RE.search(str(field))
        if m:
            return m.group(0).rstrip(".,);]}>").lower()
    return None


def _extract_arxiv_id_from_source(source: dict[str, Any]) -> Optional[str]:
    fields = [
        source.get("arxiv_id"),
        source.get("url"),
        source.get("fetch_url"),
        source.get("final_url"),
        source.get("pdf_url"),
        source.get("description"),
        source.get("title"),
        (source.get("content") or "")[:6000],
    ]
    for field in fields:
        if not field:
            continue
        m = _ARXIV_RE.search(str(field))
        if m:
            return m.group(1).lower()
    return None


def _source_role(source: dict[str, Any]) -> str:
    domain = source.get("registered_domain") or _registrable_domain(source.get("domain", ""))
    dtype = source.get("domain_type", "web")
    if domain == "arxiv.org":
        return "preprint_repository"
    if domain == "openreview.net" or "openreview" in domain:
        return "conference_repository"
    if domain == "github.com":
        return "code_or_project_repository"
    if domain == "alphaxiv.org":
        return "paper_mirror"
    if domain in SOCIAL_DOMAINS:
        return "social_discussion"
    if dtype == "news":
        return "news_report"
    if dtype == "research_paper":
        if source.get("publisher") or source.get("venue"):
            return "publisher_or_venue_copy"
        return "scholarly_copy"
    if source.get("publisher"):
        return "publisher_web_page"
    return "web_source"


def _publication_status(source: dict[str, Any]) -> str:
    role = source.get("source_role") or _source_role(source)
    if role == "preprint_repository":
        return "PREPRINT_OR_MANUSCRIPT"
    if source.get("venue") and role in {"conference_repository", "publisher_or_venue_copy"}:
        return "VENUE_LISTED_PEER_REVIEW_NOT_SERVER_VERIFIED"
    if role in {"publisher_or_venue_copy", "publisher_web_page"}:
        return "PUBLISHED_SOURCE_PEER_REVIEW_NOT_SERVER_VERIFIED"
    return "UNKNOWN"


def _work_aliases(source: dict[str, Any]) -> list[tuple[str, str]]:
    """Return strongest-first aliases for the underlying intellectual work.

    Multiple hosts/copies of one paper intentionally share aliases. Exact title
    aliases are conservative but useful when DOI/arXiv metadata is absent.
    """
    aliases: list[tuple[str, str]] = []
    doi = _extract_doi_from_source(source)
    if doi:
        aliases.append(("doi", doi))
    arxiv_id = _extract_arxiv_id_from_source(source)
    if arxiv_id:
        aliases.append(("arxiv", arxiv_id))
    openalex_id = str(source.get("openalex_id") or "").strip()
    if openalex_id:
        aliases.append(("openalex", openalex_id.lower().rsplit("/", 1)[-1]))
    title = _normalize_title_key(source.get("title"))
    authors = _normalize_author_key(
        source.get("authors") or ([source.get("author")] if source.get("author") else [])
    )
    if len(title) >= 24 and len(title.split()) >= 4:
        if authors:
            aliases.append(("title_authors", f"{title}|{authors}"))
        aliases.append(("title", title))
    return aliases


def _work_id_from_alias(kind: str, value: str) -> str:
    digest = hashlib.sha256(f"{kind}:{value}".encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"work_{digest}"


def _work_identity_confidence(alias_kind: str) -> float:
    return {
        "doi": 1.0,
        "arxiv": 1.0,
        "openalex": 1.0,
        "title_authors": 0.90,
        "title": 0.75,
        "source_fallback": 0.35,
    }.get(alias_kind, 0.35)


def _refresh_work_identity_metadata(work: dict[str, Any]) -> None:
    parsed = []
    for alias in work.get("aliases", []):
        if ":" not in alias:
            continue
        kind, value = alias.split(":", 1)
        parsed.append((_WORK_ALIAS_PRIORITY.get(kind, 99), kind, value))
    if parsed:
        _, kind, value = min(parsed)
        work["canonical_identity_type"] = kind
        work["canonical_identity_value"] = value
        work["identity_confidence"] = max(
            float(work.get("identity_confidence", 0.0)), _work_identity_confidence(kind)
        )


def _merge_work_records(state: dict[str, Any], primary_id: str, other_id: str) -> None:
    if primary_id == other_id:
        return
    works = state.setdefault("works", {})
    primary = works.setdefault(primary_id, {"work_id": primary_id, "source_ids": [], "aliases": []})
    other = works.pop(other_id, None)
    if not other:
        return
    primary["source_ids"] = sorted(set(primary.get("source_ids", [])) | set(other.get("source_ids", [])))
    primary["aliases"] = sorted(set(primary.get("aliases", [])) | set(other.get("aliases", [])))
    _refresh_work_identity_metadata(primary)
    primary["host_domains"] = sorted(
        set(primary.get("host_domains", [])) | set(other.get("host_domains", []))
    )
    primary["source_roles"] = sorted(
        set(primary.get("source_roles", [])) | set(other.get("source_roles", []))
    )
    for key in ("title", "venue", "year"):
        if not primary.get(key) and other.get(key):
            primary[key] = other[key]
    if not primary.get("authors") and other.get("authors"):
        primary["authors"] = other["authors"]
    primary["identity_confidence"] = max(
        float(primary.get("identity_confidence", 0.0)), float(other.get("identity_confidence", 0.0))
    )
    alias_index = state.setdefault("work_alias_index", {})
    for alias, wid in list(alias_index.items()):
        if wid == other_id:
            alias_index[alias] = primary_id
    for source in state.get("sources", {}).values():
        if source.get("work_id") == other_id:
            source["work_id"] = primary_id
            source["evidence_family_id"] = primary_id
            source["work_identity_confidence"] = float(
                primary.get("identity_confidence", source.get("work_identity_confidence", 0.35))
            )
    state.setdefault("metrics", {})["work_dedup_merges"] = (
        int(state.setdefault("metrics", {}).get("work_dedup_merges", 0)) + 1
    )


def _attach_source_to_work(state: dict[str, Any], source: dict[str, Any]) -> str:
    aliases = _work_aliases(source)
    alias_index = state.setdefault("work_alias_index", {})
    works = state.setdefault("works", {})
    existing = []
    for kind, value in aliases:
        wid = alias_index.get(f"{kind}:{value}")
        if wid and wid not in existing:
            existing.append(wid)

    if existing:
        work_id = existing[0]
        for other in existing[1:]:
            _merge_work_records(state, work_id, other)
    elif aliases:
        work_id = _work_id_from_alias(*aliases[0])
    else:
        fallback = source.get("content_hash") or source.get("canonical_url") or source.get("source_id")
        work_id = _work_id_from_alias("source_fallback", str(fallback))

    strongest_kind = aliases[0][0] if aliases else "source_fallback"
    identity_confidence = _work_identity_confidence(strongest_kind)
    work = works.setdefault(
        work_id,
        {
            "work_id": work_id,
            "source_ids": [],
            "aliases": [],
            "host_domains": [],
            "source_roles": [],
            "title": source.get("title"),
            "authors": source.get("authors") or [],
            "venue": source.get("venue"),
            "year": source.get("year"),
            "identity_confidence": identity_confidence,
        },
    )
    work["identity_confidence"] = max(float(work.get("identity_confidence", 0.0)), identity_confidence)
    alias_strings = [f"{k}:{v}" for k, v in aliases]
    work["aliases"] = sorted(set(work.get("aliases", [])) | set(alias_strings))
    _refresh_work_identity_metadata(work)
    work["source_ids"] = sorted(set(work.get("source_ids", [])) | {source["source_id"]})
    host = source.get("registered_domain") or source.get("domain")
    if host:
        work["host_domains"] = sorted(set(work.get("host_domains", [])) | {host})
    role = source.get("source_role") or _source_role(source)
    work["source_roles"] = sorted(set(work.get("source_roles", [])) | {role})
    for key in ("title", "venue", "year"):
        if not work.get(key) and source.get(key):
            work[key] = source.get(key)
    if not work.get("authors") and source.get("authors"):
        work["authors"] = source.get("authors")
    for alias in alias_strings:
        previous = alias_index.get(alias)
        if previous and previous != work_id:
            _merge_work_records(state, work_id, previous)
        alias_index[alias] = work_id

    source["work_id"] = work_id
    source["evidence_family_id"] = work_id
    source["work_identity_types"] = [k for k, _ in aliases] or ["source_fallback"]
    # Propagate the strongest known family identity to every hosting copy.
    family_confidence = float(work.get("identity_confidence", identity_confidence))
    family_type = work.get("canonical_identity_type", strongest_kind)
    for sid in work.get("source_ids", []):
        member = state.get("sources", {}).get(sid)
        if member:
            member["work_id"] = work_id
            member["evidence_family_id"] = work_id
            member["work_identity_confidence"] = family_confidence
            member["work_identity_type"] = family_type
    source["work_identity_confidence"] = family_confidence
    source["work_identity_type"] = family_type
    return work_id


def _work_id_for_source(source: dict[str, Any]) -> str:
    return source.get("work_id") or f"source:{source.get('source_id', 'unknown')}"


def _ensure_work_lineage(state: dict[str, Any]) -> None:
    """Best-effort migration for v7.1 persisted sessions loaded under v7.2."""
    state.setdefault("works", {})
    state.setdefault("work_alias_index", {})
    state.setdefault("metrics", {}).setdefault("work_dedup_merges", 0)
    needs_migration = any(not src.get("work_id") for src in state.get("sources", {}).values())
    if not needs_migration:
        return
    for source in state.get("sources", {}).values():
        if source.get("work_id"):
            continue
        source["host_domain"] = (
            source.get("host_domain") or source.get("registered_domain") or source.get("domain")
        )
        source["source_role"] = source.get("source_role") or _source_role(source)
        source["publication_status"] = source.get("publication_status") or _publication_status(source)
        source["publication_title"] = source.get("publication_title") or source.get("title")
        source["publication_venue"] = source.get("publication_venue") or source.get("venue")
        if not source.get("content"):
            source["content"] = _load_source_content(state["research_id"], source["source_id"])
        source["doi"] = source.get("doi") or _extract_doi_from_source(source)
        source["arxiv_id"] = source.get("arxiv_id") or _extract_arxiv_id_from_source(source)
        _attach_source_to_work(state, source)


def _normalize_quote(text: str) -> str:
    s = html.unescape(unicodedata.normalize("NFKC", text or ""))
    trans = str.maketrans(
        {
            "“": '"',
            "”": '"',
            "„": '"',
            "’": "'",
            "‘": "'",
            "—": "-",
            "–": "-",
            "−": "-",
            " ": " ",
        }
    )
    s = s.translate(trans)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _quote_match(content: str, quote: str) -> tuple[bool, str]:
    c = _normalize_quote(content)
    q = _normalize_quote(quote)
    if len(q) < MIN_QUOTE_CHARS:
        return False, "quote_too_short"
    if q in c:
        return True, "exact_normalized"
    # Permit explicit ellipsis only when all substantial segments appear in
    # order; this is still deterministic and prevents client self-certifying.
    if "..." in q or "…" in quote:
        parts = [p.strip() for p in re.split(r"(?:\.\.\.|…)", q) if len(p.strip()) >= 8]
        if len(parts) >= 2:
            pos = 0
            for part in parts:
                idx = c.find(part, pos)
                if idx < 0:
                    return False, "segment_not_found"
                pos = idx + len(part)
            return True, "ordered_ellipsis_segments"
    return False, "quote_not_found"


def _paragraphs(text: str) -> list[str]:
    blocks = [re.sub(r"\s+", " ", p).strip() for p in re.split(r"\n\s*\n+", text or "")]
    return [p for p in blocks if len(p) >= 40]


def _top_passages(text: str, objective: str, limit: int = 3, max_chars: int = 900) -> list[str]:
    blocks = _paragraphs(text)
    if not blocks and text:
        blocks = [re.sub(r"\s+", " ", text).strip()]
    ranked = sorted(
        blocks,
        key=lambda p: (_token_overlap_score(objective, p), min(len(p), 1200)),
        reverse=True,
    )
    return [p[:max_chars] for p in ranked[:limit]]


def _authority_score(source: dict[str, Any]) -> float:
    """Estimate evidentiary authority without confusing host with publication.

    A GitHub/project copy of a peer-reviewed paper remains a project/repository
    source. Venue metadata can raise confidence modestly, but host provenance is
    always retained separately and peer review is never inferred as a fact.
    """
    domain = source.get("registered_domain") or _registrable_domain(source.get("domain", ""))
    domain_type = source.get("domain_type", "web")
    role = source.get("source_role") or _source_role(source)

    role_base = {
        "conference_repository": 0.86,
        "publisher_or_venue_copy": 0.86,
        "scholarly_copy": 0.76,
        "preprint_repository": 0.72,
        "paper_mirror": 0.62,
        "publisher_web_page": 0.68,
        "code_or_project_repository": 0.50,
        "news_report": 0.68,
        "web_source": 0.52,
        "social_discussion": 0.24,
    }
    base = role_base.get(role, 0.52)

    if domain_type == "research_paper":
        base = max(base, 0.70)
    if source.get("venue"):
        base += 0.04
    if source.get("publisher"):
        base += 0.03
    if source.get("pdf_url"):
        base += 0.02
    cited = source.get("cited_by_count") or 0
    if isinstance(cited, (int, float)) and cited >= 20:
        base += 0.03
    if domain.endswith(".gov") or ".gov." in domain:
        base = max(base, 0.90)
    elif domain.endswith(".edu") or ".edu." in domain or domain.endswith(".ac.uk"):
        base = max(base, 0.82)
    if domain == "wikipedia.org" or domain.endswith(".wikipedia.org"):
        base = min(base, 0.55)
    if domain in SOCIAL_DOMAINS:
        base = min(base, 0.25)
    return round(min(1.0, max(0.0, base)), 4)


def _mechanical_source_gate(source: dict[str, Any]) -> tuple[bool, float, list[str]]:
    reasons: list[str] = []
    text = source.get("content", "") or ""
    final_url = source.get("final_url") or source.get("fetch_url") or source.get("url") or ""
    lower_url = final_url.lower()
    lower_text = text[:1200].lower()

    if any(p in lower_url for p in SEARCH_INTERMEDIARY_PATTERNS):
        return False, 0.0, ["search_intermediary_url"]
    if not text.strip():
        return False, 0.0, ["empty_content"]
    if len(text.strip()) < 80:
        reasons.append("very_short_content")
    if _is_root_url(final_url) and any(sig in lower_text for sig in GENERIC_HOME_SIGNALS):
        return False, 0.0, ["generic_homepage"]
    if source.get("fetch_failed") and len(text) < 120:
        return False, 0.0, ["fetch_failed_without_usable_snippet"]

    objective = source.get("retrieval_objective", "")
    query_text = " ".join(source.get("matched_queries", []))
    candidate_text = " ".join(filter(None, [source.get("title"), source.get("description"), text[:5000]]))
    relevance = max(
        _token_overlap_score(objective, candidate_text),
        _token_overlap_score(query_text, candidate_text),
    )
    if relevance < 0.08:
        reasons.append("low_lexical_relevance")

    full_fetch = 0.0 if source.get("fetch_failed") else 1.0
    length_factor = min(1.0, len(text) / 1800.0)
    authority = _authority_score(source)
    score = 0.38 * relevance + 0.22 * full_fetch + 0.18 * length_factor + 0.22 * authority

    if relevance < 0.025 and source.get("domain_type") != "research_paper":
        return False, round(score, 4), reasons + ["mechanically_off_topic"]
    return score >= SOURCE_MECHANICAL_ACCEPT_THRESHOLD, round(score, 4), reasons


async def _retrieve_candidates(
    queries: list[str],
    spec: RetrievalSpec,
    executed_queries_set: set[str],
    objective: str,
    *,
    fetch_top_n_per_query: Optional[int] = None,
    max_fetch_per_retrieval: Optional[int] = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Search concurrently, fuse rankings, fetch only the strongest candidates.

    The caller passes profile-specific retrieval limits. This keeps FAST mode
    genuinely fast without weakening downstream evidence-quality gates.
    """
    query_pairs: list[tuple[str, str]] = []
    for q in queries:
        q = q.strip()
        if not q:
            continue
        key = json.dumps({"q": q, "spec": spec.model_dump()}, sort_keys=True)
        if key in executed_queries_set:
            continue
        executed_queries_set.add(key)
        query_pairs.append((key, q))

    search_results = (
        await asyncio.gather(*(_do_search(q, spec) for _, q in query_pairs), return_exceptions=True)
        if query_pairs
        else []
    )

    per_query: dict[str, list[dict[str, Any]]] = {}
    search_requests = 0
    for (_, q), result in zip(query_pairs, search_results):
        search_requests += 1
        per_query[q] = [] if isinstance(result, Exception) else result

    aggregated: dict[str, dict[str, Any]] = {}
    for q, results in per_query.items():
        for r in results:
            fetch_url = r.get("pdf_url") or r.get("url")
            if not fetch_url:
                continue
            canonical = _canonicalize_url(fetch_url)
            rec = aggregated.setdefault(
                canonical,
                {
                    **r,
                    "fetch_url": fetch_url,
                    "landing_url": r.get("url"),
                    "matched_queries": [],
                    "rrf_score": 0.0,
                },
            )
            if q not in rec["matched_queries"]:
                rec["matched_queries"].append(q)
            pos = r.get("position") or 100
            rec["rrf_score"] += 1.0 / (RRF_K + max(1, pos))
            if r.get("pdf_url"):
                rec["rrf_score"] += 0.004
            cited = r.get("cited_by_count") or 0
            if spec.domain_type == "research_paper" and isinstance(cited, (int, float)):
                rec["rrf_score"] += min(0.004, math.log1p(max(cited, 0)) / 2000.0)

    ranked = sorted(aggregated.values(), key=lambda x: x["rrf_score"], reverse=True)
    top_n = fetch_top_n_per_query or FETCH_TOP_N_PER_QUERY
    max_fetch = max_fetch_per_retrieval or MAX_FETCH_PER_RETRIEVAL
    fetch_budget = min(max_fetch, max(1, len(query_pairs)) * top_n)
    selected = ranked[:fetch_budget]
    fetched, fetch_batches = await _fetch_many([r["fetch_url"] for r in selected], spec.purpose or objective)

    contexts: list[dict[str, Any]] = []
    for meta in selected:
        f = fetched.get(meta["fetch_url"], {})
        content = f.get("content") or meta.get("snippet") or ""
        contexts.append(
            {
                **meta,
                **f,
                "content": content,
                "content_origin": "search_snippet" if f.get("fetch_failed") else "fetch",
                "domain": _domain_of(f.get("final_url") or meta.get("fetch_url") or ""),
                "registered_domain": _registrable_domain(
                    _domain_of(f.get("final_url") or meta.get("fetch_url") or "")
                ),
                "retrieval_objective": objective,
            }
        )
    return contexts, {
        "search_requests": search_requests,
        "fetch_urls": len(selected),
        "fetch_batches": fetch_batches,
        "retrieval_waves": 1 if query_pairs else 0,
    }


async def _enrich_source(source: dict[str, Any]) -> None:
    """Run expensive reliability checks only once, preferably on evidence-used sources."""
    if source.get("reliability_enriched"):
        return
    r, a = await asyncio.gather(
        _check_retraction(source.get("doi") or source.get("final_url")),
        _check_author(source.get("author")),
    )
    source["retraction_check"] = r
    source["author_track_record"] = a
    source["authority_score"] = round(_authority_score(source) * _reliability_penalty(source), 4)
    source["reliability_enriched"] = True


def _merge_retrieval_context(existing: dict[str, Any], ctx: dict[str, Any]) -> None:
    existing.setdefault("matched_queries", [])
    existing["matched_queries"] = sorted(
        set(existing["matched_queries"]) | set(ctx.get("matched_queries", []))
    )
    existing.setdefault("retrieval_objectives", [])
    obj = ctx.get("retrieval_objective")
    if obj and obj not in existing["retrieval_objectives"]:
        existing["retrieval_objectives"].append(obj)
    existing["rrf_score"] = max(existing.get("rrf_score", 0.0), ctx.get("rrf_score", 0.0))


def _register_candidates(state: dict[str, Any], contexts: list[dict[str, Any]]) -> list[str]:
    sources = state.setdefault("sources", {})
    by_url = state.setdefault("source_by_canonical_url", {})
    by_hash = state.setdefault("source_by_content_hash", {})
    ids: list[str] = []

    for ctx in contexts:
        final_url = ctx.get("final_url") or ctx.get("fetch_url") or ctx.get("landing_url") or ""
        canonical = _canonicalize_url(final_url)
        chash = _content_hash(ctx.get("content", "")) if ctx.get("content") else ""

        existing_id = by_url.get(canonical) or (by_hash.get(chash) if chash else None)
        if existing_id and existing_id in sources:
            existing = sources[existing_id]
            _merge_retrieval_context(existing, ctx)
            # Opportunistically fill missing publication metadata from a new
            # retrieval representation of the same source.
            for key in (
                "title",
                "description",
                "publisher",
                "venue",
                "year",
                "pdf_url",
                "published_date",
                "doi",
                "openalex_id",
            ):
                if not existing.get(key) and ctx.get(key):
                    existing[key] = ctx.get(key)
            if not existing.get("authors") and ctx.get("authors"):
                existing["authors"] = ctx.get("authors") or []
                existing["author"] = existing["authors"][0] if existing["authors"] else existing.get("author")
            existing["source_role"] = _source_role(existing)
            existing["publication_status"] = _publication_status(existing)
            existing["publication_title"] = existing.get("title")
            existing["publication_venue"] = existing.get("venue")
            existing["authority_score"] = _authority_score(existing)
            _attach_source_to_work(state, existing)
            ids.append(existing_id)
            continue

        source_id = f"src_{uuid.uuid4().hex[:12]}"
        record = {
            "source_id": source_id,
            "url": ctx.get("landing_url") or ctx.get("url") or final_url,
            "fetch_url": ctx.get("fetch_url") or final_url,
            "final_url": final_url,
            "canonical_url": canonical,
            "domain": ctx.get("domain") or _domain_of(final_url),
            "registered_domain": ctx.get("registered_domain") or _registrable_domain(_domain_of(final_url)),
            "title": ctx.get("title"),
            "description": ctx.get("description"),
            "author": ctx.get("author") or (ctx.get("authors") or [None])[0],
            "authors": ctx.get("authors") or [],
            "publisher": ctx.get("publisher"),
            "venue": ctx.get("venue"),
            "year": ctx.get("year"),
            "cited_by_count": ctx.get("cited_by_count"),
            "pdf_url": ctx.get("pdf_url"),
            "domain_type": ctx.get("domain_type", "web"),
            "published_ts": ctx.get("published_ts"),
            "published_date": ctx.get("published_date") or ctx.get("date"),
            "content": ctx.get("content") or "",
            "content_hash": chash,
            "content_origin": ctx.get("content_origin", "fetch"),
            "fetch_failed": bool(ctx.get("fetch_failed")),
            "fetch_error": ctx.get("fetch_error"),
            "matched_queries": list(ctx.get("matched_queries", [])),
            "retrieval_objectives": [ctx.get("retrieval_objective")]
            if ctx.get("retrieval_objective")
            else [],
            "rrf_score": ctx.get("rrf_score", 0.0),
            "screen_status": "PENDING",
            "client_screen_verdict": None,
            "screen_reason": None,
            "retraction_check": {"checked": False},
            "author_track_record": {"checked": False},
            "reliability_enriched": False,
        }
        # Hosting provenance and publication provenance are separate.
        record["host_domain"] = record.get("registered_domain") or record.get("domain")
        record["source_role"] = _source_role(record)
        record["publication_status"] = _publication_status(record)
        record["publication_title"] = record.get("title")
        record["publication_venue"] = record.get("venue")
        record["doi"] = ctx.get("doi") or _extract_doi_from_source(record)
        record["openalex_id"] = ctx.get("openalex_id")
        record["arxiv_id"] = _extract_arxiv_id_from_source(record)
        ok, mechanical_score, mechanical_reasons = _mechanical_source_gate(
            record | {"retrieval_objective": ctx.get("retrieval_objective", "")}
        )
        record["mechanical_gate_passed"] = ok
        record["mechanical_quality_score"] = mechanical_score
        record["mechanical_reasons"] = mechanical_reasons
        record["authority_score"] = _authority_score(record)
        if not ok:
            record["screen_status"] = "AUTO_REJECTED"

        sources[source_id] = record
        _attach_source_to_work(state, record)
        _store_source_content(state["research_id"], source_id, record.get("content", ""), chash)
        if canonical:
            by_url[canonical] = source_id
        if chash:
            by_hash[chash] = source_id
        ids.append(source_id)
    return ids


def _source_context(state: dict[str, Any], source: dict[str, Any], objective: str) -> dict[str, Any]:
    text = _get_source_content(state, source["source_id"])
    return {
        "source_id": source["source_id"],
        "domain": source.get("domain"),
        "title": source.get("title"),
        "url": source.get("final_url") or source.get("url"),
        "domain_type": source.get("domain_type"),
        "host_domain": source.get("host_domain") or source.get("registered_domain"),
        "source_role": source.get("source_role"),
        "publication_status": source.get("publication_status"),
        "publication_venue": source.get("publication_venue") or source.get("venue"),
        "doi": source.get("doi"),
        "arxiv_id": source.get("arxiv_id"),
        "openalex_id": source.get("openalex_id"),
        "work_id": source.get("work_id"),
        "evidence_family_id": source.get("evidence_family_id"),
        "work_identity_confidence": source.get("work_identity_confidence"),
        "work_identity_type": source.get("work_identity_type"),
        "fetch_failed": source.get("fetch_failed"),
        "content_origin": source.get("content_origin"),
        "screen_status": source.get("screen_status"),
        "mechanical_gate_passed": source.get("mechanical_gate_passed"),
        "mechanical_quality_score": source.get("mechanical_quality_score"),
        "authority_score": source.get("authority_score"),
        "mechanical_reasons": source.get("mechanical_reasons", []),
        "passages": _top_passages(text, objective, limit=3, max_chars=900),
    }


def _candidate_rank(source: dict[str, Any]) -> float:
    return (
        0.55 * float(source.get("mechanical_quality_score") or 0.0)
        + 0.30 * float(source.get("authority_score") or 0.0)
        + 0.15 * min(1.0, float(source.get("rrf_score") or 0.0) * 40.0)
    )


def _shortlist_for_review(
    state: dict[str, Any],
    source_ids: list[str],
    objective: str,
    limit: int,
    *,
    include_accepted: bool = False,
    exclude_work_ids: Optional[set[str]] = None,
) -> list[dict[str, Any]]:
    candidates = []
    seen = set()
    for sid in source_ids:
        if sid in seen:
            continue
        seen.add(sid)
        source = state.get("sources", {}).get(sid)
        if not source or source.get("screen_status") == "AUTO_REJECTED":
            continue
        if exclude_work_ids and _work_id_for_source(source) in exclude_work_ids:
            continue
        if source.get("screen_status") == "ACCEPTED" and not include_accepted:
            continue
        candidates.append(source)
    candidates.sort(key=_candidate_rank, reverse=True)
    selected = []
    selected_works = set()
    for source in candidates:
        work_id = _work_id_for_source(source)
        if work_id in selected_works:
            continue
        selected_works.add(work_id)
        selected.append(source)
        if len(selected) >= max(0, limit):
            break
    selected_ids = {s["source_id"] for s in selected}
    for source in candidates:
        if source["source_id"] in selected_ids:
            if source.get("screen_status") == "DEFERRED":
                source["screen_status"] = "PENDING"
        elif source.get("screen_status") == "PENDING":
            source["screen_status"] = "DEFERRED"
            source["deferred_reason"] = "outside_profile_review_shortlist"
    return [_source_context(state, s, objective) for s in selected]


def _pending_source_contexts(
    state: dict[str, Any], source_ids: list[str], objective: str
) -> list[dict[str, Any]]:
    out = []
    for sid in source_ids:
        source = state.get("sources", {}).get(sid)
        if not source or source.get("screen_status") not in {"PENDING", "ACCEPTED"}:
            continue
        out.append(_source_context(state, source, objective))
        if len(out) >= MAX_CANDIDATE_CONTEXTS:
            break
    return out


def _claim_evidence_candidates(
    state: dict[str, Any], claim_ids: Optional[list[str]] = None, per_claim: int = 4
) -> list[dict[str, Any]]:
    """Deterministically suggest accepted sources/passages for each active claim.

    This removes the need for the client to inspect server state or call
    get_source_context repeatedly just to discover likely evidence.
    """
    wanted = set(claim_ids or [])
    accepted = [s for s in state.get("sources", {}).values() if s.get("screen_status") == "ACCEPTED"]
    out = []
    for claim in _active_claims(state):
        if wanted and claim["claim_id"] not in wanted:
            continue
        ranked = []
        for source in accepted:
            text = " ".join(
                _top_passages(
                    _get_source_content(state, source["source_id"]), claim["text"], limit=2, max_chars=700
                )
            )
            lexical = _token_overlap_score(claim["text"], f"{source.get('title') or ''} {text}")
            score = (
                0.60 * lexical
                + 0.25 * float(source.get("authority_score") or 0.0)
                + 0.15 * float(source.get("mechanical_quality_score") or 0.0)
            )
            if lexical > 0.01:
                ranked.append((score, source))
        ranked.sort(key=lambda x: x[0], reverse=True)
        # Prefer distinct underlying works so mirrors do not crowd out genuinely
        # independent evidence in the client packet.
        selected_sources = []
        seen_works = set()
        for _, source in ranked:
            work_id = _work_id_for_source(source)
            if work_id in seen_works:
                continue
            seen_works.add(work_id)
            selected_sources.append(source)
            if len(selected_sources) >= per_claim:
                break
        out.append(
            {
                "claim_id": claim["claim_id"],
                "claim_text": claim["text"],
                "sources": [_source_context(state, s, claim["text"]) for s in selected_sources],
            }
        )
    return out


def _prepare_candidate_review_queue(
    state: dict[str, Any],
    claim_ids: Optional[list[str]] = None,
    *,
    per_claim: int = 4,
) -> list[dict[str, Any]]:
    """Create a complete client work packet for initial/revised claims.

    This removes get_research_state -> bind_evidence -> judge_evidence from the
    normal path. The same review_candidates contract is used for both initial
    and gap evidence.
    """
    batches = _claim_evidence_candidates(state, claim_ids, per_claim=per_claim)
    queue = []
    for batch in batches:
        for source in batch.get("sources", []):
            queue.append(
                {
                    "claim_id": batch["claim_id"],
                    "source_id": source["source_id"],
                    "objective": batch["claim_text"],
                }
            )
    dedup = {(q["claim_id"], q["source_id"]): q for q in queue}
    state["candidate_review_queue"] = list(dedup.values())
    return batches


def _phase_violation(state: dict[str, Any], allowed: set[str], expected_tool: str) -> ResearchToolResponse:
    return ResearchToolResponse(
        status="PHASE_VIOLATION",
        data={"current_phase": state.get("phase"), "allowed_phases": sorted(allowed)},
        next_action=NextAction(
            tool=expected_tool,
            reason="The requested operation is not valid in the current protocol phase.",
            instructions=["Follow the server-owned phase machine; do not skip quality gates."],
        ),
    )


def _active_claims(state: dict[str, Any]) -> list[dict[str, Any]]:
    superseded = set(state.get("superseded_claim_ids", []))
    return [c for c in state.get("claims", []) if c["claim_id"] not in superseded]


def _claim_by_id(state: dict[str, Any], claim_id: str) -> dict[str, Any]:
    for c in state.get("claims", []):
        if c["claim_id"] == claim_id:
            return c
    raise ValueError(f"Unknown claim_id {claim_id!r}; never invent claim IDs.")


def _atomicity_warning(text: str, split_depth: int) -> Optional[str]:
    if split_depth >= MAX_ATOMICITY_SPLIT_DEPTH:
        return None
    lowered = text.lower()
    sentence_count = len([s for s in re.split(r"[.!?]+", text) if s.strip()])
    conjunctions = len(re.findall(r"\b(and|but|while|whereas|as well as)\b", lowered))
    if ";" in text or sentence_count > 1 or conjunctions >= 2:
        return "Claim may contain multiple independently falsifiable propositions. Split it or explicitly override atomicity."
    return None


def _all_atomicity_resolved(state: dict[str, Any]) -> bool:
    for c in _active_claims(state):
        if c.get("atomicity_status") == "REVIEW_REQUIRED":
            return False
    return True


def _apply_metrics(state: dict[str, Any], metrics: dict[str, int], *, include_wave: bool = True) -> None:
    m = state.setdefault("metrics", {})
    for key in ("search_requests", "fetch_urls", "fetch_batches"):
        m[key] = int(m.get(key, 0)) + int(metrics.get(key, 0))
    if include_wave:
        m["retrieval_waves"] = int(m.get("retrieval_waves", 0)) + (
            1 if int(metrics.get("retrieval_waves", 0)) > 0 else 0
        )


def _budget_remaining(state: dict[str, Any]) -> dict[str, int]:
    policy = state.get("policy", {})
    metrics = state.get("metrics", {})
    return {
        "search_requests": max(
            0, int(policy.get("max_search_requests", 10**9)) - int(metrics.get("search_requests", 0))
        ),
        "fetch_urls": max(0, int(policy.get("max_fetch_urls", 10**9)) - int(metrics.get("fetch_urls", 0))),
    }


def _consume_followup_round(state: dict[str, Any], *, reason: str) -> bool:
    max_rounds = int(state.get("policy", {}).get("max_followup_rounds", 0))
    used = int(state.get("followup_rounds", 0))
    if used >= max_rounds:
        return False
    state["followup_rounds"] = used + 1
    state.setdefault("metrics", {})["followup_rounds"] = state["followup_rounds"]
    state.setdefault("followup_history", []).append({"at": time.time(), "reason": reason})
    return True


def _elapsed_ms(state: dict[str, Any]) -> int:
    return int((time.time() - state.get("started_at", time.time())) * 1000)


def _temporal_weight(claim: dict[str, Any], source: dict[str, Any]) -> float:
    mode = claim.get("temporal_mode", TemporalMode.TIMELESS.value)
    if mode in {TemporalMode.HISTORICAL.value, TemporalMode.FOUNDATIONAL.value, TemporalMode.TIMELESS.value}:
        return 1.0
    ts = source.get("published_ts")
    if ts is None:
        return 0.60
    age_days = max(0.0, (time.time() - float(ts)) / 86400.0)
    return 0.5 ** (age_days / CURRENT_EVIDENCE_HALF_LIFE_DAYS)


def _source_screen_relevance(source: dict[str, Any]) -> float:
    verdict = source.get("client_screen_verdict")
    return {"RELEVANT": 1.0, "PARTIAL": 0.60}.get(verdict, 0.0)


def _reliability_penalty(source: dict[str, Any]) -> float:
    r = source.get("retraction_check") or {}
    if r.get("retracted"):
        return 0.03
    if r.get("concern_flagged"):
        return 0.45
    if r.get("corrected"):
        return 0.70
    a = source.get("author_track_record") or {}
    count = a.get("retracted_count", 0) if a.get("checked") else 0
    return max(0.35, 1.0 - 0.12 * count)


def _evidence_weight(claim: dict[str, Any], ev: dict[str, Any], source: dict[str, Any]) -> float:
    authority = source.get("authority_score", 0.5)
    relevance = float(ev.get("source_relevance", _source_screen_relevance(source)))
    full_fetch = 1.0 if source.get("content_origin") == "fetch" and not source.get("fetch_failed") else 0.35
    temporal = _temporal_weight(claim, source)
    reliability = _reliability_penalty(source)
    return ev.get("strength", 0.7) * authority * relevance * full_fetch * temporal * reliability


def _aggregate_evidence_by_work(
    records: list[tuple[dict[str, Any], dict[str, Any], float]],
) -> list[dict[str, Any]]:
    """Collapse mirrors/copies so one intellectual work contributes once.

    If different quotations from the same work receive conflicting client
    semantic judgments, only the stronger direction survives when the margin is
    material; near-ties are treated as an internal work conflict and contribute
    to neither side.
    """
    grouped: dict[str, list[tuple[dict[str, Any], dict[str, Any], float]]] = defaultdict(list)
    for ev, source, weight in records:
        grouped[_work_id_for_source(source)].append((ev, source, weight))

    out: list[dict[str, Any]] = []
    for work_id, rows in grouped.items():
        support_rows = [r for r in rows if r[0].get("relation") == "SUPPORTS"]
        contradict_rows = [r for r in rows if r[0].get("relation") == "CONTRADICTS"]
        best_support = max(support_rows, key=lambda r: r[2]) if support_rows else None
        best_contradict = max(contradict_rows, key=lambda r: r[2]) if contradict_rows else None

        relation = None
        chosen = None
        internal_conflict = False
        if best_support and best_contradict:
            sw, cw = best_support[2], best_contradict[2]
            denom = max(sw, cw, 1e-9)
            if abs(sw - cw) / denom <= 0.10:
                internal_conflict = True
                chosen = best_support if sw >= cw else best_contradict
            elif sw > cw:
                relation, chosen = "SUPPORTS", best_support
            else:
                relation, chosen = "CONTRADICTS", best_contradict
        elif best_support:
            relation, chosen = "SUPPORTS", best_support
        elif best_contradict:
            relation, chosen = "CONTRADICTS", best_contradict

        if chosen is None:
            continue
        ev, source, weight = chosen
        out.append(
            {
                "work_id": work_id,
                "relation": relation,
                "internal_conflict": internal_conflict,
                "evidence": ev,
                "source": source,
                "weight": weight if relation else 0.0,
                "support_weight": best_support[2] if best_support else 0.0,
                "contradict_weight": best_contradict[2] if best_contradict else 0.0,
                "copy_count": len(rows),
            }
        )
    return out


def assess_one_claim(state: dict[str, Any], claim: dict[str, Any]) -> ClaimAssessment:
    eligible = [
        e
        for e in state.get("evidence", [])
        if e["claim_id"] == claim["claim_id"]
        and e.get("quote_verified")
        and e.get("relation") in {"SUPPORTS", "CONTRADICTS"}
    ]
    if not eligible:
        return ClaimAssessment(
            claim_id=claim["claim_id"],
            status="UNKNOWN",
            stance_confidence=0.0,
            evidence_quality=0.0,
            resolution_confidence=0.0,
            metrics={"had_eligible_evidence": False},
        )

    records = []
    for ev in eligible:
        source = state.get("sources", {}).get(ev["source_id"], {})
        if source.get("screen_status") != "ACCEPTED":
            continue
        records.append((ev, source, _evidence_weight(claim, ev, source)))

    if not records:
        return ClaimAssessment(
            claim_id=claim["claim_id"],
            status="UNKNOWN",
            stance_confidence=0.0,
            evidence_quality=0.0,
            resolution_confidence=0.0,
            metrics={"had_eligible_evidence": False},
        )

    work_rows = _aggregate_evidence_by_work(records)
    directional = [w for w in work_rows if w.get("relation") in {"SUPPORTS", "CONTRADICTS"}]
    support = sum(w["weight"] for w in directional if w["relation"] == "SUPPORTS")
    contradict = sum(w["weight"] for w in directional if w["relation"] == "CONTRADICTS")
    total = support + contradict
    stance_conf = abs(support - contradict) / total if total > 0 else 0.0
    direction = "SUPPORTS" if support >= contradict else "CONTRADICTS"
    winning = [w for w in directional if w["relation"] == direction]

    winning_work_ids = {w["work_id"] for w in winning}
    independent_winning_work_ids = {
        w["work_id"] for w in winning if float(w["source"].get("work_identity_confidence", 0.35)) >= 0.70
    }
    contradicting_work_ids = {
        w["work_id"]
        for w in directional
        if w["relation"] == "CONTRADICTS" and float(w["source"].get("work_identity_confidence", 0.35)) >= 0.70
    }
    winning_domains = {
        w["source"].get("registered_domain") for w in winning if w["source"].get("registered_domain")
    }
    internal_conflicts = [w["work_id"] for w in work_rows if w.get("internal_conflict")]

    work_count_factor = min(1.0, len(independent_winning_work_ids) / max(1, MIN_INDEPENDENT_WORKS))
    quote_ratio = sum(1 for w in winning if w["evidence"].get("quote_verified")) / max(1, len(winning))
    full_fetch_ratio = sum(
        1
        for w in winning
        if w["source"].get("content_origin") == "fetch" and not w["source"].get("fetch_failed")
    ) / max(1, len(winning))
    relevance_avg = sum(_source_screen_relevance(w["source"]) for w in winning) / max(1, len(winning))
    authority_avg = sum(w["source"].get("authority_score", 0.5) for w in winning) / max(1, len(winning))
    temporal_avg = sum(_temporal_weight(claim, w["source"]) for w in winning) / max(1, len(winning))
    host_diversity = min(1.0, len(winning_domains) / max(1, len(winning_work_ids)))

    evidence_quality = (
        0.20 * work_count_factor
        + 0.15 * quote_ratio
        + 0.15 * full_fetch_ratio
        + 0.15 * relevance_avg
        + 0.20 * authority_avg
        + 0.10 * temporal_avg
        + 0.05 * host_diversity
    )

    any_full_fetch = any(
        w["source"].get("content_origin") == "fetch" and not w["source"].get("fetch_failed") for w in winning
    )
    max_authority = max((w["source"].get("authority_score", 0.0) for w in winning), default=0.0)
    sufficient = (
        len(independent_winning_work_ids) >= MIN_INDEPENDENT_WORKS
        and any_full_fetch
        and max_authority >= MIN_AUTHORITY_FOR_STRONG_RESOLUTION
        and evidence_quality >= MIN_EVIDENCE_QUALITY
        and stance_conf >= MIN_STANCE_CONFIDENCE
    )

    adversarial_required = bool(state.get("contested"))
    adversarial_done = bool(claim.get("disconfirmation_attempted"))

    if total == 0:
        status = "CONTESTED" if internal_conflicts else "UNKNOWN"
    elif stance_conf < CONTESTED_MARGIN_THRESHOLD:
        status = "CONTESTED"
    elif len(contradicting_work_ids) >= MIN_DISSENTING_WORKS_FOR_OVERRIDE and direction == "SUPPORTS":
        status = "CONTESTED"
    elif sufficient and (not adversarial_required or adversarial_done):
        status = "SUPPORTED" if direction == "SUPPORTS" else "CONTRADICTED"
    else:
        status = "PROVISIONAL_SUPPORTED" if direction == "SUPPORTS" else "PROVISIONAL_CONTRADICTED"

    resolution_conf = min(stance_conf, evidence_quality)
    flags = []
    if len(independent_winning_work_ids) < MIN_INDEPENDENT_WORKS:
        flags.append("insufficient_independent_works")
    if not any_full_fetch:
        flags.append("no_full_fetch_evidence")
    if max_authority < MIN_AUTHORITY_FOR_STRONG_RESOLUTION:
        flags.append("no_sufficiently_authoritative_source")
    if adversarial_required and not adversarial_done:
        flags.append("disconfirmation_attempt_required")
    if evidence_quality < MIN_EVIDENCE_QUALITY:
        flags.append("evidence_quality_below_threshold")
    if internal_conflicts:
        flags.append("within_work_judgment_conflict")

    return ClaimAssessment(
        claim_id=claim["claim_id"],
        status=status,
        stance_confidence=round(stance_conf, 4),
        evidence_quality=round(evidence_quality, 4),
        resolution_confidence=round(resolution_conf, 4),
        metrics={
            "had_eligible_evidence": True,
            "support_weight": round(support, 4),
            "contradict_weight": round(contradict, 4),
            "winning_direction": direction,
            "winning_independent_works": len(independent_winning_work_ids),
            "winning_work_ids": sorted(winning_work_ids),
            "independence_eligible_work_ids": sorted(independent_winning_work_ids),
            "low_confidence_work_ids": sorted(winning_work_ids - independent_winning_work_ids),
            "winning_host_domains": len(winning_domains),
            "contradicting_work_count": len(contradicting_work_ids),
            "full_fetch_ratio": round(full_fetch_ratio, 4),
            "authority_avg": round(authority_avg, 4),
            "temporal_avg": round(temporal_avg, 4),
            "host_diversity": round(host_diversity, 4),
            "within_work_conflicts": internal_conflicts,
            "raw_evidence_records": len(records),
            "independent_work_records": len(work_rows),
            "disconfirmation_attempted": adversarial_done,
            "quality_flags": flags,
        },
    )


def _assessment_partition(assessments: list[ClaimAssessment]) -> dict[str, list[ClaimAssessment]]:
    out = {"resolved": [], "provisional": [], "contested": [], "unknown": []}
    for a in assessments:
        if a.status in {"SUPPORTED", "CONTRADICTED"}:
            out["resolved"].append(a)
        elif a.status.startswith("PROVISIONAL_"):
            out["provisional"].append(a)
        elif a.status == "CONTESTED":
            out["contested"].append(a)
        else:
            out["unknown"].append(a)
    return out


def _agent_rules() -> list[str]:
    return [
        "Never invent server IDs or inspect implementation state to choose the next tool.",
        "Follow next_action exactly; it contains the complete bounded task.",
        "RELATED_BUT_INSUFFICIENT means topical evidence that does not establish the claim.",
        "Stop researching any claim already strongly resolved by the server.",
        "Treat different URLs/hosts with the same work_id as one evidence family, never independent confirmation.",
        "Do not introduce new factual claims during final synthesis outside synthesis_manifest.",
    ]


def _next(
    tool: str,
    reason: str,
    instructions: list[str],
    required_input: Optional[dict[str, Any]] = None,
    completion_condition: Optional[str] = None,
) -> NextAction:
    return NextAction(
        tool=tool,
        reason=reason,
        instructions=instructions,
        required_input=required_input or {},
        completion_condition=completion_condition,
    )


def _research_quality_summary(state: dict[str, Any]) -> dict[str, Any]:
    accepted = [s for s in state.get("sources", {}).values() if s.get("screen_status") == "ACCEPTED"]
    return {
        "phase": state.get("phase"),
        "accepted_source_copies": len(accepted),
        "accepted_unique_works": len({_work_id_for_source(s) for s in accepted}),
        "active_claims": len(_active_claims(state)),
        "verified_citations": len(state.get("verified_citations", [])),
        "metrics": state.get("metrics", {}),
        "elapsed_ms": _elapsed_ms(state),
    }


async def init_research(
    topic: str,
    max_rounds: Optional[int] = None,
    contested: bool = False,
    mode: ResearchMode = ResearchMode.FAST,
) -> ResearchToolResponse:
    """Start a research session with an explicit latency/coverage profile.

    FAST is the default and preserves the same hard evidence gates while
    fetching/reviewing fewer candidates and stopping resolved claims early.
    """
    profile = MODE_POLICIES[mode.value]
    rounds = profile["max_default_rounds"] if max_rounds is None else max(0, int(max_rounds))
    research_id = f"res_{uuid.uuid4().hex[:12]}"
    state = {
        "research_id": research_id,
        "topic": topic,
        "contested": contested,
        "mode": mode.value,
        "phase": "INITIALIZED",
        "started_at": time.time(),
        "policy": {
            "max_gap_rounds": rounds,
            "max_followup_rounds": rounds,
            **profile,
        },
        "gap_rounds": 0,
        "followup_rounds": 0,
        "executed_queries_set": set(),
        "plan": None,
        "sources": {},
        "source_by_canonical_url": {},
        "source_by_content_hash": {},
        "works": {},
        "work_alias_index": {},
        "claims": [],
        "superseded_claim_ids": [],
        "evidence": [],
        "last_assessments": [],
        "candidate_review_queue": [],
        "claim_tensions": [],
        "tension_review_done": False,
        "verified_citations": [],
        "citation_audit": None,
        "metrics": {
            "search_requests": 0,
            "fetch_urls": 0,
            "fetch_batches": 0,
            "retrieval_waves": 0,
            "gap_rounds": 0,
            "followup_rounds": 0,
            "sources_presented_to_client": 0,
            "candidate_reviews": 0,
            "early_stop_skips": 0,
            "work_dedup_merges": 0,
        },
    }
    persist(state)
    return ResearchToolResponse(
        status="INITIALIZED",
        data={
            "research_id": research_id,
            "contested": contested,
            "mode": mode.value,
            "max_gap_rounds": rounds,
            "profile": profile,
        },
        next_action=_next(
            "plan_research",
            "Decompose the topic into non-overlapping evidence tracks.",
            [
                "For academic/scientific tracks use retrieval.domain_type='research_paper'.",
                "For contested topics include at least one disconfirming track.",
                f"This session is {mode.value}; keep the plan compact and evidence-focused.",
            ],
            {"research_id": research_id, "plan": "ResearchPlan"},
            "A non-overlapping plan is accepted.",
        ),
        agent_rules=_agent_rules(),
    )


async def plan_research(research_id: str, plan: ResearchPlan) -> ResearchToolResponse:
    """Validate and store the research plan before any retrieval."""
    state = load_state(research_id)
    if state.get("phase") != "INITIALIZED":
        return _phase_violation(state, {"INITIALIZED"}, "dispatch_parallel_subagents")
    if not plan.tasks and plan.execution_mode == "parallel_subagents":
        raise ValueError("parallel_subagents mode requires at least one task")
    if len(plan.tasks) > MAX_PARALLEL_SUBAGENTS:
        raise ValueError(f"At most {MAX_PARALLEL_SUBAGENTS} subagents are allowed")
    profile_task_cap = int(_mode_policy(state).get("max_parallel_tasks", MAX_PARALLEL_SUBAGENTS))
    if plan.execution_mode == "parallel_subagents" and len(plan.tasks) > profile_task_cap:
        return ResearchToolResponse(
            status="PLAN_REJECTED_PROFILE_BUDGET",
            data={
                "proposed_task_count": len(plan.tasks),
                "profile_task_cap": profile_task_cap,
                "mode": state.get("mode"),
            },
            next_action=_next(
                "plan_research",
                "The plan exceeds this research mode's latency budget.",
                [
                    f"Merge overlapping/adjacent evidence tracks so the plan uses at most {profile_task_cap} tasks."
                ],
            ),
            agent_rules=_agent_rules(),
        )

    conflicts = []
    for i, a in enumerate(plan.tasks):
        kwa = _keyword_set(a.objective + " " + a.task_boundaries)
        for b in plan.tasks[i + 1 :]:
            kwb = _keyword_set(b.objective + " " + b.task_boundaries)
            if kwa and kwb:
                jac = len(kwa & kwb) / len(kwa | kwb)
                if jac >= 0.58:
                    conflicts.append({"a": a.subagent_id, "b": b.subagent_id, "overlap": round(jac, 3)})
    if conflicts:
        return ResearchToolResponse(
            status="PLAN_REJECTED_OVERLAP",
            data={"conflicts": conflicts},
            next_action=_next(
                "plan_research",
                "Tasks overlap too much.",
                ["Redraft task_boundaries so each subagent owns a distinct evidence question."],
            ),
            agent_rules=_agent_rules(),
        )
    if state.get("contested") and not any(t.seeks_disconfirming_evidence for t in plan.tasks):
        return ResearchToolResponse(
            status="PLAN_REJECTED_ADVERSARIAL_COVERAGE",
            data={},
            next_action=_next(
                "plan_research",
                "Contested sessions require an explicit disconfirming track.",
                ["Set seeks_disconfirming_evidence=True on at least one appropriate task."],
            ),
            agent_rules=_agent_rules(),
        )

    state["plan"] = plan.model_dump()
    state["phase"] = "PLANNED"
    persist(state)
    return ResearchToolResponse(
        status="PLAN_ACCEPTED",
        data={"task_count": len(plan.tasks), "execution_mode": plan.execution_mode},
        next_action=_next(
            "dispatch_parallel_subagents"
            if plan.execution_mode == "parallel_subagents"
            else "discovery_search",
            "The retrieval plan passed structural checks.",
            ["Run the accepted plan; do not generate claims until candidate sources are screened."],
        ),
        quality_gate={"task_overlap": "PASS", "adversarial_coverage": "PASS"},
        agent_rules=_agent_rules(),
    )


async def dispatch_parallel_subagents(
    research_id: str, tasks: Optional[list[SubagentTask]] = None
) -> ResearchToolResponse:
    """Execute independent retrieval tracks concurrently and return only a compact review shortlist."""
    state = load_state(research_id)
    if state.get("phase") not in {"PLANNED", "RETRIEVING"}:
        return _phase_violation(state, {"PLANNED", "RETRIEVING"}, "screen_sources")
    if tasks is None:
        stored = state.get("plan") or {}
        tasks = [SubagentTask(**t) for t in stored.get("tasks", [])]
    if not tasks:
        raise ValueError("No subagent tasks available")

    state["phase"] = "RETRIEVING"
    policy = _mode_policy(state)

    async def run(task: SubagentTask):
        queries = task.queries[: int(policy.get("max_initial_queries_per_task", len(task.queries)))]
        contexts, metrics = await _retrieve_candidates(
            queries,
            task.retrieval,
            state["executed_queries_set"],
            task.objective,
            fetch_top_n_per_query=policy["fetch_top_n_per_query"],
            max_fetch_per_retrieval=policy["max_fetch_per_retrieval"],
        )
        return task, contexts, metrics

    raw = await asyncio.gather(*(run(t) for t in tasks), return_exceptions=True)
    findings, errors = [], []
    review_queue = []
    for task, result in zip(tasks, raw):
        if isinstance(result, Exception):
            errors.append({"subagent_id": task.subagent_id, "error": str(result)})
            continue
        actual_task, contexts, metrics = result
        _apply_metrics(state, metrics, include_wave=False)
        source_ids = _register_candidates(state, contexts)
        shortlist = _shortlist_for_review(
            state,
            source_ids,
            actual_task.objective,
            policy["initial_review_limit_per_task"],
            include_accepted=False,
        )
        findings.append(
            {
                "subagent_id": actual_task.subagent_id,
                "objective": actual_task.objective,
                "seeks_disconfirming_evidence": actual_task.seeks_disconfirming_evidence,
                "candidate_contexts": shortlist,
                "auto_rejected_count": sum(
                    1 for sid in source_ids if state["sources"][sid].get("screen_status") == "AUTO_REJECTED"
                ),
                "deferred_count": sum(
                    1 for sid in source_ids if state["sources"][sid].get("screen_status") == "DEFERRED"
                ),
            }
        )
        review_queue.extend(shortlist)

    review_queue = list({x["source_id"]: x for x in review_queue}.values())
    if findings:
        state["metrics"]["retrieval_waves"] = int(state["metrics"].get("retrieval_waves", 0)) + 1
    state["metrics"]["sources_presented_to_client"] += len(review_queue)
    state["phase"] = "SOURCE_SCREENING"
    persist(state)
    return ResearchToolResponse(
        status="SUBAGENTS_COMPLETE" if not errors else "SUBAGENTS_PARTIAL",
        data={"findings": findings, "errors": errors, "review_queue": review_queue},
        next_action=_next(
            "screen_sources",
            "Only the strongest mechanically-filtered candidates need semantic source screening.",
            [
                "Screen exactly the sources in data.review_queue; no ID bookkeeping is required.",
                "RELEVANT means directly useful evidence; PARTIAL means useful context but insufficient alone; otherwise IRRELEVANT.",
            ],
            {"research_id": research_id, "screenings_for": [x["source_id"] for x in review_queue]},
            "Every source in review_queue receives one verdict.",
        ),
        quality_gate={"candidate_source_gate": "PENDING_CLIENT_SCREEN"},
        agent_rules=_agent_rules(),
    )


async def discovery_search(
    research_id: str,
    queries: list[str],
    purpose: str,
    retrieval: Optional[RetrievalSpec] = None,
) -> ResearchToolResponse:
    """Run a compact additional retrieval wave; later waves consume the global follow-up budget."""
    state = load_state(research_id)
    if state.get("phase") not in {
        "PLANNED",
        "RETRIEVING",
        "SOURCE_SCREENING",
        "EVIDENCE_BINDING",
        "ASSESSED",
        "GAP_RESEARCH",
    }:
        return _phase_violation(state, {"PLANNED", "EVIDENCE_BINDING", "ASSESSED"}, "get_research_state")
    if int(state.get("metrics", {}).get("retrieval_waves", 0)) > 0:
        if not _consume_followup_round(state, reason="discovery_search"):
            state["phase"] = "CLAIM_GRAPH_STABLE" if state.get("claims") else "CLAIM_REGISTRATION"
            persist(state)
            return ResearchToolResponse(
                status="FOLLOWUP_BUDGET_EXHAUSTED",
                data={
                    "followup_rounds": state.get("followup_rounds"),
                    "max_followup_rounds": state.get("policy", {}).get("max_followup_rounds"),
                },
                next_action=_next(
                    "assess_claims" if state.get("claims") else "register_claims",
                    "Global follow-up retrieval budget is exhausted.",
                    ["Preserve uncertainty rather than bypassing the budget."],
                ),
                agent_rules=_agent_rules(),
            )
    spec = retrieval or RetrievalSpec(purpose=purpose)
    if not spec.purpose:
        spec.purpose = purpose
    policy = _mode_policy(state)
    budget = _budget_remaining(state)
    if budget["search_requests"] <= 0 or budget["fetch_urls"] <= 0:
        state["phase"] = "CLAIM_GRAPH_STABLE" if state.get("claims") else "CLAIM_REGISTRATION"
        persist(state)
        return ResearchToolResponse(
            status="RETRIEVAL_BUDGET_EXHAUSTED",
            data={"remaining_budget": budget},
            next_action=_next(
                "assess_claims" if state.get("claims") else "register_claims",
                "The global network budget is exhausted.",
                ["Preserve uncertainty; do not bypass the mode budget."],
            ),
            agent_rules=_agent_rules(),
        )
    query_cap = min(int(policy["max_queries_per_gap"]), budget["search_requests"])
    fetch_cap = min(int(policy["max_fetch_per_retrieval"]), budget["fetch_urls"])
    contexts, metrics = await _retrieve_candidates(
        queries[:query_cap],
        spec,
        state["executed_queries_set"],
        purpose,
        fetch_top_n_per_query=policy["fetch_top_n_per_query"],
        max_fetch_per_retrieval=fetch_cap,
    )
    _apply_metrics(state, metrics)
    source_ids = _register_candidates(state, contexts)
    shortlist = _shortlist_for_review(state, source_ids, purpose, policy["initial_review_limit_per_task"])
    state["metrics"]["sources_presented_to_client"] += len(shortlist)
    state["phase"] = "SOURCE_SCREENING"
    persist(state)
    return ResearchToolResponse(
        status="DISCOVERY_COMPLETE",
        data={"review_queue": shortlist},
        next_action=_next(
            "screen_sources",
            "Screen the compact review_queue before using any new source.",
            ["Screen exactly the returned source IDs; deferred sources do not block progress."],
            {"screenings_for": [x["source_id"] for x in shortlist]},
        ),
        agent_rules=_agent_rules(),
    )


async def screen_sources(research_id: str, screenings: list[SourceScreening]) -> ResearchToolResponse:
    """Initial semantic source gate over only the server-provided review shortlist."""
    state = load_state(research_id)
    if state.get("phase") != "SOURCE_SCREENING":
        return _phase_violation(state, {"SOURCE_SCREENING"}, "screen_sources")

    pending = {sid for sid, s in state.get("sources", {}).items() if s.get("screen_status") == "PENDING"}
    supplied = {s.source_id for s in screenings}
    unknown = supplied - set(state.get("sources", {}))
    if unknown:
        raise ValueError(f"Unknown source IDs: {sorted(unknown)}")

    accepted: list[str] = []
    rejected: list[dict[str, Any]] = []
    newly_accepted: list[dict[str, Any]] = []
    for screening in screenings:
        source = state["sources"][screening.source_id]
        if source.get("screen_status") == "AUTO_REJECTED":
            rejected.append({"source_id": screening.source_id, "reason": "mechanical_auto_reject"})
            continue
        source["client_screen_verdict"] = screening.verdict
        source["screen_reason"] = screening.reason
        if source.get("mechanical_gate_passed") and screening.verdict in {"RELEVANT", "PARTIAL"}:
            source["screen_status"] = "ACCEPTED"
            accepted.append(screening.source_id)
            newly_accepted.append(source)
        else:
            source["screen_status"] = "REJECTED"
            rejected.append({"source_id": screening.source_id, "reason": screening.reason})

    # FAST mode defers expensive Crossref/OpenAlex checks until a source is
    # actually used as evidence. Other modes enrich accepted sources eagerly.
    if newly_accepted and state.get("mode") != ResearchMode.FAST.value:
        await asyncio.gather(*(_enrich_source(s) for s in newly_accepted))

    remaining = sorted(pending - supplied)
    total_accepted = sum(1 for s in state.get("sources", {}).values() if s.get("screen_status") == "ACCEPTED")
    if remaining:
        remaining_contexts = _pending_source_contexts(state, remaining, state.get("topic", ""))
        next_action = _next(
            "screen_sources",
            "Some shortlisted sources remain unscreened.",
            ["Screen only data.remaining_contexts; deferred sources are intentionally ignored."],
            {"screenings_for": remaining},
        )
        status = "SOURCE_SCREENING_INCOMPLETE"
    elif not state.get("claims") and total_accepted == 0:
        state["phase"] = "RETRIEVING"
        remaining_contexts = []
        next_action = _next(
            "discovery_search",
            "No evidence-grade source survived the shortlist.",
            ["Use fewer, more specific queries and the correct domain_type."],
        )
        status = "NO_ACCEPTABLE_SOURCES"
    elif not state.get("claims"):
        state["phase"] = "CLAIM_REGISTRATION"
        remaining_contexts = []
        next_action = _next(
            "register_claims",
            "Initial sources are screened; register atomic claims.",
            ["Create claims only. The server will suggest likely evidence sources after registration."],
        )
        status = "SOURCES_SCREENED"
    else:
        remaining_contexts = []
        candidates = _prepare_candidate_review_queue(state, per_claim=4)
        if state.get("candidate_review_queue"):
            state["phase"] = "CANDIDATE_REVIEW"
            next_action = _next(
                "review_candidates",
                "Accepted sources are ready for unified relevance + quote + entailment review.",
                [
                    "Review only data.evidence_candidates; no get_research_state, bind_evidence, or judge_evidence call is needed."
                ],
                {"review_batches": candidates},
            )
            status = "SOURCES_SCREENED_REVIEW_REQUIRED"
        else:
            state["phase"] = "ASSESSED"
            next_action = _next(
                "assess_claims",
                "No new claim/source pair survived candidate selection.",
                ["Assess and let the server identify any remaining research gaps."],
            )
            status = "SOURCES_SCREENED_NO_NEW_CANDIDATES"

    persist(state)
    return ResearchToolResponse(
        status=status,
        data={
            "accepted_source_ids": accepted,
            "rejected": rejected,
            "remaining_pending": remaining,
            "remaining_contexts": remaining_contexts,
            "total_accepted_sources": total_accepted,
        },
        next_action=next_action,
        quality_gate={"source_screening": "PASS" if not remaining else "PENDING"},
        agent_rules=_agent_rules(),
    )


async def register_claims(research_id: str, claims: list[ClaimDraft]) -> ResearchToolResponse:
    """Register new claims only; does not accept evidence.

    This separation prevents claim creation and evidence judgment from being
    conflated in one call. Server generates every claim_id.
    """
    state = load_state(research_id)
    if state.get("phase") not in {"CLAIM_REGISTRATION", "EVIDENCE_BINDING", "ASSESSED"}:
        return _phase_violation(state, {"CLAIM_REGISTRATION"}, "register_claims")

    mapping: dict[str, str] = {}
    warnings: dict[str, str] = {}
    evidence_candidates: list[dict[str, Any]] = []
    for draft in claims:
        cid = f"claim_{uuid.uuid4().hex[:12]}"
        warning = None if draft.atomicity_override else _atomicity_warning(draft.text, 0)
        record = {
            "claim_id": cid,
            "client_ref": draft.client_ref,
            "text": draft.text,
            "entity": draft.entity,
            "temporal_mode": draft.temporal_mode.value,
            "split_depth": 0,
            "atomicity_status": "OVERRIDDEN"
            if draft.atomicity_override
            else ("REVIEW_REQUIRED" if warning else "PASS"),
            "atomicity_override_reason": draft.atomicity_override_reason,
            "created_at": time.time(),
            "disconfirmation_attempted": False,
        }
        state["claims"].append(record)
        mapping[draft.client_ref] = cid
        if warning:
            warnings[cid] = warning

    if warnings:
        state["phase"] = "ATOMICITY_REVIEW"
        next_action = _next(
            "split_claim",
            "Some claims failed the atomicity gate.",
            [
                "For each warned claim either call split_claim with atomic child claims,",
                "or call override_atomicity with a concrete reason it cannot be split further.",
            ],
            {"warned_claim_ids": list(warnings)},
            "No active claim remains in REVIEW_REQUIRED atomicity state.",
        )
        status = "ATOMICITY_REVIEW_REQUIRED"
    else:
        evidence_candidates = _prepare_candidate_review_queue(state, list(mapping.values()), per_claim=4)
        if state.get("candidate_review_queue"):
            state["phase"] = "CANDIDATE_REVIEW"
            next_action = _next(
                "review_candidates",
                "Claims passed atomicity; review the best distinct-work evidence candidates in one bounded call.",
                [
                    "For each returned claim/source pair choose RELEVANT/PARTIAL/IRRELEVANT.",
                    "If useful, copy one direct quote from passages and assign SUPPORTS/CONTRADICTS/RELATED_BUT_INSUFFICIENT/IRRELEVANT.",
                    "Do not call get_research_state, bind_evidence, or judge_evidence on the normal path.",
                ],
                {"review_batches": evidence_candidates},
                "All returned pairs are reviewed once.",
            )
            status = "CLAIMS_REGISTERED_REVIEW_REQUIRED"
        else:
            state["phase"] = "ASSESSED"
            next_action = _next(
                "assess_claims",
                "No accepted source is a viable initial evidence candidate.",
                ["Assess now; UNKNOWN claims will receive targeted gap-research instructions."],
            )
            status = "CLAIMS_REGISTERED_NO_INITIAL_EVIDENCE"

    persist(state)
    return ResearchToolResponse(
        status=status,
        data={
            "claim_ids": mapping,
            "atomicity_warnings": warnings,
            "active_claim_count": len(_active_claims(state)),
            "evidence_candidates": evidence_candidates if not warnings else [],
        },
        next_action=next_action,
        quality_gate={"atomicity": "PASS" if not warnings else "BLOCKED"},
        agent_rules=_agent_rules(),
    )


async def split_claim(
    research_id: str, parent_claim_id: str, children: list[ClaimDraft]
) -> ResearchToolResponse:
    """Split a compound claim. The parent is superseded automatically."""
    state = load_state(research_id)
    if state.get("phase") != "ATOMICITY_REVIEW":
        return _phase_violation(state, {"ATOMICITY_REVIEW"}, "split_claim")
    parent = _claim_by_id(state, parent_claim_id)
    if parent_claim_id in set(state.get("superseded_claim_ids", [])):
        raise ValueError("Cannot split an already-superseded claim")
    depth = int(parent.get("split_depth", 0)) + 1
    mapping: dict[str, str] = {}
    warnings: dict[str, str] = {}
    new_ids = []
    for child in children:
        cid = f"claim_{uuid.uuid4().hex[:12]}"
        warning = None if child.atomicity_override else _atomicity_warning(child.text, depth)
        state["claims"].append(
            {
                "claim_id": cid,
                "client_ref": child.client_ref,
                "text": child.text,
                "entity": child.entity or parent.get("entity"),
                "temporal_mode": child.temporal_mode.value,
                "split_depth": depth,
                "parent_claim_id": parent_claim_id,
                "atomicity_status": "OVERRIDDEN"
                if child.atomicity_override
                else ("REVIEW_REQUIRED" if warning else "PASS"),
                "atomicity_override_reason": child.atomicity_override_reason,
                "created_at": time.time(),
                "disconfirmation_attempted": False,
            }
        )
        mapping[child.client_ref] = cid
        new_ids.append(cid)
        if warning:
            warnings[cid] = warning
    state.setdefault("superseded_claim_ids", [])
    state["superseded_claim_ids"] = sorted(set(state["superseded_claim_ids"]) | {parent_claim_id})

    if _all_atomicity_resolved(state):
        review_batches = _prepare_candidate_review_queue(state, new_ids, per_claim=4)
        if state.get("candidate_review_queue"):
            state["phase"] = "CANDIDATE_REVIEW"
            next_action = _next(
                "review_candidates",
                "All active claims passed atomicity; review evidence for the new child claims.",
                ["Use only returned review_batches."],
                {"review_batches": review_batches},
            )
            status = "CLAIM_SPLIT_REVIEW_REQUIRED"
        else:
            state["phase"] = "ASSESSED"
            next_action = _next(
                "assess_claims",
                "All active claims passed atomicity but no initial evidence candidate survived.",
                ["Assess to generate targeted gaps."],
            )
            status = "CLAIM_SPLIT_ATOMICITY_PASS_NO_EVIDENCE"
    else:
        next_action = _next(
            "split_claim",
            "Additional active claims still require atomicity review.",
            ["Continue splitting or use override_atomicity on remaining warned claims."],
        )
        status = "CLAIM_SPLIT_REVIEW_REMAINS"
    persist(state)
    return ResearchToolResponse(
        status=status,
        data={
            "parent_superseded": parent_claim_id,
            "child_claim_ids": mapping,
            "atomicity_warnings": warnings,
        },
        next_action=next_action,
        quality_gate={"atomicity": "PASS" if _all_atomicity_resolved(state) else "BLOCKED"},
        agent_rules=_agent_rules(),
    )


async def override_atomicity(research_id: str, claim_id: str, reason: str) -> ResearchToolResponse:
    """Explicitly override an atomicity warning for a maximally atomic claim."""
    state = load_state(research_id)
    if state.get("phase") != "ATOMICITY_REVIEW":
        return _phase_violation(state, {"ATOMICITY_REVIEW"}, "override_atomicity")
    claim = _claim_by_id(state, claim_id)
    if claim_id in set(state.get("superseded_claim_ids", [])):
        raise ValueError("Cannot override a superseded claim")
    if len(reason.strip()) < 12:
        raise ValueError("Provide a concrete atomicity override reason")
    claim["atomicity_status"] = "OVERRIDDEN"
    claim["atomicity_override_reason"] = reason.strip()
    if _all_atomicity_resolved(state):
        judged_claims = {
            e.get("claim_id")
            for e in state.get("evidence", [])
            if e.get("relation") not in {None, "UNJUDGED"}
        }
        need_review = [c["claim_id"] for c in _active_claims(state) if c["claim_id"] not in judged_claims]
        review_batches = (
            _prepare_candidate_review_queue(state, need_review, per_claim=4) if need_review else []
        )
        if state.get("candidate_review_queue"):
            state["phase"] = "CANDIDATE_REVIEW"
            next_action = _next(
                "review_candidates",
                "All active claims passed atomicity; review evidence for claims that still lack judged evidence.",
                ["Use only returned review_batches."],
                {"review_batches": review_batches},
            )
            status = "ATOMICITY_PASS_REVIEW_REQUIRED"
        else:
            state["phase"] = "ASSESSED"
            next_action = _next(
                "assess_claims",
                "All active claims passed atomicity.",
                ["Assess current evidence and let the server route remaining gaps."],
            )
            status = "ATOMICITY_PASS"
    else:
        next_action = _next(
            "split_claim",
            "Other active claims still require atomicity review.",
            ["Resolve every remaining warning."],
        )
        status = "ATOMICITY_REVIEW_REMAINS"
    persist(state)
    return ResearchToolResponse(
        status=status,
        data={
            "claim_id": claim_id,
            "remaining_warned": [
                c["claim_id"] for c in _active_claims(state) if c.get("atomicity_status") == "REVIEW_REQUIRED"
            ],
        },
        next_action=next_action,
        agent_rules=_agent_rules(),
    )


async def revise_claim(research_id: str, claim_id: str, replacement: ClaimDraft) -> ResearchToolResponse:
    """Replace an incorrect/overbroad active claim without leaving the parent unresolved.

    The old claim is superseded automatically. Use this for semantic revision;
    use split_claim when one claim must become multiple children.
    """
    state = load_state(research_id)
    if state.get("phase") not in {"EVIDENCE_BINDING", "ASSESSED", "ATOMICITY_REVIEW"}:
        return _phase_violation(state, {"EVIDENCE_BINDING", "ASSESSED"}, "revise_claim")
    parent = _claim_by_id(state, claim_id)
    new_id = f"claim_{uuid.uuid4().hex[:12]}"
    depth = int(parent.get("split_depth", 0))
    warning = None if replacement.atomicity_override else _atomicity_warning(replacement.text, depth)
    state["claims"].append(
        {
            "claim_id": new_id,
            "client_ref": replacement.client_ref,
            "text": replacement.text,
            "entity": replacement.entity or parent.get("entity"),
            "temporal_mode": replacement.temporal_mode.value,
            "split_depth": depth,
            "revises_claim_id": claim_id,
            "atomicity_status": "OVERRIDDEN"
            if replacement.atomicity_override
            else ("REVIEW_REQUIRED" if warning else "PASS"),
            "atomicity_override_reason": replacement.atomicity_override_reason,
            "created_at": time.time(),
            "disconfirmation_attempted": False,
        }
    )
    state.setdefault("superseded_claim_ids", [])
    state["superseded_claim_ids"] = sorted(set(state["superseded_claim_ids"]) | {claim_id})
    if warning:
        state["phase"] = "ATOMICITY_REVIEW"
        next_action = _next(
            "split_claim", "Replacement claim still fails atomicity.", ["Split it or override with a reason."]
        )
        status = "CLAIM_REVISED_ATOMICITY_REVIEW_REQUIRED"
    else:
        review_batches = _prepare_candidate_review_queue(state, [new_id], per_claim=4)
        if state.get("candidate_review_queue"):
            state["phase"] = "CANDIDATE_REVIEW"
            next_action = _next(
                "review_candidates",
                "Replacement claim is active; review likely evidence in one bounded call.",
                ["Use only the replacement claim_id and returned review_batches."],
                {"review_batches": review_batches},
            )
            status = "CLAIM_REVISED_REVIEW_REQUIRED"
        else:
            state["phase"] = "ASSESSED"
            next_action = _next(
                "assess_claims",
                "Replacement claim is active but no initial evidence candidate survived.",
                ["Assess to generate a targeted gap plan."],
            )
            status = "CLAIM_REVISED_NO_INITIAL_EVIDENCE"
    persist(state)
    return ResearchToolResponse(
        status=status,
        data={"superseded_claim_id": claim_id, "replacement_claim_id": new_id, "atomicity_warning": warning},
        next_action=next_action,
        agent_rules=_agent_rules(),
    )


async def bind_evidence(research_id: str, bindings: list[EvidenceBinding]) -> ResearchToolResponse:
    """Bind direct source quotations to existing claims and verify quote presence.

    This tool does NOT accept semantic stance. Keeping quote binding separate
    from entailment judgment gives weak client models one bounded operation at a
    time and prevents a topical passage from becoming support merely because it
    was attached to a claim.
    """
    state = load_state(research_id)
    if state.get("phase") != "EVIDENCE_BINDING":
        return _phase_violation(state, {"EVIDENCE_BINDING"}, "bind_evidence")
    if not _all_atomicity_resolved(state):
        state["phase"] = "ATOMICITY_REVIEW"
        persist(state)
        return _phase_violation(state, {"ATOMICITY_REVIEW"}, "split_claim")

    active_map = {c["claim_id"]: c for c in _active_claims(state)}
    accepted_ids = {
        sid for sid, src in state.get("sources", {}).items() if src.get("screen_status") == "ACCEPTED"
    }
    accepted, rejected = [], []
    evidence_source_ids: set[str] = set()
    existing_keys = {
        (e["claim_id"], e["source_id"], _normalize_quote(e.get("quote", "")))
        for e in state.get("evidence", [])
    }
    for b in bindings:
        if b.claim_id not in active_map:
            rejected.append({"claim_id": b.claim_id, "source_id": b.source_id, "reason": "claim_not_active"})
            continue
        if b.source_id not in accepted_ids:
            rejected.append(
                {"claim_id": b.claim_id, "source_id": b.source_id, "reason": "source_not_accepted"}
            )
            continue
        matched, match_type = _quote_match(_get_source_content(state, b.source_id), b.quote)
        if not matched:
            rejected.append(
                {
                    "claim_id": b.claim_id,
                    "source_id": b.source_id,
                    "reason": match_type,
                    "quote": b.quote,
                }
            )
            continue
        key = (b.claim_id, b.source_id, _normalize_quote(b.quote))
        if key in existing_keys:
            existing = next(
                e
                for e in state.get("evidence", [])
                if (e["claim_id"], e["source_id"], _normalize_quote(e.get("quote", ""))) == key
            )
            accepted.append(
                {
                    "evidence_id": existing["evidence_id"],
                    "claim_id": b.claim_id,
                    "source_id": b.source_id,
                    "already_bound": True,
                }
            )
            evidence_source_ids.add(b.source_id)
            continue
        record = {
            "evidence_id": f"ev_{uuid.uuid4().hex[:12]}",
            "claim_id": b.claim_id,
            "source_id": b.source_id,
            "quote": b.quote,
            "quote_verified": True,
            "quote_match_type": match_type,
            "relation": "UNJUDGED",
            "strength": 0.0,
            "created_at": time.time(),
        }
        state["evidence"].append(record)
        existing_keys.add(key)
        accepted.append(
            {
                "evidence_id": record["evidence_id"],
                "claim_id": b.claim_id,
                "source_id": b.source_id,
                "already_bound": False,
            }
        )
        evidence_source_ids.add(b.source_id)

    if evidence_source_ids:
        await asyncio.gather(*(_enrich_source(state["sources"][sid]) for sid in evidence_source_ids))

    to_judge = []
    accepted_eids = {x["evidence_id"] for x in accepted}
    for ev in state.get("evidence", []):
        if ev["evidence_id"] not in accepted_eids or ev.get("relation") != "UNJUDGED":
            continue
        claim = active_map.get(ev["claim_id"], {})
        source = state.get("sources", {}).get(ev["source_id"], {})
        to_judge.append(
            {
                "evidence_id": ev["evidence_id"],
                "claim_id": ev["claim_id"],
                "claim_text": claim.get("text"),
                "source_id": ev["source_id"],
                "source_title": source.get("title"),
                "source_domain": source.get("domain"),
                "quote": ev["quote"],
            }
        )

    if to_judge:
        state["phase"] = "EVIDENCE_BINDING"
        next_action = _next(
            "judge_evidence",
            "Quote integrity passed; now make the bounded semantic entailment judgment.",
            [
                "Judge only whether the quoted passage entails/contradicts the claim.",
                "Use RELATED_BUT_INSUFFICIENT when it is topical but does not establish the claim.",
                "Do not infer facts beyond the quoted passage.",
            ],
            {"evidence": to_judge, "judgments": "List[EvidenceRelationJudgment]"},
            "Every newly bound evidence_id has a semantic relation judgment.",
        )
    else:
        next_action = _next(
            "bind_evidence",
            "No new quote-verified evidence is ready for judgment.",
            ["Use get_source_context to choose direct quotes, then bind them."],
        )

    persist(state)
    return ResearchToolResponse(
        status="EVIDENCE_QUOTES_VERIFIED" if accepted else "EVIDENCE_BINDING_FAILED",
        data={"bound": accepted, "rejected": rejected, "evidence_to_judge": to_judge},
        next_action=next_action,
        quality_gate={
            "quote_integrity": "PASS" if accepted and not rejected else ("PARTIAL" if accepted else "BLOCKED")
        },
        agent_rules=_agent_rules(),
    )


async def judge_evidence(research_id: str, judgments: list[EvidenceRelationJudgment]) -> ResearchToolResponse:
    """Assign bounded semantic relation labels to quote-verified evidence.

    The server owns evidence IDs and quote integrity. The client does exactly one
    semantic task here: classify the relationship between the displayed quote
    and its claim.
    """
    state = load_state(research_id)
    if state.get("phase") != "EVIDENCE_BINDING":
        return _phase_violation(state, {"EVIDENCE_BINDING"}, "judge_evidence")
    evidence_map = {e["evidence_id"]: e for e in state.get("evidence", [])}
    unknown = [j.evidence_id for j in judgments if j.evidence_id not in evidence_map]
    if unknown:
        raise ValueError(f"Unknown evidence IDs: {unknown}")

    updated = []
    for j in judgments:
        ev = evidence_map[j.evidence_id]
        if not ev.get("quote_verified"):
            raise ValueError(f"Evidence {j.evidence_id} does not have a verified quote")
        ev["relation"] = j.relation
        ev["strength"] = j.strength
        ev["judged_at"] = time.time()
        updated.append({"evidence_id": j.evidence_id, "relation": j.relation, "strength": j.strength})

    still_unjudged = [e["evidence_id"] for e in state.get("evidence", []) if e.get("relation") == "UNJUDGED"]
    if still_unjudged:
        next_action = _next(
            "judge_evidence",
            "Some bound evidence still lacks a semantic relation judgment.",
            ["Judge the remaining evidence IDs before assessment."],
            {"remaining_evidence_ids": still_unjudged},
        )
        status = "EVIDENCE_JUDGMENT_INCOMPLETE"
    else:
        state["phase"] = "ASSESSED"
        next_action = _next(
            "assess_claims",
            "All bound evidence has semantic judgments; run deterministic aggregation.",
            ["Do not self-assign claim confidence."],
        )
        status = "EVIDENCE_JUDGED"

    persist(state)
    return ResearchToolResponse(
        status=status,
        data={"updated": updated, "remaining_unjudged": still_unjudged},
        next_action=next_action,
        quality_gate={"entailment_judgment": "PASS" if not still_unjudged else "PENDING"},
        agent_rules=_agent_rules(),
    )


async def assess_claims(research_id: str) -> ResearchToolResponse:
    """Compute deterministic claim resolution and return a complete next-step packet."""
    state = load_state(research_id)
    if state.get("phase") not in {"ASSESSED", "EVIDENCE_BINDING"}:
        return _phase_violation(state, {"ASSESSED", "EVIDENCE_BINDING"}, "assess_claims")
    if state.get("phase") == "EVIDENCE_BINDING" and not state.get("evidence"):
        review_batches = _prepare_candidate_review_queue(state, per_claim=4)
        if state.get("candidate_review_queue"):
            state["phase"] = "CANDIDATE_REVIEW"
            persist(state)
            return ResearchToolResponse(
                status="INITIAL_CANDIDATE_REVIEW_REQUIRED",
                data={"review_batches": review_batches},
                next_action=_next(
                    "review_candidates",
                    "Claims need evidence; use the unified candidate review path.",
                    [
                        "Review the returned pairs directly; do not call get_research_state, bind_evidence, or judge_evidence."
                    ],
                    {"review_batches": review_batches},
                ),
                agent_rules=_agent_rules(),
            )
        state["phase"] = "ASSESSED"
        persist(state)
        return ResearchToolResponse(
            status="NO_INITIAL_EVIDENCE_CANDIDATES",
            data={},
            next_action=_next(
                "assess_claims",
                "No initial candidate survived; reassess to generate targeted research gaps.",
                [],
            ),
            agent_rules=_agent_rules(),
        )

    assessments = [assess_one_claim(state, c) for c in _active_claims(state)]
    state["last_assessments"] = [a.model_dump() for a in assessments]
    partition = _assessment_partition(assessments)
    open_claims = partition["unknown"] + partition["provisional"] + partition["contested"]

    claim_map = {c["claim_id"]: c for c in _active_claims(state)}
    open_details = []
    for a in open_claims:
        flags = a.metrics.get("quality_flags", [])
        if a.status == "CONTESTED":
            mode = "resolve_conflict"
        elif "disconfirmation_attempt_required" in flags:
            mode = "disconfirm"
        else:
            mode = "support"
        open_details.append(
            {
                "claim_id": a.claim_id,
                "claim_text": claim_map.get(a.claim_id, {}).get("text"),
                "status": a.status,
                "resolution_confidence": a.resolution_confidence,
                "quality_flags": flags,
                "suggested_mode": mode,
            }
        )

    if not open_claims:
        state["phase"] = "CLAIM_GRAPH_STABLE"
        if state.get("contested"):
            next_action = _next(
                "review_claim_tensions",
                "All active claims are strongly resolved; contested session requires one tension review.",
                ["Use only active claim IDs. An empty tension list is valid if there is no genuine tension."],
                {
                    "active_claims": [
                        {"claim_id": c["claim_id"], "text": c["text"]} for c in _active_claims(state)
                    ]
                },
            )
        else:
            next_action = _next(
                "verify_citations",
                "Claim graph is stable; the server can auto-build and audit citation coverage.",
                ["Call verify_citations with research_id only; citations are optional overrides."],
            )
        status = "CLAIM_GRAPH_STABLE"
    else:
        state["phase"] = "ASSESSED"
        next_action = _next(
            "research_unknowns",
            "Only the unresolved/provisional/contested claims need more retrieval.",
            [
                "Generate compact targeted queries only for data.open_claims.",
                "Do not research any claim listed in data.resolved.",
                "Use suggested_mode exactly unless there is a concrete reason to override it.",
            ],
            {"open_claims": open_details},
            "Every active claim is strongly resolved or the retrieval budget is exhausted.",
        )
        status = "MORE_RESEARCH_REQUIRED"

    persist(state)
    return ResearchToolResponse(
        status=status,
        data={
            "assessments": [a.model_dump() for a in assessments],
            "resolved": [a.claim_id for a in partition["resolved"]],
            "open_claims": open_details,
        },
        next_action=next_action,
        quality_gate={"claim_resolution": "PASS" if not open_claims else "BLOCKED"},
        agent_rules=_agent_rules(),
    )


async def research_unknowns(research_id: str, gaps: list[GapPlan]) -> ResearchToolResponse:
    """Run targeted retrieval only for claims that still need work, then return one compact review packet."""
    state = load_state(research_id)
    if state.get("phase") != "ASSESSED":
        return _phase_violation(state, {"ASSESSED"}, "assess_claims")

    assessment_map = {a.claim_id: a for a in [assess_one_claim(state, c) for c in _active_claims(state)]}
    actionable = []
    skipped = []
    active_ids = {c["claim_id"] for c in _active_claims(state)}
    for g in gaps:
        if g.claim_id not in active_ids:
            raise ValueError(f"Unknown or superseded claim_id {g.claim_id!r}")
        current = assessment_map.get(g.claim_id)
        if current and current.status in {"SUPPORTED", "CONTRADICTED"}:
            skipped.append({"claim_id": g.claim_id, "reason": "already_strongly_resolved"})
            continue
        actionable.append(g)

    state["metrics"]["early_stop_skips"] += len(skipped)
    if not actionable:
        unresolved_elsewhere = [
            a for a in assessment_map.values() if a.status not in {"SUPPORTED", "CONTRADICTED"}
        ]
        if unresolved_elsewhere:
            state["phase"] = "ASSESSED"
            persist(state)
            details = [
                {
                    "claim_id": a.claim_id,
                    "claim_text": _claim_by_id(state, a.claim_id)["text"],
                    "status": a.status,
                    "quality_flags": a.metrics.get("quality_flags", []),
                }
                for a in unresolved_elsewhere
            ]
            return ResearchToolResponse(
                status="NO_ACTIONABLE_GAPS_SUBMITTED",
                data={"skipped": skipped, "still_open": details},
                next_action=_next(
                    "research_unknowns",
                    "The submitted gaps were already resolved, but other active claims still need work.",
                    ["Generate gap plans only for data.still_open."],
                    {"still_open": details},
                ),
                agent_rules=_agent_rules(),
            )
        state["phase"] = "CLAIM_GRAPH_STABLE"
        persist(state)
        return ResearchToolResponse(
            status="EARLY_STOP_ALL_REQUESTED_CLAIMS_RESOLVED",
            data={"skipped": skipped},
            next_action=_next(
                "review_claim_tensions" if state.get("contested") else "verify_citations",
                "All active claims already satisfy strong resolution gates.",
                ["Do not perform more retrieval."],
            ),
            agent_rules=_agent_rules(),
        )

    budget = _budget_remaining(state)
    if budget["search_requests"] <= 0 or budget["fetch_urls"] <= 0:
        state["phase"] = "CLAIM_GRAPH_STABLE"
        persist(state)
        return ResearchToolResponse(
            status="RETRIEVAL_BUDGET_EXHAUSTED",
            data={"remaining_budget": budget, "open_claim_ids": [g.claim_id for g in actionable]},
            next_action=_next(
                "review_claim_tensions" if state.get("contested") else "verify_citations",
                "The mode-level retrieval budget is exhausted; preserve unresolved claims explicitly.",
                ["Do not bypass the global Search/Fetch budget."],
            ),
            agent_rules=_agent_rules(),
        )

    max_rounds = int(state.get("policy", {}).get("max_gap_rounds", 0))
    if state.get("gap_rounds", 0) >= max_rounds or not _consume_followup_round(
        state, reason="research_unknowns"
    ):
        state["phase"] = "CLAIM_GRAPH_STABLE"
        persist(state)
        return ResearchToolResponse(
            status="GAP_BUDGET_EXHAUSTED",
            data={"skipped": skipped, "gap_rounds": state.get("gap_rounds", 0), "max_gap_rounds": max_rounds},
            next_action=_next(
                "review_claim_tensions" if state.get("contested") else "verify_citations",
                "Gap budget is exhausted; unresolved claims are preserved as provisional/unknown.",
                ["Proceed without inventing closure."],
            ),
            agent_rules=_agent_rules(),
        )

    policy = _mode_policy(state)
    # Allocate the remaining global network budget across currently open claims
    # before launching parallel work. This prevents a single gap round from
    # overshooting FAST mode simply because many claims are unresolved at once.
    max_claims_by_search = max(1, budget["search_requests"])
    max_claims_by_fetch = max(1, budget["fetch_urls"])
    max_actionable = min(len(actionable), max_claims_by_search, max_claims_by_fetch)
    budget_deferred = actionable[max_actionable:]
    actionable = actionable[:max_actionable]
    per_gap_query_cap = max(
        1, min(int(policy["max_queries_per_gap"]), budget["search_requests"] // max(1, len(actionable)))
    )
    per_gap_fetch_cap = max(
        1, min(int(policy["max_fetch_per_retrieval"]), budget["fetch_urls"] // max(1, len(actionable)))
    )

    state["phase"] = "GAP_RESEARCH"
    state["gap_rounds"] = int(state.get("gap_rounds", 0)) + 1
    state["metrics"]["gap_rounds"] = state["gap_rounds"]

    async def run(g: GapPlan):
        claim = _claim_by_id(state, g.claim_id)
        current = assessment_map.get(g.claim_id)
        flags = set((current.metrics.get("quality_flags", []) if current else []))
        prior_work_ids = set((current.metrics.get("winning_work_ids", []) if current else []))
        independence_hint = ""
        if "insufficient_independent_works" in flags and prior_work_ids:
            independence_hint = f" | Independence requirement: find a DISTINCT underlying study/work, not another host or mirror of {sorted(prior_work_ids)}"
        objective = f"Claim: {claim['text']} | Gap goal: {g.reason} | Mode: {g.mode}{independence_hint}"
        spec = g.retrieval
        if not spec.purpose:
            spec.purpose = objective[:2000]
        queries = g.queries[:per_gap_query_cap]
        contexts, metrics = await _retrieve_candidates(
            queries,
            spec,
            state["executed_queries_set"],
            objective,
            fetch_top_n_per_query=policy["fetch_top_n_per_query"],
            max_fetch_per_retrieval=per_gap_fetch_cap,
        )
        return g, objective, contexts, metrics

    results = await asyncio.gather(*(run(g) for g in actionable), return_exceptions=True)
    review_batches, errors, queue = [], [], []
    for gap, result in zip(actionable, results):
        if isinstance(result, Exception):
            errors.append({"claim_id": gap.claim_id, "error": str(result)})
            continue
        g, objective, contexts, metrics = result
        _apply_metrics(state, metrics, include_wave=False)
        if g.mode in {"disconfirm", "resolve_conflict"}:
            _claim_by_id(state, g.claim_id)["disconfirmation_attempted"] = True
        ids = _register_candidates(state, contexts)
        current_assessment = assessment_map.get(g.claim_id)
        current_flags = set(
            (current_assessment.metrics.get("quality_flags", []) if current_assessment else [])
        )
        excluded_works = (
            set((current_assessment.metrics.get("winning_work_ids", []) if current_assessment else []))
            if "insufficient_independent_works" in current_flags
            else set()
        )
        shortlist = _shortlist_for_review(
            state,
            ids,
            objective,
            policy["gap_review_limit"],
            include_accepted=True,
            exclude_work_ids=excluded_works,
        )
        # If every returned URL was merely another copy of an already-used work,
        # expose that fact instead of pretending it is new independent evidence.
        if not shortlist and excluded_works:
            review_batches.append(
                {
                    "claim_id": g.claim_id,
                    "mode": g.mode,
                    "items": [],
                    "note": "retrieval_returned_only_existing_work_families",
                    "excluded_work_ids": sorted(excluded_works),
                }
            )
            continue
        items = []
        for ctx in shortlist:
            item = {"claim_id": g.claim_id, "claim_text": _claim_by_id(state, g.claim_id)["text"], **ctx}
            items.append(item)
            queue.append({"claim_id": g.claim_id, "source_id": ctx["source_id"], "objective": objective})
        review_batches.append({"claim_id": g.claim_id, "mode": g.mode, "items": items})

    if review_batches or errors:
        state["metrics"]["retrieval_waves"] = int(state["metrics"].get("retrieval_waves", 0)) + 1

    # Unique pair queue; the response itself is the complete work packet.
    dedup = {}
    for q in queue:
        dedup[(q["claim_id"], q["source_id"])] = q
    state["candidate_review_queue"] = list(dedup.values())
    state["metrics"]["sources_presented_to_client"] += len(state["candidate_review_queue"])

    if state["candidate_review_queue"]:
        state["phase"] = "CANDIDATE_REVIEW"
        next_action = _next(
            "review_candidates",
            "Review each returned claim/source pair once; this replaces screen_sources + bind_evidence + judge_evidence for gap rounds.",
            [
                "For each item choose RELEVANT/PARTIAL/IRRELEVANT.",
                "If useful, copy one direct quote from item.passages and assign the bounded evidence relation.",
                "Use RELATED_BUT_INSUFFICIENT when the passage is topical but does not establish the claim.",
            ],
            {"review_batches": review_batches},
            "Every pair in review_batches is reviewed or explicitly skipped.",
        )
        status = "GAP_REVIEW_REQUIRED"
    else:
        state["phase"] = "ASSESSED"
        next_action = _next(
            "assess_claims",
            "No viable candidates survived deterministic filtering.",
            ["Reassess; the server will decide whether more budget remains."],
        )
        status = "NO_VIABLE_GAP_CANDIDATES"

    persist(state)
    return ResearchToolResponse(
        status=status,
        data={
            "review_batches": review_batches,
            "skipped_resolved": skipped,
            "errors": errors,
            "budget_deferred_claim_ids": [g.claim_id for g in budget_deferred],
            "remaining_network_budget": _budget_remaining(state),
            "gap_round": state["gap_rounds"],
            "gap_rounds_remaining": max(0, max_rounds - state["gap_rounds"]),
        },
        next_action=next_action,
        agent_rules=_agent_rules(),
    )


async def review_candidates(research_id: str, reviews: list[CandidateEvidenceReview]) -> ResearchToolResponse:
    """Unified claim/source review: relevance + direct quote + semantic relation.

    Initial broad retrieval still uses screen_sources before claims exist. Once
    claim IDs exist, this is the preferred path for both initial evidence and
    targeted gap evidence; bind_evidence/judge_evidence remain low-level
    compatibility tools rather than the normal happy path.
    """
    state = load_state(research_id)
    if state.get("phase") != "CANDIDATE_REVIEW":
        return _phase_violation(state, {"CANDIDATE_REVIEW"}, "review_candidates")

    active = {c["claim_id"]: c for c in _active_claims(state)}
    queue = {(q["claim_id"], q["source_id"]): q for q in state.get("candidate_review_queue", [])}
    supplied = {(r.claim_id, r.source_id) for r in reviews}
    invalid = supplied - set(queue)
    if invalid:
        raise ValueError(f"Review pair(s) were not requested by the server: {sorted(invalid)}")

    accepted_sources = set()
    evidence_sources_to_enrich = set()
    created = []
    skipped = []
    quote_failures = []
    existing_keys = {
        (e["claim_id"], e["source_id"], _normalize_quote(e.get("quote", ""))): e
        for e in state.get("evidence", [])
    }

    for r in reviews:
        claim = active.get(r.claim_id)
        source = state.get("sources", {}).get(r.source_id)
        if not claim or not source:
            skipped.append(
                {"claim_id": r.claim_id, "source_id": r.source_id, "reason": "unknown_or_inactive"}
            )
            continue
        if r.verdict == "IRRELEVANT" or not source.get("mechanical_gate_passed"):
            skipped.append({"claim_id": r.claim_id, "source_id": r.source_id, "reason": r.reason})
            if source.get("screen_status") == "PENDING":
                source["screen_status"] = "DEFERRED"
            continue

        source["screen_status"] = "ACCEPTED"
        previous_verdict = source.get("client_screen_verdict")
        if previous_verdict != "RELEVANT":
            source["client_screen_verdict"] = r.verdict
        source["screen_reason"] = r.reason
        accepted_sources.add(r.source_id)

        if not r.quote:
            skipped.append({"claim_id": r.claim_id, "source_id": r.source_id, "reason": "no_quote_selected"})
            continue
        matched, match_type = _quote_match(_get_source_content(state, r.source_id), r.quote)
        if not matched:
            quote_failures.append(
                {
                    "claim_id": r.claim_id,
                    "source_id": r.source_id,
                    "reason": match_type,
                    "suggested_passages": _top_passages(
                        _get_source_content(state, r.source_id), claim["text"], limit=3, max_chars=900
                    ),
                }
            )
            continue
        relation = r.relation or "RELATED_BUT_INSUFFICIENT"
        key = (r.claim_id, r.source_id, _normalize_quote(r.quote))
        if key in existing_keys:
            ev = existing_keys[key]
            ev["relation"] = relation
            ev["strength"] = r.strength
            ev["source_relevance"] = 1.0 if r.verdict == "RELEVANT" else 0.60
            ev["judged_at"] = time.time()
        else:
            ev = {
                "evidence_id": f"ev_{uuid.uuid4().hex[:12]}",
                "claim_id": r.claim_id,
                "source_id": r.source_id,
                "quote": r.quote,
                "quote_verified": True,
                "quote_match_type": match_type,
                "relation": relation,
                "strength": r.strength,
                "source_relevance": 1.0 if r.verdict == "RELEVANT" else 0.60,
                "created_at": time.time(),
                "judged_at": time.time(),
            }
            state["evidence"].append(ev)
            existing_keys[key] = ev
        created.append(
            {
                "evidence_id": ev["evidence_id"],
                "claim_id": r.claim_id,
                "source_id": r.source_id,
                "relation": relation,
                "quote_match_type": match_type,
            }
        )
        if relation in {"SUPPORTS", "CONTRADICTS"}:
            evidence_sources_to_enrich.add(r.source_id)

    if evidence_sources_to_enrich:
        await asyncio.gather(*(_enrich_source(state["sources"][sid]) for sid in evidence_sources_to_enrich))

    remaining_pairs = [q for pair, q in queue.items() if pair not in supplied]
    state["candidate_review_queue"] = remaining_pairs
    state["metrics"]["candidate_reviews"] += len(reviews)

    if quote_failures:
        # Failed quote pairs remain in the queue so the client can correct them
        # using the passages returned in this response; no get_source_context is needed.
        for f in quote_failures:
            pair = (f["claim_id"], f["source_id"])
            if pair not in {(q["claim_id"], q["source_id"]) for q in remaining_pairs}:
                remaining_pairs.append(queue[pair])
        state["candidate_review_queue"] = remaining_pairs

    if remaining_pairs:
        state["phase"] = "CANDIDATE_REVIEW"
        next_action = _next(
            "review_candidates",
            "Some claim/source pairs still need review or quote correction.",
            ["Use data.quote_failures.suggested_passages for corrections; do not inspect server state."],
            {"remaining_pairs": remaining_pairs, "quote_failures": quote_failures},
        )
        status = "CANDIDATE_REVIEW_INCOMPLETE"
    else:
        state["phase"] = "ASSESSED"
        next_action = _next(
            "assess_claims",
            "Gap candidates were reviewed and evidence stored.",
            ["Run deterministic aggregation now."],
        )
        status = "CANDIDATE_REVIEW_COMPLETE"

    persist(state)
    return ResearchToolResponse(
        status=status,
        data={
            "evidence_created_or_updated": created,
            "skipped": skipped,
            "quote_failures": quote_failures,
            "remaining_pairs": remaining_pairs,
        },
        next_action=next_action,
        quality_gate={
            "server_quote_integrity": "PASS" if not quote_failures else "PARTIAL",
            "client_semantic_entailment": "RECORDED",
        },
        agent_rules=_agent_rules(),
    )


async def get_source_context(research_id: str, source_id: str, query: str = "") -> ResearchToolResponse:
    """Return a few relevant passages from persisted full source content.

    Use when the client needs a better direct quote without re-fetching the URL.
    """
    state = load_state(research_id)
    source = state.get("sources", {}).get(source_id)
    if not source:
        raise ValueError(f"Unknown source_id {source_id!r}")
    objective = query or " ".join(source.get("retrieval_objectives", [])) or state.get("topic", "")
    return ResearchToolResponse(
        status="SOURCE_CONTEXT",
        data={
            "source_id": source_id,
            "screen_status": source.get("screen_status"),
            "url": source.get("final_url") or source.get("url"),
            "title": source.get("title"),
            "passages": _top_passages(
                _get_source_content(state, source_id), objective, limit=5, max_chars=1200
            ),
        },
        agent_rules=_agent_rules(),
    )


async def review_claim_tensions(research_id: str, tensions: list[ClaimTension]) -> ResearchToolResponse:
    """Record cross-claim tensions after the claim graph is stable.

    Mandatory once for contested sessions. Tensions may describe genuine
    theoretical disagreement even when both claims accurately represent
    different positions.
    """
    state = load_state(research_id)
    if state.get("phase") != "CLAIM_GRAPH_STABLE":
        return _phase_violation(state, {"CLAIM_GRAPH_STABLE"}, "assess_claims")
    known = {c["claim_id"] for c in _active_claims(state)}
    for t in tensions:
        if t.claim_id_a not in known or t.claim_id_b not in known:
            raise ValueError("Tension claim IDs must reference active server-generated claims")
    state["claim_tensions"] = [t.model_dump() for t in tensions]
    state["tension_review_done"] = True
    state["phase"] = "TENSION_REVIEWED"
    persist(state)
    return ResearchToolResponse(
        status="TENSION_REVIEW_RECORDED",
        data={"tension_count": len(tensions)},
        next_action=_next(
            "verify_citations",
            "Tension gate passed; run citation coverage/integrity audit.",
            [
                "Call verify_citations with research_id only; the server auto-selects winning quote-verified evidence."
            ],
        ),
        quality_gate={"tension_review": "PASS"},
        agent_rules=_agent_rules(),
    )


async def verify_citations(
    research_id: str, citations: Optional[list[CitationCheck]] = None
) -> ResearchToolResponse:
    """Server-owned citation integrity + coverage audit.

    Normal use: pass research_id only. The server automatically selects the
    strongest quote-verified winning evidence for each strongly resolved claim.
    Optional citations are accepted only as explicit client overrides.
    """
    state = load_state(research_id)
    allowed = {"CLAIM_GRAPH_STABLE", "TENSION_REVIEWED", "CITATION_AUDIT"}
    if state.get("phase") not in allowed:
        return _phase_violation(state, allowed, "assess_claims")
    if state.get("contested") and not state.get("tension_review_done"):
        return ResearchToolResponse(
            status="TENSION_REVIEW_REQUIRED",
            data={},
            next_action=_next(
                "review_claim_tensions", "Contested sessions must review tensions before citation audit.", []
            ),
            agent_rules=_agent_rules(),
        )

    state["phase"] = "CITATION_AUDIT"
    assessments = [assess_one_claim(state, c) for c in _active_claims(state)]
    resolved = {a.claim_id: a for a in assessments if a.status in {"SUPPORTED", "CONTRADICTED"}}
    evidence_by_claim: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in state.get("evidence", []):
        evidence_by_claim[e["claim_id"]].append(e)

    selected: list[CitationCheck] = []
    if citations:
        selected = citations
    else:
        for claim_id, assessment in resolved.items():
            winning_relation = "SUPPORTS" if assessment.status == "SUPPORTED" else "CONTRADICTS"
            candidates = []
            for e in evidence_by_claim.get(claim_id, []):
                if not e.get("quote_verified") or e.get("relation") != winning_relation:
                    continue
                source = state.get("sources", {}).get(e["source_id"], {})
                if source.get("screen_status") != "ACCEPTED":
                    continue
                if float(source.get("work_identity_confidence", 0.35)) < 0.70:
                    continue
                claim = _claim_by_id(state, claim_id)
                candidates.append((_evidence_weight(claim, e, source), e))
            if candidates:
                candidates.sort(key=lambda x: x[0], reverse=True)
                used_works = set()
                for _, e in candidates:
                    source = state.get("sources", {}).get(e["source_id"], {})
                    work_id = _work_id_for_source(source)
                    if work_id in used_works:
                        continue
                    selected.append(
                        CitationCheck(claim_id=claim_id, source_id=e["source_id"], quote=e["quote"])
                    )
                    used_works.add(work_id)
                    if len(used_works) >= MIN_CITATION_WORKS_PER_CLAIM:
                        break

    verified, failed = [], []
    covered_work_ids: dict[str, set[str]] = defaultdict(set)
    for citation in selected:
        assessment = resolved.get(citation.claim_id)
        if not assessment:
            failed.append({**citation.model_dump(), "reason": "claim_not_strongly_resolved"})
            continue
        source = state.get("sources", {}).get(citation.source_id)
        if not source or source.get("screen_status") != "ACCEPTED":
            failed.append({**citation.model_dump(), "reason": "source_not_accepted"})
            continue
        matched, match_type = _quote_match(_get_source_content(state, citation.source_id), citation.quote)
        if not matched:
            failed.append({**citation.model_dump(), "reason": match_type})
            continue
        winning_relation = "SUPPORTS" if assessment.status == "SUPPORTED" else "CONTRADICTS"
        matching = [
            e
            for e in evidence_by_claim.get(citation.claim_id, [])
            if e.get("source_id") == citation.source_id
            and e.get("quote_verified")
            and e.get("relation") == winning_relation
            and _normalize_quote(e.get("quote", "")) == _normalize_quote(citation.quote)
        ]
        if not matching:
            failed.append({**citation.model_dump(), "reason": "citation_not_bound_as_winning_evidence"})
            continue
        work_id = _work_id_for_source(source)
        verified.append(
            {
                "claim_id": citation.claim_id,
                "source_id": citation.source_id,
                "work_id": work_id,
                "host_domain": source.get("host_domain") or source.get("registered_domain"),
                "source_role": source.get("source_role"),
                "publication_venue": source.get("publication_venue") or source.get("venue"),
                "work_identity_confidence": source.get("work_identity_confidence"),
                "work_identity_type": source.get("work_identity_type"),
                "quote": citation.quote,
                "match_type": match_type,
                "content_hash": source.get("content_hash"),
                "verified_at": time.time(),
            }
        )
        if float(source.get("work_identity_confidence", 0.35)) >= 0.70:
            covered_work_ids[citation.claim_id].add(work_id)

    missing = sorted(
        claim_id
        for claim_id in resolved
        if len(covered_work_ids.get(claim_id, set())) < MIN_CITATION_WORKS_PER_CLAIM
    )
    state["verified_citations"] = verified
    state["citation_audit"] = {
        "verified_count": len(verified),
        "failed": failed,
        "missing_claim_ids": missing,
        "independent_work_coverage": {k: len(v) for k, v in covered_work_ids.items()},
        "required_independent_works_per_claim": MIN_CITATION_WORKS_PER_CLAIM,
        "all_verified_and_covered": not failed and not missing,
        "checked_at": time.time(),
        "selection_mode": "client_override" if citations else "server_auto",
    }

    if failed or missing:
        state["phase"] = "CITATION_AUDIT"
        status = "CITATION_AUDIT_FAILED"
        next_action = _next(
            "verify_citations",
            "Automatic citation coverage could not close every resolved claim.",
            [
                "Only if needed, provide explicit CitationCheck overrides for the failed/missing claim IDs.",
                "Use get_source_context only for those specific failures.",
            ],
            {"failed": failed, "missing_claim_ids": missing},
        )
    else:
        state["phase"] = "READY_TO_FINALIZE"
        status = "ALL_CITATIONS_VERIFIED_AND_COVERED"
        next_action = _next(
            "finalize_research",
            "All hard quality gates passed.",
            ["Finalize once; synthesis must obey the returned manifest."],
        )

    persist(state)
    return ResearchToolResponse(
        status=status,
        data={
            "verified_citations": verified,
            "failed_citations": failed,
            "missing_claim_ids": missing,
            "independent_work_coverage": state["citation_audit"]["independent_work_coverage"],
            "required_independent_works_per_claim": MIN_CITATION_WORKS_PER_CLAIM,
            "selection_mode": state["citation_audit"]["selection_mode"],
        },
        next_action=next_action,
        quality_gate={
            "server_quote_integrity": "PASS" if not failed else "BLOCKED",
            "citation_coverage": "PASS" if not missing else "BLOCKED",
            "citation_work_independence": "PASS" if not missing else "BLOCKED",
            "client_semantic_entailment": "PREVIOUSLY_RECORDED",
        },
        agent_rules=_agent_rules(),
    )


async def finalize_research(research_id: str) -> ResearchToolResponse:
    """Finalize into an auditable synthesis manifest; do not generate prose inside the MCP."""
    state = load_state(research_id)
    if state.get("phase") != "READY_TO_FINALIZE":
        expected = "verify_citations"
        if (
            state.get("contested")
            and not state.get("tension_review_done")
            and state.get("phase") == "CLAIM_GRAPH_STABLE"
        ):
            expected = "review_claim_tensions"
        return _phase_violation(state, {"READY_TO_FINALIZE"}, expected)

    assessments = [assess_one_claim(state, c) for c in _active_claims(state)]
    amap = {a.claim_id: a for a in assessments}
    citations_by_claim: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for c in state.get("verified_citations", []):
        source = state.get("sources", {}).get(c["source_id"], {})
        citations_by_claim[c["claim_id"]].append(
            {
                "source_id": c["source_id"],
                "work_id": c.get("work_id") or source.get("work_id"),
                "url": source.get("final_url") or source.get("url"),
                "title": source.get("title"),
                "domain_type": source.get("domain_type"),
                "host_domain": source.get("host_domain") or source.get("registered_domain"),
                "source_role": source.get("source_role"),
                "publication_status": source.get("publication_status"),
                "publication_venue": source.get("publication_venue") or source.get("venue"),
                "work_identity_type": source.get("work_identity_type"),
                "work_identity_confidence": source.get("work_identity_confidence"),
                "quote": c["quote"],
            }
        )

    resolved, unresolved = [], []
    for claim in _active_claims(state):
        a = amap[claim["claim_id"]]
        item = {
            "claim_id": claim["claim_id"],
            "text": claim["text"],
            "status": a.status,
            "stance_confidence": a.stance_confidence,
            "evidence_quality": a.evidence_quality,
            "resolution_confidence": a.resolution_confidence,
            "metrics": a.metrics,
            "citations": citations_by_claim.get(claim["claim_id"], []),
            "synthesis_class": "DIRECTLY_SUPPORTED"
            if a.status in {"SUPPORTED", "CONTRADICTED"}
            else "UNRESOLVED_OR_PROVISIONAL",
        }
        (resolved if a.status in {"SUPPORTED", "CONTRADICTED"} else unresolved).append(item)

    accepted_sources = [s for s in state.get("sources", {}).values() if s.get("screen_status") == "ACCEPTED"]
    accepted_work_ids = {_work_id_for_source(s) for s in accepted_sources}
    source_roles: dict[str, int] = defaultdict(int)
    publication_statuses: dict[str, int] = defaultdict(int)
    for source in accepted_sources:
        source_roles[source.get("source_role") or "unknown"] += 1
        publication_statuses[source.get("publication_status") or "UNKNOWN"] += 1
    source_summary = {
        "accepted_source_copies": len(accepted_sources),
        "unique_work_families": len(accepted_work_ids),
        "identity_confident_work_families": len(
            {
                _work_id_for_source(s)
                for s in accepted_sources
                if float(s.get("work_identity_confidence", 0.35)) >= 0.70
            }
        ),
        "research_paper_retrieval_class": sum(
            1 for s in accepted_sources if s.get("domain_type") == "research_paper"
        ),
        "news_retrieval_class": sum(1 for s in accepted_sources if s.get("domain_type") == "news"),
        "web_retrieval_class": sum(1 for s in accepted_sources if s.get("domain_type") == "web"),
        "full_fetch": sum(
            1 for s in accepted_sources if s.get("content_origin") == "fetch" and not s.get("fetch_failed")
        ),
        "unique_host_domains": len(
            {
                s.get("host_domain") or s.get("registered_domain")
                for s in accepted_sources
                if s.get("host_domain") or s.get("registered_domain")
            }
        ),
        "source_roles": dict(sorted(source_roles.items())),
        "publication_statuses": dict(sorted(publication_statuses.items())),
        "note": "Source copies/hosts are not independent evidence. Independence is counted by identity-confident work_id families. Publication status is conservative and does not certify peer review unless externally established.",
    }

    manifest = {
        "no_new_factual_claims": True,
        "verified_claim_ids": [x["claim_id"] for x in resolved],
        "provisional_or_unresolved_claim_ids": [x["claim_id"] for x in unresolved],
        "factual_statement_rule": "Every factual statement in the final answer must map to a returned claim_id. Do not invent numerical thresholds, counts, dates, or causal claims.",
        "synthesis_classes": {
            "DIRECTLY_SUPPORTED": {
                "rule": "Externally factual statement directly mapped to one resolved claim_id and its audited citations.",
                "may_introduce_new_numbers_dates_or_causal_facts": False,
            },
            "DERIVED_INFERENCE": {
                "rule": "Reasoning derived only from returned resolved claim_ids. Label explicitly as inference/analysis and list basis_claim_ids when material.",
                "may_introduce_new_external_facts": False,
            },
            "SPECULATIVE_RECOMMENDATION": {
                "rule": "Actionable recommendation or hypothesis not itself empirically established by the claim graph. Label explicitly as recommendation/speculation.",
                "may_be_presented_as_verified_fact": False,
            },
        },
        "inference_rule": "New reasoning is allowed only as DERIVED_INFERENCE with explicit basis claim IDs; it must not be presented as externally verified fact.",
        "recommendation_rule": "Engineering or policy advice not directly established by a verified claim must be labeled SPECULATIVE_RECOMMENDATION. Do not invent quantitative thresholds unless a resolved claim contains them.",
        "citation_rule": "Use only citations attached to resolved_claims for DIRECTLY_SUPPORTED factual assertions. Distinct URLs from one work_id are not independent citations.",
        "tension_rule": "Recorded tension resolutions are DERIVED_INFERENCE from the client and must be labeled as analysis/inference unless separately represented by verified claims.",
        "audit_language": {
            "quote_integrity": "server-deterministic",
            "evidence_entailment": "client-semantic judgment",
            "confidence": "server-computed",
            "citation_coverage": "server-audited",
            "evidence_independence": "server-audited at underlying work_id level",
            "host_provenance": "separate from publication provenance",
        },
        "server_metrics": {**state.get("metrics", {}), "elapsed_ms": _elapsed_ms(state)},
        "source_summary": source_summary,
    }

    state["phase"] = "COMPLETE"
    persist(state)
    status = "COMPLETE" if not unresolved else "COMPLETE_WITH_UNRESOLVED"
    return ResearchToolResponse(
        status=status,
        data={
            "topic": state.get("topic"),
            "mode": state.get("mode"),
            "resolved_claims": resolved,
            "unresolved_claims": unresolved,
            "tensions": [
                dict(t, synthesis_class="DERIVED_INFERENCE") for t in state.get("claim_tensions", [])
            ],
            "superseded_claim_ids": state.get("superseded_claim_ids", []),
            "quality_summary": _research_quality_summary(state),
            "synthesis_manifest": manifest,
        },
        quality_gate={
            "source_screening": "PASS",
            "atomicity": "PASS",
            "claim_resolution": "PASS" if not unresolved else "PARTIAL_BUDGET_LIMITED",
            "tension_review": "PASS"
            if (not state.get("contested") or state.get("tension_review_done"))
            else "BLOCKED",
            "server_quote_integrity": "PASS",
            "client_semantic_entailment": "RECORDED",
            "citation_coverage": "PASS",
            "citation_work_independence": "PASS",
        },
        agent_rules=_agent_rules(),
    )


async def get_research_state(research_id: str) -> ResearchToolResponse:
    """Debug/inspection tool. Returns compact protocol state, not full source text."""
    state = load_state(research_id)
    return ResearchToolResponse(
        status="OK",
        data={
            "research_id": research_id,
            "topic": state.get("topic"),
            "mode": state.get("mode"),
            "phase": state.get("phase"),
            "contested": state.get("contested"),
            "gap_rounds": state.get("gap_rounds"),
            "max_gap_rounds": state.get("policy", {}).get("max_gap_rounds"),
            "followup_rounds": state.get("followup_rounds"),
            "max_followup_rounds": state.get("policy", {}).get("max_followup_rounds"),
            "active_claims": [
                {
                    "claim_id": c["claim_id"],
                    "text": c["text"],
                    "atomicity_status": c.get("atomicity_status"),
                    "disconfirmation_attempted": c.get("disconfirmation_attempted"),
                }
                for c in _active_claims(state)
            ],
            "source_counts": {
                "total_source_copies": len(state.get("sources", {})),
                "accepted_source_copies": sum(
                    1 for s in state.get("sources", {}).values() if s.get("screen_status") == "ACCEPTED"
                ),
                "accepted_work_families": len(
                    {
                        _work_id_for_source(s)
                        for s in state.get("sources", {}).values()
                        if s.get("screen_status") == "ACCEPTED"
                    }
                ),
                "rejected": sum(
                    1
                    for s in state.get("sources", {}).values()
                    if s.get("screen_status") in {"REJECTED", "AUTO_REJECTED"}
                ),
                "pending": sum(
                    1 for s in state.get("sources", {}).values() if s.get("screen_status") == "PENDING"
                ),
                "deferred": sum(
                    1 for s in state.get("sources", {}).values() if s.get("screen_status") == "DEFERRED"
                ),
            },
            "debug_note": "get_research_state is inspection-only; normal execution should follow the previous tool's next_action without calling this tool.",
            "metrics": state.get("metrics", {}),
            "citation_audit": state.get("citation_audit"),
            "elapsed_ms": _elapsed_ms(state),
        },
        agent_rules=_agent_rules(),
    )


async def check_server_config() -> ResearchToolResponse:
    """Check API key, DNS, and live TinyFish Search/Fetch endpoint health."""
    problems: list[str] = []
    if not TINYFISH_API_KEY:
        problems.append("TINYFISH_API_KEY is not set")
    for label, url in (("search", TINYFISH_SEARCH_URL), ("fetch", TINYFISH_FETCH_URL)):
        host = _domain_of(url)
        try:
            socket.getaddrinfo(host, 443)
        except socket.gaierror:
            problems.append(f"{label} endpoint host does not resolve: {host}")
    if not problems:
        try:
            sr = await get_http_client().get(
                TINYFISH_SEARCH_URL,
                params={"query": "tinyfish config probe", "language": "en", "domain_type": "web"},
                headers={"X-API-Key": TINYFISH_API_KEY},
            )
            if sr.status_code in {401, 403, 404, 405}:
                problems.append(f"Search probe returned {sr.status_code}")
        except Exception as exc:
            problems.append(f"Search probe failed: {exc}")
        try:
            fr = await get_http_client().post(
                TINYFISH_FETCH_URL,
                json={
                    "urls": ["https://www.tinyfish.ai/"],
                    "format": "markdown",
                    "per_url_timeout_ms": 12000,
                },
                headers={"X-API-Key": TINYFISH_API_KEY},
            )
            if fr.status_code in {401, 403, 404, 405}:
                problems.append(f"Fetch probe returned {fr.status_code}")
        except Exception as exc:
            problems.append(f"Fetch probe failed: {exc}")
    return ResearchToolResponse(
        status="CONFIG_OK" if not problems else "CONFIG_PROBLEM",
        data={
            "problems": problems,
            "search_url": TINYFISH_SEARCH_URL,
            "fetch_url": TINYFISH_FETCH_URL,
            "search_concurrency": MAX_INFLIGHT_SEARCH,
            "fetch_concurrency": MAX_INFLIGHT_FETCH,
            "crossref_checks": ENABLE_CROSSREF_CHECKS,
            "openalex_checks": ENABLE_OPENALEX_CHECKS,
            "minimum_independent_works": MIN_INDEPENDENT_WORKS,
            "minimum_citation_works_per_claim": MIN_CITATION_WORKS_PER_CLAIM,
            "protocol_version": PROTOCOL_VERSION,
            "default_mode": ResearchMode.FAST.value,
            "default_mode_policy": MODE_POLICIES[ResearchMode.FAST.value],
        },
        agent_rules=_agent_rules(),
    )
