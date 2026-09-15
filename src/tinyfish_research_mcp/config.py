"""Runtime configuration for TinyFish Guided Research MCP."""

from __future__ import annotations

import os
from enum import Enum
from typing import Any

PROTOCOL_VERSION = "7.2"
PACKAGE_VERSION = "1.0.0"

DB_PATH = os.environ.get("RESEARCH_DB_PATH", "research_state.db")
TINYFISH_API_KEY = os.environ.get("TINYFISH_API_KEY", "")
TINYFISH_SEARCH_URL = os.environ.get("TINYFISH_SEARCH_URL", "https://api.search.tinyfish.ai")
TINYFISH_FETCH_URL = os.environ.get("TINYFISH_FETCH_URL", "https://api.fetch.tinyfish.ai")

SEARCH_REQUESTS_PER_MINUTE = float(os.environ.get("TINYFISH_SEARCH_RPM", "30"))
FETCH_URLS_PER_MINUTE = float(os.environ.get("TINYFISH_FETCH_URLS_RPM", "150"))
MAX_INFLIGHT_SEARCH = int(os.environ.get("TINYFISH_SEARCH_CONCURRENCY", "8"))
MAX_INFLIGHT_FETCH = int(os.environ.get("TINYFISH_FETCH_CONCURRENCY", "6"))
MAX_RETRIES = int(os.environ.get("TINYFISH_MAX_RETRIES", "4"))
BASE_BACKOFF_SECONDS = float(os.environ.get("TINYFISH_BASE_BACKOFF", "0.4"))
RATE_COOLDOWN_FACTOR = 0.5
RATE_COOLDOWN_SECONDS = 20.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

FETCH_BATCH_SIZE = 10
FETCH_TOP_N_PER_QUERY = int(os.environ.get("FETCH_TOP_N_PER_QUERY", "3"))
MAX_FETCH_PER_RETRIEVAL = int(os.environ.get("MAX_FETCH_PER_RETRIEVAL", "24"))
MAX_PARALLEL_SUBAGENTS = int(os.environ.get("MAX_PARALLEL_SUBAGENTS", "12"))
MAX_CANDIDATE_CONTEXTS = int(os.environ.get("MAX_CANDIDATE_CONTEXTS", "10"))
MAX_ATOMICITY_SPLIT_DEPTH = int(os.environ.get("MAX_ATOMICITY_SPLIT_DEPTH", "3"))

RRF_K = 60.0
SOURCE_MECHANICAL_ACCEPT_THRESHOLD = 0.30
MIN_QUOTE_CHARS = 18
MIN_INDEPENDENT_WORKS = int(os.environ.get("MIN_INDEPENDENT_WORKS", "2"))
MIN_CITATION_WORKS_PER_CLAIM = int(os.environ.get("MIN_CITATION_WORKS_PER_CLAIM", str(MIN_INDEPENDENT_WORKS)))
MIN_EVIDENCE_QUALITY = 0.78
MIN_STANCE_CONFIDENCE = 0.60
CONTESTED_MARGIN_THRESHOLD = 0.15
MIN_DISSENTING_WORKS_FOR_OVERRIDE = 3
CURRENT_EVIDENCE_HALF_LIFE_DAYS = 180.0
MIN_AUTHORITY_FOR_STRONG_RESOLUTION = 0.50

ENABLE_CROSSREF_CHECKS = os.environ.get("ENABLE_CROSSREF_CHECKS", "1") == "1"
ENABLE_OPENALEX_CHECKS = os.environ.get("ENABLE_OPENALEX_CHECKS", "0") == "1"
CROSSREF_API_BASE = "https://api.crossref.org/works"
OPENALEX_API_BASE = "https://api.openalex.org"
EXTERNAL_CHECK_TIMEOUT = 7.0

TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "dclid",
    "msclkid",
    "mc_cid",
    "mc_eid",
    "igshid",
    "ref",
    "ref_src",
    "source",
    "campaign",
    "campaign_id",
}
SOCIAL_DOMAINS = {
    "facebook.com",
    "reddit.com",
    "x.com",
    "twitter.com",
    "tiktok.com",
    "instagram.com",
    "quora.com",
}
SEARCH_INTERMEDIARY_PATTERNS = (
    "scholar.google.",
    "google.com/search",
    "bing.com/search",
    "duckduckgo.com",
)
GENERIC_HOME_SIGNALS = (
    "welcome to",
    "home page",
    "find a journal",
    "sign in | create an account",
    "explore our questions",
    "all research fields",
    "research topics bring together",
)
STOPWORDS = {
    "the",
    "a",
    "an",
    "of",
    "and",
    "or",
    "in",
    "on",
    "for",
    "to",
    "is",
    "are",
    "was",
    "were",
    "be",
    "with",
    "from",
    "that",
    "this",
    "what",
    "how",
    "why",
    "when",
    "where",
    "who",
    "which",
    "into",
    "about",
    "does",
    "do",
    "can",
    "could",
    "would",
    "should",
    "their",
    "its",
    "than",
    "as",
}


class ResearchMode(str, Enum):
    FAST = "FAST"
    BALANCED = "BALANCED"
    EXHAUSTIVE = "EXHAUSTIVE"


MODE_POLICIES: dict[str, dict[str, int]] = {
    ResearchMode.FAST.value: {
        "fetch_top_n_per_query": 2,
        "max_fetch_per_retrieval": 10,
        "initial_review_limit_per_task": 4,
        "gap_review_limit": 3,
        "max_queries_per_gap": 2,
        "max_initial_queries_per_task": 2,
        "max_parallel_tasks": 4,
        "max_search_requests": 24,
        "max_fetch_urls": 48,
        "max_default_rounds": 2,
    },
    ResearchMode.BALANCED.value: {
        "fetch_top_n_per_query": 3,
        "max_fetch_per_retrieval": 16,
        "initial_review_limit_per_task": 5,
        "gap_review_limit": 5,
        "max_queries_per_gap": 3,
        "max_initial_queries_per_task": 3,
        "max_parallel_tasks": 6,
        "max_search_requests": 60,
        "max_fetch_urls": 120,
        "max_default_rounds": 4,
    },
    ResearchMode.EXHAUSTIVE.value: {
        "fetch_top_n_per_query": 4,
        "max_fetch_per_retrieval": 24,
        "initial_review_limit_per_task": 8,
        "gap_review_limit": 8,
        "max_queries_per_gap": 5,
        "max_initial_queries_per_task": 5,
        "max_parallel_tasks": 10,
        "max_search_requests": 140,
        "max_fetch_urls": 280,
        "max_default_rounds": 6,
    },
}


def mode_policy(state: dict[str, Any]) -> dict[str, int]:
    mode = state.get("mode", ResearchMode.FAST.value)
    return MODE_POLICIES.get(mode, MODE_POLICIES[ResearchMode.FAST.value])
