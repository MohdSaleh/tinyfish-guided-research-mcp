"""External provider clients: TinyFish Search/Fetch, Crossref and OpenAlex."""

from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Literal, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

from .config import (
    BASE_BACKOFF_SECONDS,
    CROSSREF_API_BASE,
    ENABLE_CROSSREF_CHECKS,
    ENABLE_OPENALEX_CHECKS,
    EXTERNAL_CHECK_TIMEOUT,
    FETCH_BATCH_SIZE,
    FETCH_URLS_PER_MINUTE,
    MAX_INFLIGHT_FETCH,
    MAX_INFLIGHT_SEARCH,
    MAX_RETRIES,
    OPENALEX_API_BASE,
    RATE_COOLDOWN_FACTOR,
    RATE_COOLDOWN_SECONDS,
    RETRYABLE_STATUS,
    SEARCH_REQUESTS_PER_MINUTE,
    TINYFISH_API_KEY,
    TINYFISH_FETCH_URL,
    TINYFISH_SEARCH_URL,
    TRACKING_PARAMS,
)
from .models import RetrievalSpec
from .observability import log_event, span


class TokenBucket:
    def __init__(self, rate_per_minute: float):
        self.rate_per_second = rate_per_minute / 60.0
        self.capacity = max(1.0, rate_per_minute)
        self.tokens = self.capacity
        self.last_refill = time.monotonic()
        self.lock = asyncio.Lock()
        self.cooldown_until = 0.0
        self.cooldown_factor = 1.0

    def _refill(self) -> None:
        now = time.monotonic()
        if now >= self.cooldown_until:
            self.cooldown_factor = 1.0
        elapsed = now - self.last_refill
        rate = self.rate_per_second * self.cooldown_factor
        self.tokens = min(self.capacity, self.tokens + elapsed * rate)
        self.last_refill = now

    async def acquire(self, cost: float = 1.0) -> None:
        while True:
            async with self.lock:
                self._refill()
                if self.tokens >= cost:
                    self.tokens -= cost
                    return
                rate = max(self.rate_per_second * self.cooldown_factor, 1e-6)
                wait_time = (cost - self.tokens) / rate
            await asyncio.sleep(min(wait_time, 2.0))

    def trigger_cooldown(self) -> None:
        self.cooldown_factor = RATE_COOLDOWN_FACTOR
        self.cooldown_until = time.monotonic() + RATE_COOLDOWN_SECONDS


_SEARCH_BUCKET = TokenBucket(SEARCH_REQUESTS_PER_MINUTE)
_FETCH_BUCKET = TokenBucket(FETCH_URLS_PER_MINUTE)
_SEARCH_SEMAPHORE = asyncio.Semaphore(MAX_INFLIGHT_SEARCH)
_FETCH_SEMAPHORE = asyncio.Semaphore(MAX_INFLIGHT_FETCH)
_HTTP_CLIENT: Optional[httpx.AsyncClient] = None


def get_http_client() -> httpx.AsyncClient:
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None:
        _HTTP_CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(25.0, connect=8.0),
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=24),
            follow_redirects=True,
        )
    return _HTTP_CLIENT


async def _request_with_retry(
    method: str,
    url: str,
    *,
    pool: Literal["search", "fetch", "external"],
    cost: float = 1.0,
    **kwargs: Any,
) -> httpx.Response:
    client = get_http_client()
    last_exc: Optional[Exception] = None
    last_resp: Optional[httpx.Response] = None

    for attempt in range(MAX_RETRIES):
        try:
            if pool == "search":
                await _SEARCH_BUCKET.acquire(cost)
                sem = _SEARCH_SEMAPHORE
            elif pool == "fetch":
                await _FETCH_BUCKET.acquire(cost)
                sem = _FETCH_SEMAPHORE
            else:
                sem = asyncio.Semaphore(1)

            async with sem:
                resp = await client.request(method, url, **kwargs)
            last_resp = resp

            if resp.status_code in RETRYABLE_STATUS:
                log_event("provider_retry", attempt=attempt + 1, error_type=f"http_{resp.status_code}")
                if resp.status_code == 429:
                    _SEARCH_BUCKET.trigger_cooldown()
                    _FETCH_BUCKET.trigger_cooldown()
                await asyncio.sleep(BASE_BACKOFF_SECONDS * (2**attempt))
                continue
            return resp
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as exc:
            last_exc = exc
            await asyncio.sleep(BASE_BACKOFF_SECONDS * (2**attempt))

    if last_resp is not None:
        return last_resp
    if last_exc is not None:
        raise RuntimeError(f"Request to {url!r} failed after retries: {last_exc}") from last_exc
    raise RuntimeError(f"Request to {url!r} failed with no response")


def _domain_of(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower().removeprefix("www.")
    except Exception:
        return ""


def _registrable_domain(domain: str) -> str:
    parts = [p for p in domain.lower().split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    common_second = {"co", "com", "org", "net", "gov", "ac", "edu"}
    if len(parts[-1]) == 2 and parts[-2] in common_second and len(parts) >= 3:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _canonicalize_url(url: str) -> str:
    try:
        p = urlsplit(url)
        scheme = (p.scheme or "https").lower()
        host = (p.hostname or "").lower().removeprefix("www.")
        port = p.port
        netloc = f"{host}:{port}" if port and port not in (80, 443) else host
        path = re.sub(r"/{2,}", "/", p.path or "/")
        if path != "/":
            path = path.rstrip("/")
        kept = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            kl = k.lower()
            if kl.startswith("utm_") or kl in TRACKING_PARAMS:
                continue
            kept.append((k, v))
        query = urlencode(kept, doseq=True)
        return urlunsplit((scheme, netloc, path, query, ""))
    except Exception:
        return url


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


async def _do_search(query: str, spec: RetrievalSpec) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"query": query, "language": spec.language, "domain_type": spec.domain_type}
    if spec.purpose:
        params["purpose"] = spec.purpose[:2000]
    if spec.location:
        params["location"] = spec.location
    if spec.include_domains:
        params["include_domains"] = ",".join(spec.include_domains)
    if spec.exclude_domains:
        params["exclude_domains"] = ",".join(spec.exclude_domains)
    if spec.recency_minutes is not None:
        params["recency_minutes"] = spec.recency_minutes
    if spec.after_date:
        params["after_date"] = spec.after_date
    if spec.before_date:
        params["before_date"] = spec.before_date
    if spec.pub_year_min is not None:
        params["pub_year_min"] = spec.pub_year_min
    if spec.pub_year_max is not None:
        params["pub_year_max"] = spec.pub_year_max

    with span("tinyfish.search", query=query[:200], domain_type=spec.domain_type):
        resp = await _request_with_retry(
            "GET",
            TINYFISH_SEARCH_URL,
            pool="search",
            cost=1.0,
            params=params,
            headers={"X-API-Key": TINYFISH_API_KEY},
        )
    if resp.status_code != 200:
        raise RuntimeError(f"TinyFish Search {resp.status_code}: {resp.text[:300]}")
    payload = resp.json()
    out = []
    for r in payload.get("results", []):
        out.append(
            {
                "query": query,
                "position": r.get("position"),
                "url": r.get("url"),
                "title": r.get("title"),
                "snippet": r.get("snippet") or "",
                "date": r.get("date"),
                "publisher": r.get("publisher"),
                "authors": r.get("authors") or [],
                "venue": r.get("venue"),
                "year": r.get("year"),
                "cited_by_count": r.get("cited_by_count"),
                "pdf_url": r.get("pdf_url"),
                "doi": r.get("doi"),
                "openalex_id": r.get("openalex_id") or r.get("openalex"),
                "domain_type": spec.domain_type,
            }
        )
    return out


async def _do_fetch_batch(urls: list[str], purpose: str) -> dict[str, dict[str, Any]]:
    body: dict[str, Any] = {
        "urls": urls,
        "format": "markdown",
        "per_url_timeout_ms": 30000,
    }
    if purpose.strip():
        body["purpose"] = purpose[:2000]

    with span("tinyfish.fetch", url_count=len(urls)):
        resp = await _request_with_retry(
            "POST",
            TINYFISH_FETCH_URL,
            pool="fetch",
            cost=float(len(urls)),
            json=body,
            headers={"X-API-Key": TINYFISH_API_KEY, "Content-Type": "application/json"},
        )
    if resp.status_code != 200:
        raise RuntimeError(f"TinyFish Fetch {resp.status_code}: {resp.text[:300]}")

    payload = resp.json()
    out: dict[str, dict[str, Any]] = {}
    for r in payload.get("results", []):
        requested = r.get("url")
        if not requested:
            continue
        out[requested] = {
            "fetch_url": requested,
            "final_url": r.get("final_url") or requested,
            "title": r.get("title"),
            "description": r.get("description"),
            "language": r.get("language"),
            "author": r.get("author"),
            "published_date": r.get("published_date"),
            "published_ts": _parse_date_to_ts(r.get("published_date")),
            "content": r.get("text") if isinstance(r.get("text"), str) else json.dumps(r.get("text") or {}),
            "fetch_failed": False,
            "fetch_error": None,
        }
    for e in payload.get("errors", []):
        requested = e.get("url")
        if not requested:
            continue
        out[requested] = {
            "fetch_url": requested,
            "final_url": requested,
            "content": "",
            "fetch_failed": True,
            "fetch_error": e.get("error") or e.get("code") or "fetch_failed",
        }
    for url in urls:
        if url not in out:
            out[url] = {
                "fetch_url": url,
                "final_url": url,
                "content": "",
                "fetch_failed": True,
                "fetch_error": "missing_from_fetch_response",
            }
    return out


async def _fetch_many(urls: list[str], purpose: str) -> tuple[dict[str, dict[str, Any]], int]:
    chunks = [urls[i : i + FETCH_BATCH_SIZE] for i in range(0, len(urls), FETCH_BATCH_SIZE)]

    async def run(chunk: list[str]) -> dict[str, dict[str, Any]]:
        try:
            return await _do_fetch_batch(chunk, purpose)
        except Exception as exc:
            return {
                u: {
                    "fetch_url": u,
                    "final_url": u,
                    "content": "",
                    "fetch_failed": True,
                    "fetch_error": str(exc),
                }
                for u in chunk
            }

    batches = await asyncio.gather(*(run(c) for c in chunks)) if chunks else []
    merged: dict[str, dict[str, Any]] = {}
    for batch in batches:
        merged.update(batch)
    return merged, len(chunks)


async def _check_retraction(url: Optional[str]) -> dict[str, Any]:
    result = {"checked": False, "retracted": False, "corrected": False, "concern_flagged": False}
    if not ENABLE_CROSSREF_CHECKS or not url:
        return result
    m = _DOI_RE.search(url)
    if not m:
        return result
    doi = m.group(0).rstrip(".,)")
    if doi in _RETRACTION_CACHE:
        return _RETRACTION_CACHE[doi]
    try:
        resp = await asyncio.wait_for(
            get_http_client().get(f"{CROSSREF_API_BASE}/{doi}"), timeout=EXTERNAL_CHECK_TIMEOUT
        )
        result["checked"] = True
        if resp.status_code == 200:
            msg = resp.json().get("message", {})
            for u in msg.get("update-to", []):
                t = (u.get("type") or "").lower()
                if "retraction" in t:
                    result["retracted"] = True
                elif "concern" in t:
                    result["concern_flagged"] = True
                elif "correction" in t or "erratum" in t:
                    result["corrected"] = True
    except Exception:
        result["checked"] = False
    _RETRACTION_CACHE[doi] = result
    return result


async def _check_author(author: Optional[str]) -> dict[str, Any]:
    result = {"checked": False, "retracted_count": 0}
    if not ENABLE_OPENALEX_CHECKS or not author:
        return result
    if author in _AUTHOR_CACHE:
        return _AUTHOR_CACHE[author]
    try:
        client = get_http_client()
        r = await asyncio.wait_for(
            client.get(f"{OPENALEX_API_BASE}/authors", params={"search": author, "per_page": 1}),
            timeout=EXTERNAL_CHECK_TIMEOUT,
        )
        if r.status_code == 200 and r.json().get("results"):
            aid = r.json()["results"][0]["id"].rsplit("/", 1)[-1]
            wr = await asyncio.wait_for(
                client.get(
                    f"{OPENALEX_API_BASE}/works",
                    params={"filter": f"author.id:{aid},is_retracted:true", "per_page": 1},
                ),
                timeout=EXTERNAL_CHECK_TIMEOUT,
            )
            result["checked"] = True
            if wr.status_code == 200:
                result["retracted_count"] = wr.json().get("meta", {}).get("count", 0)
    except Exception:
        result["checked"] = False
    _AUTHOR_CACHE[author] = result
    return result


_DOI_RE = re.compile(r"10\.\d{4,9}/[^\s&?#\"\']+", re.I)
_RETRACTION_CACHE: dict[str, dict[str, Any]] = {}
_AUTHOR_CACHE: dict[str, dict[str, Any]] = {}


async def close_http_client() -> None:
    global _HTTP_CLIENT
    if _HTTP_CLIENT is not None:
        await _HTTP_CLIENT.aclose()
        _HTTP_CLIENT = None
