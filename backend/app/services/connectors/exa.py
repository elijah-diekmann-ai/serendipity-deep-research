# backend/app/services/connectors/exa.py
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

from .base import BaseConnector, ConnectorResult
from ..caching import cached_get
from ...core.config import get_settings

settings = get_settings()


class ExaConnector(BaseConnector):
    """
    Exa connector implementing the current /search and /findSimilar APIs.

    Design goals:
    - Use `type="deep"` for better recall on company research.
    - Use the `contents` object with:
        * text: true
        * livecrawl: "fallback"
        * subpages + subpageTarget: to automatically pull /team, /about, /portfolio etc.
        * highlights: to focus extraction on HQ, identifiers, team, funding, product,
          technology, and other hard evidence (patents, filings, technical specs).
    - Support both generic search and similarity search via a single `fetch` entrypoint,
      controlled by `mode` in params: "search" (default) or "similar".
    - Normalise results into a common "snippets" list:
        {
          "url": ...,
          "title": ...,
          "snippet": ...,
          "domain": ...,
          "provider": "exa",
          "published_date": ...
        }
    """

    name = "exa"

    def __init__(self) -> None:
        self.search_url = "https://api.exa.ai/search"
        self.find_similar_url = "https://api.exa.ai/findSimilar"
        self.max_results_per_query = 10
        self.default_subpages = 3
        self.default_subpage_targets = [
            "team",
            "about",
            "company",
            "leadership",
            "management",
            "people",
            "founders",
            "board",
            "partners",
            "portfolio",
            "investments",
            "companies",
            "product",
            "products",
            "solutions",
            "platform",
            "technology",
            "engineering",
            "tech",
            "docs",
            "documentation",
            "developers",
            "api",
            "blog",
            "news",
            "press",
            "careers",
        ]

    def _headers(self) -> Dict[str, str]:
        return {
            "x-api-key": settings.EXA_API_KEY,
            "Content-Type": "application/json",
            "accept": "application/json",
        }

    def _build_search_payload(
        self,
        query: str,
        include_domains: Optional[List[str]] = None,
        start_published_date: Optional[str] = None,
        end_published_date: Optional[str] = None,
        category: Optional[str] = None,
        highlights_query: Optional[str] = None,
        subpages: Optional[int] = None,
        subpage_targets: Optional[List[str]] = None,
        num_results: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Construct a /search payload using Exa's modern "contents" block.

        The default highlights query is tuned for high-density research:
        legal entity identifiers (ABN/ACN/EIN/VAT/company number, stock tickers),
        incorporation dates and jurisdictions, headquarters, leadership, funding
        amounts, technical architectures and benchmarks, IP/patents, regulatory
        approvals, revenue/headcount metrics, and major risks.
        """
        contents: Dict[str, Any] = {
            "text": True,
            "livecrawl": "fallback",
        }

        # Subpages: pull team / portfolio / about / investors / tech pages automatically
        sp = subpages if subpages is not None else self.default_subpages
        if sp and sp > 0:
            contents["subpages"] = int(sp)

        targets = subpage_targets if subpage_targets else self.default_subpage_targets
        if targets:
            contents["subpageTarget"] = targets

        contents["highlights"] = {
            "numSentences": 6,
            "query": highlights_query
            or (
                "Legal entity name, incorporation/registration date, jurisdiction, "
                "headquarters address, registration numbers and identifiers "
                "(ABN, ACN, EIN, VAT, company number, stock ticker), founding story "
                "or spin-out origin, leadership team and key executives, products "
                "and services, target customers, pricing model, technology stack "
                "and architecture, patents and IP identifiers, regulatory filings "
                "(SEC, clinical trials, certifications), funding rounds and "
                "investors, revenue/ARR/headcount or other scale metrics, "
                "competitors, market focus, and key risks."
            ),
        }

        payload: Dict[str, Any] = {
            "query": query,
            "numResults": num_results or self.max_results_per_query,
            "type": "deep",
            "contents": contents,
        }

        # Prefer company- or news-focused results when caller specifies a category
        if category:
            payload["category"] = category

        if include_domains:
            payload["includeDomains"] = include_domains

        if start_published_date:
            payload["startPublishedDate"] = start_published_date
        if end_published_date:
            payload["endPublishedDate"] = end_published_date

        # Never use Exa's own marketing site as "evidence" about the target company
        # unless explicitly requested.
        payload.setdefault("excludeDomains", []).append("exa.ai")

        return payload

    async def _parse_results(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Normalise Exa results into our "snippet" shape, preferring highlights when present.
        """
        results: List[Dict[str, Any]] = []

        for r in data.get("results", []):
            url = r.get("url")
            if not url:
                continue

            parsed = urlparse(url)
            domain = parsed.netloc or None

            snippet_parts: List[str] = []

            # Prefer semantic highlights when available
            highlights = r.get("highlights")
            if isinstance(highlights, list):
                snippet_parts.extend(
                    h.get("text", "")
                    for h in highlights
                    if isinstance(h, dict)
                )
            elif isinstance(highlights, dict):
                text_val = highlights.get("text")
                if isinstance(text_val, str):
                    snippet_parts.append(text_val)

            text_val = r.get("text")
            if isinstance(text_val, str):
                snippet_parts.append(text_val)

            snippet_text = " ".join(p for p in snippet_parts if p).strip()
            snippet_text = snippet_text[:4000]  # keep DB + prompt size bounded

            results.append(
                {
                    "url": url,
                    "title": r.get("title"),
                    "snippet": snippet_text,
                    "domain": domain,
                    "provider": "exa",
                    "published_date": r.get("publishedDate") or r.get("published_date"),
                }
            )

        return results

    async def _search_single(
        self,
        client: httpx.AsyncClient,
        query: str,
        include_domains: Optional[List[str]] = None,
        start_published_date: Optional[str] = None,
        end_published_date: Optional[str] = None,
        category: Optional[str] = None,
        highlights_query: Optional[str] = None,
        subpages: Optional[int] = None,
        subpage_targets: Optional[List[str]] = None,
        num_results: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        # Cache key sensitive to all filters that materially affect results
        cache_parts: List[str] = ["search", query]
        if include_domains:
            cache_parts.append("domains:" + ",".join(sorted(include_domains)))
        if start_published_date:
            cache_parts.append(f"start:{start_published_date}")
        if end_published_date:
            cache_parts.append(f"end:{end_published_date}")
        if category:
            cache_parts.append(f"cat:{category}")
        cache_key = "exa:" + "|".join(cache_parts)

        cached = await cached_get(cache_key)
        if cached is not None:
            return cached

        payload = self._build_search_payload(
            query=query,
            include_domains=include_domains,
            start_published_date=start_published_date,
            end_published_date=end_published_date,
            category=category,
            highlights_query=highlights_query,
            subpages=subpages,
            subpage_targets=subpage_targets,
            num_results=num_results,
        )

        try:
            resp = await client.post(
                self.search_url,
                headers=self._headers(),
                json=payload,
                timeout=30,
            )
        except httpx.HTTPError:
            return []

        # Handle rate limits with a local, single retry
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else 5
            await asyncio.sleep(delay)
            try:
                resp = await client.post(
                    self.search_url,
                    headers=self._headers(),
                    json=payload,
                    timeout=30,
                )
            except httpx.HTTPError:
                return []

        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            # Treat client errors (except rate limiting) as "no data"
            return []

        resp.raise_for_status()
        data = resp.json()
        results = await self._parse_results(data)

        await cached_get(cache_key, set_value=results, ttl=60 * 60 * 24)
        return results

    async def _find_similar(
        self,
        client: httpx.AsyncClient,
        url: str,
        num_results: int = 5,
        exclude_domains: Optional[List[str]] = None,
        highlights_query: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Wrapper for Exa's /findSimilar for competitor / market mapping.
        """
        if not url:
            return []

        parsed = urlparse(url)
        seed_domain = parsed.netloc or None

        cache_parts: List[str] = ["similar", url]
        if exclude_domains:
            cache_parts.append("exclude:" + ",".join(sorted(exclude_domains)))
        cache_key = "exa:" + "|".join(cache_parts)

        cached = await cached_get(cache_key)
        if cached is not None:
            return cached

        effective_excludes = list(exclude_domains or [])
        if seed_domain and seed_domain not in effective_excludes:
            effective_excludes.append(seed_domain)
        if "exa.ai" not in effective_excludes:
            effective_excludes.append("exa.ai")

        payload: Dict[str, Any] = {
            "url": url,
            "numResults": num_results,
            "contents": {
                "text": True,
                "highlights": {
                    "numSentences": 3,
                    "query": highlights_query
                    or "Core product offering, customer segment, business model, and positioning versus the target.",
                },
            },
        }
        if effective_excludes:
            payload["excludeDomains"] = effective_excludes

        try:
            resp = await client.post(
                self.find_similar_url,
                headers=self._headers(),
                json=payload,
                timeout=30,
            )
        except httpx.HTTPError:
            return []

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else 5
            await asyncio.sleep(delay)
            try:
                resp = await client.post(
                    self.find_similar_url,
                    headers=self._headers(),
                    json=payload,
                    timeout=30,
                )
            except httpx.HTTPError:
                return []

        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            return []

        resp.raise_for_status()
        data = resp.json()
        results = await self._parse_results(data)

        await cached_get(cache_key, set_value=results, ttl=60 * 60 * 24)
        return results

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
    )
    async def fetch(self, **params: Any) -> ConnectorResult:
        """
        Unified entrypoint expected by the orchestrator.

        Supported modes (params["mode"]):
        - "search" (default): calls /search with `queries`.
        - "similar": calls /findSimilar with `url`.

        Returns:
            ConnectorResult({"snippets": [...]})
        """
        if not settings.EXA_API_KEY:
            return ConnectorResult({})

        mode = (params.get("mode") or "search").lower()
        snippets: List[Dict[str, Any]] = []

        async with httpx.AsyncClient(timeout=30) as client:
            if mode == "search":
                queries = params.get("queries") or []
                if isinstance(queries, str):
                    queries = [queries]
                queries = [
                    str(q).strip() for q in queries if str(q).strip()
                ]
                if not queries:
                    return ConnectorResult({})

                include_domains = params.get("include_domains")
                if isinstance(include_domains, str):
                    include_domains = [include_domains]
                if include_domains is not None:
                    include_domains = [
                        d.strip()
                        for d in include_domains
                        if isinstance(d, str) and d.strip()
                    ] or None

                start_published_date = params.get("start_published_date")
                end_published_date = params.get("end_published_date")
                category = params.get("category")
                highlights_query = params.get("highlights_query")
                subpages = params.get("subpages")
                subpage_targets = params.get("subpage_targets")
                num_results = params.get("num_results")

                tasks = [
                    self._search_single(
                        client,
                        q,
                        include_domains=include_domains,
                        start_published_date=start_published_date,
                        end_published_date=end_published_date,
                        category=category,
                        highlights_query=highlights_query,
                        subpages=subpages,
                        subpage_targets=subpage_targets,
                        num_results=num_results,
                    )
                    for q in queries
                ]
                results_per_query = await asyncio.gather(
                    *tasks, return_exceptions=True
                )
                for res in results_per_query:
                    if isinstance(res, Exception):
                        # Bubble up so Tenacity can retry the entire fetch
                        raise res
                    snippets.extend(res)

            elif mode in ("similar", "find_similar"):
                url = params.get("url")
                if not isinstance(url, str) or not url.strip():
                    return ConnectorResult({})
                num_results = params.get("num_results", 5)
                exclude_domains = params.get("exclude_domains")
                if isinstance(exclude_domains, str):
                    exclude_domains = [exclude_domains]
                if exclude_domains is not None:
                    exclude_domains = [
                        d.strip()
                        for d in exclude_domains
                        if isinstance(d, str) and d.strip()
                    ] or None
                highlights_query = params.get("highlights_query")

                results = await self._find_similar(
                    client,
                    url=url,
                    num_results=int(num_results),
                    exclude_domains=exclude_domains,
                    highlights_query=highlights_query,
                )
                snippets.extend(results)

            else:
                # Unknown mode – treat as no-op instead of exploding the whole job.
                return ConnectorResult({})

        return ConnectorResult({"snippets": snippets})
