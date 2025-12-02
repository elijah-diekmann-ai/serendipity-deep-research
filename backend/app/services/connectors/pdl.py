# backend/app/services/connectors/pdl.py
# Currently PDL is our primary People discovery layer, we may use Apollo for discovery in the future.

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple
import logging

import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from .base import BaseConnector, ConnectorResult
from ...core.config import get_settings
from ..caching import cached_get

settings = get_settings()
logger = logging.getLogger(__name__)


class PDLConnector(BaseConnector):
    """
    People Data Labs connector used for BOTH discovery and enrichment.

    Two modes of operation:
    1. **Discovery (search_people)**: Find leadership/founders at a company using
       PDL's Person Search API. Returns structured people data including LinkedIn,
       work history, and education.
    2. **Enrichment (enrich_many)**: Enrich existing PersonNodes with deeper
       biography data (work history, education, skills).

    PDL is particularly valuable because:
    - Works on free tier (100 matches/month)
    - Has LinkedIn-derived data (work history, education)
    - Can find people at stealth/early-stage companies

    Returned payload shape for discovery:
        {
          "people": [
            {
              "pdl_id": "...",
              "full_name": "Jane Doe",
              "title": "CEO",
              "company": "Acme Corp",
              "company_domain": "acme.com",
              "linkedin_url": "https://linkedin.com/in/janedoe",
              "photo_url": "...",
              "source": "pdl",
              # Full PDL data for enrichment
              "pdl_data": { ... }
            },
            ...
          ]
        }
    """

    name = "pdl"
    base_url = "https://api.peopledatalabs.com/v5"

    def __init__(self) -> None:
        self.api_key: Optional[str] = getattr(settings, "PDL_API_KEY", None)
        # Allow override via settings, but default to a conservative threshold.
        self.min_likelihood: int = int(
            getattr(settings, "PDL_MIN_LIKELIHOOD", 3) or 3
        )
        self.max_results: int = int(
            getattr(settings, "PDL_MAX_SEARCH_RESULTS", 3) or 3
        )

    async def _enrich_single(
        self,
        client: httpx.AsyncClient,
        person_input: Dict[str, Any],
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Enrich a single person using the PDL Person Enrichment API (v5).

        person_input: {
          "key": str,
          "name": str | None,
          "company": str | None,
          "linkedin_url": str | None,
          # ...optionally other hints
        }

        Returns:
          (key, payload_or_none)

        where payload_or_none is the **PDL 'data' object** (flattened person
        profile), not the entire HTTP response envelope, or None if no match
        or if the match is below min_likelihood.
        """
        key = str(person_input.get("key") or "")
        if not key:
            # Defensive: we never want enrichment errors to bubble up.
            logger.debug("PDL enrichment called with missing key; skipping.")
            return "", None

        if not self.api_key:
            logger.debug("PDL_API_KEY not configured; skipping enrichment for key=%s", key)
            return key, None

        # Normalised inputs
        name = (person_input.get("name") or "").strip() or None
        company = (person_input.get("company") or "").strip() or None
        linkedin_url = (person_input.get("linkedin_url") or "").strip() or None

        # Build parameters subject to PDL minimum input rules:
        # profile OR email OR phone OR email_hash OR lid OR pdl_id OR
        # ((first_name AND last_name) OR name) AND (company OR school OR location OR ...).
        params: Dict[str, Any] = {
            "api_key": self.api_key,
            "min_likelihood": self.min_likelihood,
            "pretty": "false",
        }

        if linkedin_url:
            # Preferred matching: direct LinkedIn profile.
            params["profile"] = linkedin_url
            if name:
                params["name"] = name
            if company:
                params["company"] = company
        else:
            # Fallback to name + company.
            if not (name and company):
                logger.debug(
                    "Insufficient fields for PDL enrichment (no profile or name+company) for key=%s",
                    key,
                )
                return key, None
            params["name"] = name
            params["company"] = company

        try:
            resp = await client.get(
                f"{self.base_url}/person/enrich",
                params=params,
                timeout=30,
            )
        except httpx.HTTPError as e:
            logger.warning(
                "PDL HTTP error for key=%s: %s",
                key,
                e,
            )
            return key, None

        # 404 = no match (non-billable).
        if resp.status_code == 404:
            logger.debug("PDL returned 404 (no match) for key=%s", key)
            return key, None

        # Basic single-shot retry on 429 within this helper.
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else 5
            logger.info(
                "PDL rate limited (429). Retrying after %s seconds for key=%s",
                delay,
                key,
            )
            await asyncio.sleep(delay)
            try:
                resp = await client.get(
                    f"{self.base_url}/person/enrich",
                    params=params,
                    timeout=30,
                )
            except httpx.HTTPError as e:
                logger.warning(
                    "PDL HTTP error on retry for key=%s: %s",
                    key,
                    e,
                )
                return key, None

        # Other 4xx (auth, bad request, etc.) → treat as configuration/inputs issue.
        if 400 <= resp.status_code < 500 and resp.status_code != 200:
            logger.warning(
                "PDL returned %s for key=%s; skipping enrichment. Body: %s",
                resp.status_code,
                key,
                resp.text[:500],
            )
            return key, None

        try:
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning(
                "PDL non-2xx response for key=%s: %s (body: %s)",
                key,
                e,
                resp.text[:500],
            )
            return key, None

        try:
            body = resp.json()
        except ValueError:
            logger.warning("PDL returned non-JSON response for key=%s", key)
            return key, None

        # PDL docs describe an envelope:
        # { "status": 200, "likelihood": 8, "data": { ...person fields... } }
        # but some tooling may show only the data object; support both.
        likelihood: Optional[float] = None

        if isinstance(body, dict) and "data" in body:
            status_val = body.get("status")
            if status_val is not None and status_val != 200:
                logger.info(
                    "PDL logical status=%s for key=%s; skipping.",
                    status_val,
                    key,
                )
                return key, None
            likelihood = body.get("likelihood")
            data = body.get("data") or None
        else:
            # Treat the body as the data object itself.
            data = body
            if isinstance(body, dict) and "likelihood" in body:
                likelihood = body.get("likelihood")

        if isinstance(likelihood, (int, float)) and likelihood < self.min_likelihood:
            logger.info(
                "PDL likelihood %.2f below threshold %s for key=%s; skipping.",
                likelihood,
                self.min_likelihood,
                key,
            )
            return key, None

        if not isinstance(data, dict) or not data:
            return key, None

        # At this point `data` should be the flattened PDL Person record.
        return key, data

    async def enrich_many(
        self,
        people_inputs: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Given a list of person input dicts, return a mapping:

            { key: pdl_data_object }

        where pdl_data_object is the 'data' payload returned by
        the Person Enrichment API (or omitted if no match).

        Notes:
        - If the API key is missing or inputs are empty, returns {}.
        - Concurrency is bounded via a semaphore to respect PDL rate limits.
        """
        if not self.api_key or not people_inputs:
            return {}

        # Conservative default; can be tuned via settings.PDL_MAX_CONCURRENT.
        max_concurrent = int(
            getattr(settings, "PDL_MAX_CONCURRENT", 5) or 5
        )
        sem = asyncio.Semaphore(max_concurrent)

        async with httpx.AsyncClient(timeout=30) as client:

            async def bound_enrich(person_input: Dict[str, Any]):
                async with sem:
                    return await self._enrich_single(client, person_input)

            tasks = [bound_enrich(p) for p in people_inputs]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        enriched: Dict[str, Dict[str, Any]] = {}
        for res in results:
            if isinstance(res, Exception):
                logger.warning("PDL enrichment task raised: %s", res)
                continue
            key, payload = res
            if payload:
                enriched[key] = payload

        return enriched

    # -------------------------------------------------------------------------
    # Discovery: Search for people at a company
    # -------------------------------------------------------------------------

    async def _search_people_by_company(
        self,
        client: httpx.AsyncClient,
        company_domain: Optional[str] = None,
        company_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Use PDL Person Search API to find leadership at a company.

        PDL uses Elasticsearch query syntax. We search for:
        - People currently at the company (job_company_name or job_company_website)
        - With senior titles (founder, ceo, cto, president, vp, head, director)

        Returns normalized person dicts ready for entity resolution.
        """
        if not self.api_key:
            logger.debug("PDL_API_KEY not configured; skipping search.")
            return []

        if not company_domain and not company_name:
            logger.debug("PDL search requires company_domain or company_name.")
            return []

        # Build Elasticsearch query for PDL
        must_clauses: List[Dict[str, Any]] = []

        # Company filter
        company_filters: List[Dict[str, Any]] = []
        if company_domain:
            domain = company_domain.lower().strip()
            if domain.startswith("www."):
                domain = domain[4:]
            company_filters.append({"term": {"job_company_website": domain}})
        if company_name:
            # Use match_phrase for names to handle multi-word companies better than 'term'
            company_filters.append({"match_phrase": {"job_company_name": company_name}})

        if company_filters:
            must_clauses.append({"bool": {"should": company_filters}})

        leadership_filter = {
            "bool": {
                "should": [
                    # Seniority levels
                    {
                        "terms": {
                            "job_title_levels": [
                                "cxo",
                                "vp",
                                "director",
                                "owner",
                                "partner",
                            ]
                        }
                    },
                    # Specific roles (important for "Founder" which isn't a level)
                    {
                        "terms": {
                            "job_title_role": [
                                "founder",
                                "co-founder",
                                "cofounder",
                                "ceo",
                                "chief executive officer",
                                "cto",
                                "chief technology officer",
                                "president",
                            ]
                        }
                    },
                ],
                # Removing minimum_should_match here too just to be safe, though it's logically correct for "OR".
                # Default for should is 1 anyway if it's the only clause, or if inside a must context.
            }
        }
        must_clauses.append(leadership_filter)

        es_query = {
            "query": {
                "bool": {
                    "must": must_clauses,
                }
            }
        }

        # Use POST with JSON body as recommended by PDL docs
        headers = {
            "X-Api-Key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Removing "searchType": "elastic" from body as it caused 400 errors.
        # It is likely a URL parameter or inferred.
        payload = {
            "query": es_query,
            "size": self.max_results,
            "pretty": False,
            "fields": [
                "id",
                "full_name",
                "first_name",
                "last_name",
                "linkedin_url",
                "job_title",
                "job_title_role",
                "job_title_sub_role",
                "job_title_levels",
                "job_company_name",
                "job_company_website",
                "job_company_location_name",
                "location_name",
                "work_email",
                "personal_emails",
                "mobile_phone",
                "industry",
                "job_company_is_current",
                # Enrichment fields
                "experience",
                "education",
                "profiles",
                "skills",
                "summary",
            ],
        }

        try:
            resp = await client.post(
                f"{self.base_url}/person/search",
                headers=headers,
                json=payload,
                timeout=30,
            )
        except httpx.HTTPError as e:
            logger.warning("PDL Person Search HTTP error: %s", e)
            return []

        # Handle rate limits
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else 5
            logger.info("PDL rate limited (429). Retrying after %s seconds.", delay)
            await asyncio.sleep(delay)
            try:
                resp = await client.post(
                    f"{self.base_url}/person/search",
                    headers=headers,
                    json=payload,
                    timeout=30,
                )
            except httpx.HTTPError as e:
                logger.warning("PDL Person Search HTTP error on retry: %s", e)
                return []

        if resp.status_code == 404:
            logger.debug("PDL Person Search returned no results.")
            return []

        if 400 <= resp.status_code < 500:
            logger.warning(
                "PDL Person Search returned %s: %s",
                resp.status_code,
                resp.text[:500],
            )
            return []

        try:
            resp.raise_for_status()
        except httpx.HTTPError as e:
            logger.warning("PDL Person Search non-2xx: %s", e)
            return []

        try:
            body = resp.json()
        except ValueError:
            logger.warning("PDL Person Search returned non-JSON response.")
            return []

        # PDL returns { "status": 200, "data": [...], "total": N }
        raw_people = body.get("data") or []
        if not raw_people:
            logger.debug("PDL Person Search returned empty data array.")
            return []

        # Normalize to our internal format
        people: List[Dict[str, Any]] = []
        for p in raw_people:
            if not isinstance(p, dict):
                continue

            # Extract name
            full_name = p.get("full_name") or ""
            if not full_name:
                first = p.get("first_name") or ""
                last = p.get("last_name") or ""
                full_name = f"{first} {last}".strip()
            if not full_name:
                continue

            # Extract current job info
            job_title = p.get("job_title") or p.get("job_title_role")
            job_company = p.get("job_company_name")
            job_company_domain = p.get("job_company_website")

            # LinkedIn URL
            linkedin_url = p.get("linkedin_url")

            # Photo
            photo_url = None
            profiles = p.get("profiles") or []
            for profile in profiles:
                if isinstance(profile, dict) and profile.get("network") == "linkedin":
                    photo_url = profile.get("photo_url")
                    break

            people.append({
                "pdl_id": p.get("id"),
                "full_name": full_name,
                "title": job_title,
                "company": job_company,
                "company_domain": job_company_domain,
                "linkedin_url": linkedin_url,
                "photo_url": photo_url,
                "source": "pdl",
                # Keep full PDL data for enrichment
                "pdl_data": p,
            })

        logger.info(
            "PDL Person Search found %d people for %s",
            len(people),
            company_domain or company_name,
        )
        return people

    # -------------------------------------------------------------------------
    # Connector interface (used by orchestrator)
    # -------------------------------------------------------------------------

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    async def fetch(self, **kwargs: Any) -> ConnectorResult:
        """
        Unified entrypoint expected by the orchestrator.

        Expected params:
        - company_domain: str (preferred for company search)
        - company_name: str (fallback for company search)
        - full_name: str (REQUIRED for person lookup)
        - location: str (optional for person lookup)
        - linkedin_url: str (optional for person lookup)

        Returns:
            ConnectorResult({
              "people": [...],
            })
        """
        if not self.api_key:
            logger.info("PDL_API_KEY not configured; returning empty result.")
            return ConnectorResult({})

        # 1. Person Lookup / Enrichment Mode
        full_name = kwargs.get("full_name")
        if full_name:
            # We are looking for a specific person. Use enrichment logic.
            person_input = {
                "name": full_name,
                "company": kwargs.get("company_name"),
                "linkedin_url": kwargs.get("linkedin_url"),
                "location": kwargs.get("location"),
                "key": "target_person"
            }
            
            # enrich_many expects a list and returns a dict keyed by 'key'
            enriched_map = await self.enrich_many([person_input])
            pdl_data = enriched_map.get("target_person")
            
            people = []
            if pdl_data:
                # Normalize to the same shape as search results so downstream can ingest it uniformly
                # PDL enrichment returns the flat person object directly.
                p = pdl_data
                
                # Extract name
                fn = p.get("full_name") or full_name
                
                # Extract current job info
                job_title = p.get("job_title") or p.get("job_title_role")
                job_company = p.get("job_company_name") or p.get("employer")
                job_company_domain = p.get("job_company_website")
                
                # Profiles
                linkedin_url = p.get("linkedin_url")
                photo_url = None
                # If enrichment returns 'profiles' list, check it too? 
                # Usually top-level linkedin_url is best.
                
                people.append({
                    "pdl_id": p.get("id"),
                    "full_name": fn,
                    "title": job_title,
                    "company": job_company,
                    "company_domain": job_company_domain,
                    "linkedin_url": linkedin_url,
                    "photo_url": photo_url, # might be deeper in payload
                    "source": "pdl",
                    "pdl_data": p,
                })
                
            # Cache key for person lookup
            # We don't cache enrichment in this connector usually (cached_get is for HTTP), 
            # but here we are wrapping it. enrich_many calls HTTP which is not cached inside PDLConnector 
            # except via logic? No, PDLConnector calls requests directly.
            # Let's not add extra caching layer here for now as it's specific 
            # and PDL might return different things.
            
            return ConnectorResult({"people": people})

        # 2. Company Leadership Search Mode
        company_domain = kwargs.get("company_domain")
        company_name = kwargs.get("company_name")

        if not company_domain and not company_name:
            logger.debug("PDL fetch requires full_name (person) or company_domain/name (company).")
            return ConnectorResult({})

        cache_key = (
            f"pdl:search:{company_domain or ''}|{company_name or ''}|size={self.max_results}"
        )
        cached = await cached_get(cache_key)
        if cached is not None:
            return ConnectorResult(cached)

        async with httpx.AsyncClient(timeout=30) as client:
            people = await self._search_people_by_company(
                client,
                company_domain=company_domain,
                company_name=company_name,
            )

        result_data = {"people": people}
        # Cache for 7 days – leadership for a domain is relatively stable.
        await cached_get(cache_key, set_value=result_data, ttl=60 * 60 * 24 * 7)

        return ConnectorResult(result_data)
