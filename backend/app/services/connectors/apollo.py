# backend/app/services/connectors/apollo.py

# Note: the Apollo connector is currently not used / not setup in the .env file as we are using PDL.
# Apollo doesn't provide free tier for people search API, it merely doesn't deduct credits for people already on paid plans.

from __future__ import annotations

import asyncio
import logging

from typing import Any, Dict, List, Optional

import httpx

from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

from .base import BaseConnector, ConnectorResult
from ..caching import cached_get
from ...core.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

class ApolloConnector(BaseConnector):
    """
    Apollo.io connector used as a "Recruiter" / discovery layer.

    Responsibilities:
    - Given a company domain, resolve the best-matching Apollo organization
      and extract firmographics (headcount, founded year, revenue band).
    - Use People API Search to find leadership / founder-type people linked to
      the organization/domain.
    - Return a NORMALISED internal payload, not raw Apollo JSON:
        {
          "people": [
            {
              "full_name": "Jane Doe",
              "title": "CEO",
              "company": "Emergence Quantum",
              "company_domain": "emergencequantum.com",
              "linkedin_url": "https://linkedin.com/in/jane-doe",
              "photo_url": "https://...",
              "source": "apollo",
              "apollo_person_id": "..."
            },
            ...
          ],
          "organization": {
            "apollo_organization_id": "...",
            "name": "Emergence Quantum",
            "primary_domain": "emergencequantum.com",
            "estimated_num_employees": "11-50",
            "founded_year": 2024,
            "annual_revenue": "1000000-5000000"
          },
          "pagination": {
            "page": 1,
            "total_pages": 1
          }
        }
    - Missing data (no org / no people) is a legitimate outcome.
    """
    name = "apollo"

    def __init__(self) -> None:
        self.api_key: Optional[str] = getattr(settings, "APOLLO_API_KEY", None)
        self.base_url = "https://api.apollo.io/api/v1"
        self.people_per_page = 25

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _auth_headers(self) -> Dict[str, str]:
        """
        Apollo API uses Bearer tokens for authentication.
        """
        return {
            "Authorization": f"Bearer {self.api_key}",
            "X-Api-Key": self.api_key,
            "Content-Type": "application/json",
            "accept": "application/json",
        }

    def _normalise_domain(self, raw: str | None) -> Optional[str]:
        if not raw:
            return None
        d = raw.strip().lower()
        if "://" in d:
            d = d.split("://", 1)[1]
        # Strip path/query
        d = d.split("/", 1)[0]
        # Strip port
        d = d.split(":", 1)[0]
        if d.startswith("www."):
            d = d[4:]
        return d or None

    async def _search_organization(
        self,
        client: httpx.AsyncClient,
        domain: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Use organizations/enrich to find the best-matching organization.
        This is more robust than mixed_companies/search with name guessing.
        """
        try:
            resp = await client.get(
                f"{self.base_url}/organizations/enrich",
                headers=self._auth_headers(),
                params={"domain": domain},
                timeout=30,
            )
        except httpx.HTTPError:
            # Treat network-layer problems as retriable via Tenacity
            raise

        # Rate limiting: single backoff + retry
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else 5
            await asyncio.sleep(delay)
            try:
                resp = await client.get(
                    f"{self.base_url}/organizations/enrich",
                    headers=self._auth_headers(),
                    params={"domain": domain},
                    timeout=30,
                )
            except httpx.HTTPError:
                raise

        # 4xx (non-rate-limit) => caller treats as "no org"
        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            logger.warning(
                "Apollo %s returned %s: %s",
                "organizations/enrich",
                resp.status_code,
                resp.text[:500],
            )
            return None

        try:
            resp.raise_for_status()
        except httpx.HTTPError:
            # 5xx etc: let Tenacity retry at the top level
            raise

        data = resp.json() or {}
        org = data.get("organization")

        if not org:
            return None

        org_id = org.get("id") or org.get("organization_id")
        primary_domain = (
            org.get("primary_domain")
            or org.get("domain")
            or org.get("website_url")
            or domain
        )
        # The exact key names can vary; be defensive.
        est_employees = (
            org.get("estimated_num_employees")
            or org.get("estimated_num_employees_range")
            or org.get("employee_count")
            or org.get("num_employees")
        )
        founded_year = org.get("founded_year") or org.get("year_founded")
        annual_revenue = (
            org.get("annual_revenue")
            or org.get("annual_revenue_range")
            or org.get("revenue")
        )

        return {
            "apollo_organization_id": org_id,
            "name": org.get("name"),
            "primary_domain": primary_domain,
            "estimated_num_employees": est_employees,
            "founded_year": founded_year,
            "annual_revenue": annual_revenue,
        }

    async def _search_organization_by_name(
        self,
        client: httpx.AsyncClient,
        company_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Fallback when we don't have a domain: use Organization Search
        to resolve the company and derive its primary domain + firmographics.
        """
        name = (company_name or "").strip()
        if not name:
            return None

        payload: Dict[str, Any] = {
            "page": 1,
            "per_page": 1,
            "q_organization_name": name,
        }

        try:
            resp = await client.post(
                f"{self.base_url}/mixed_companies/search",
                headers=self._auth_headers(),
                json=payload,
                timeout=30,
            )
        except httpx.HTTPError:
            raise

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else 5
            await asyncio.sleep(delay)
            resp = await client.post(
                f"{self.base_url}/mixed_companies/search",
                headers=self._auth_headers(),
                json=payload,
                timeout=30,
            )

        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            return None

        resp.raise_for_status()
        data = resp.json() or {}
        orgs = (
            data.get("organizations")
            or data.get("companies")
            or []
        )
        if not orgs:
            return None

        org = orgs[0]
        primary_domain = (
            org.get("primary_domain")
            or org.get("domain")
            or org.get("website_url")
        )
        est_employees = (
            org.get("estimated_num_employees")
            or org.get("estimated_num_employees_range")
            or org.get("employee_count")
            or org.get("num_employees")
        )
        founded_year = org.get("founded_year") or org.get("year_founded")
        annual_revenue = (
            org.get("annual_revenue")
            or org.get("annual_revenue_range")
            or org.get("revenue")
        )

        return {
            "apollo_organization_id": org.get("id") or org.get("organization_id"),
            "name": org.get("name"),
            "primary_domain": primary_domain,
            "estimated_num_employees": est_employees,
            "founded_year": founded_year,
            "annual_revenue": annual_revenue,
        }

    async def _search_people(
        self,
        client: httpx.AsyncClient,
        domain: Optional[str],
        apollo_organization_id: Optional[str],
    ) -> Dict[str, Any]:
        """
        Use People API Search (api_search) to find founders/leadership.
        We only pull the first page; pagination metadata is returned so the
        system can extend this in future if needed.
        """
        if not domain and not apollo_organization_id:
            return {
                "people": [],
                "pagination": {"page": 1, "total_pages": 1},
            }

        payload: Dict[str, Any] = {
            "page": 1,
            "per_page": self.people_per_page,
            "person_titles": [
                "founder",
                "co-founder",
                "ceo",
                "chief executive officer",
                "cto",
                "chief technology officer",
                "president",
                "board",
                "vp",
                "vice president",
                "head",
            ],
            "person_seniorities": ["owner", "founder", "c_suite", "vp", "head"],
        }

        if apollo_organization_id:
            payload["organization_ids"] = [apollo_organization_id]
        elif domain:
            # People API Search: filter by company using q_organization_domains_list
            # No "www", accepts up to 1,000 domains per request.
            payload["q_organization_domains_list"] = [domain]

        try:
            resp = await client.post(
                f"{self.base_url}/mixed_people/api_search",
                headers=self._auth_headers(),
                json=payload,
                timeout=30,
            )
        except httpx.HTTPError:
            raise

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else 5
            await asyncio.sleep(delay)
            try:
                resp = await client.post(
                    f"{self.base_url}/mixed_people/api_search",
                    headers=self._auth_headers(),
                    json=payload,
                    timeout=30,
                )
            except httpx.HTTPError:
                raise

        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            # Invalid filters / permissions issues – treat as "no people".
            logger.warning(
                "Apollo %s returned %s: %s",
                "mixed_people/api_search",
                resp.status_code,
                resp.text[:500],
            )
            return {
                "people": [],
                "pagination": {"page": 1, "total_pages": 1},
            }

        try:
            resp.raise_for_status()
        except httpx.HTTPError:
            raise

        data = resp.json() or {}
        raw_people = data.get("people") or []
        pagination = data.get("pagination") or {}

        people: List[Dict[str, Any]] = []
        for p in raw_people:
            if not isinstance(p, dict):
                continue

            org = p.get("organization") or {}
            full_name = (
                p.get("name")
                or " ".join(
                    x
                    for x in [
                        p.get("first_name"),
                        p.get("last_name"),
                    ]
                    if x
                )
                or "Unknown"
            )
            title = p.get("title") or p.get("job_title") or p.get("headline")
            linkedin_url = (
                p.get("linkedin_url")
                or p.get("linkedin_url_clean")
                or p.get("linkedin_profile_url")
            )
            photo_url = p.get("photo_url") or p.get("avatar_url") or p.get("photo")
            company_name = (
                org.get("name")
                or p.get("organization_name")
                or p.get("company_name")
            )
            company_domain = (
                org.get("primary_domain")
                or org.get("domain")
                or (domain or None)
            )

            # Pass raw organization data for fallback enrichment
            person_data = {
                "apollo_person_id": p.get("id"),
                "full_name": full_name,
                "title": title,
                "company": company_name,
                "company_domain": company_domain,
                "linkedin_url": linkedin_url,
                "photo_url": photo_url,
                "source": "apollo",
                # Internal field for fallback org enrichment
                "organization_raw": org
            }
            people.append(person_data)

        pagination_out = {
            "page": pagination.get("page", 1),
            "total_pages": pagination.get("total_pages", 1),
        }

        return {
            "people": people,
            "pagination": pagination_out,
        }

    async def _fetch_for_domain(
        self,
        domain_raw: str,
    ) -> Dict[str, Any]:
        """
        Core logic: resolve leadership for a single domain using People API Search only.
        On lower Apollo plans, organization search/enrichment endpoints may not be
        available, so we treat Apollo strictly as a people-discovery layer.
        """
        domain = self._normalise_domain(domain_raw)
        if not domain:
            return {
                "people": [],
                "organization": None,
                "pagination": {"page": 1, "total_pages": 1},
            }

        async with httpx.AsyncClient(timeout=30) as client:
            people_payload = await self._search_people(
                client,
                domain=domain,
                apollo_organization_id=None,  # People API Search only
            )

        # Try to scrape basic org info from the first person if available
        people = people_payload.get("people", [])
        org = None
        
        if people:
            first_person_org = people[0].get("organization_raw", {})
            if first_person_org:
                est_employees = (
                    first_person_org.get("estimated_num_employees")
                    or first_person_org.get("estimated_num_employees_range")
                    or first_person_org.get("employee_count")
                )
                annual_revenue = (
                    first_person_org.get("annual_revenue")
                    or first_person_org.get("revenue")
                )
                
                org = {
                    "apollo_organization_id": first_person_org.get("id"),
                    "name": first_person_org.get("name"),
                    "primary_domain": domain,
                    "estimated_num_employees": est_employees,
                    "founded_year": first_person_org.get("founded_year"),
                    "annual_revenue": annual_revenue,
                }

        # Cleanup raw field
        for p in people:
            p.pop("organization_raw", None)

        return {
            "people": people,
            "organization": org,  # best-effort scraped org data
            "pagination": people_payload.get(
                "pagination", {"page": 1, "total_pages": 1}
            ),
        }

    # ------------------------------------------------------------------
    # Public connector entrypoint (async)
    # ------------------------------------------------------------------

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    async def fetch(self, **kwargs: Any) -> ConnectorResult:
        """
        Unified entrypoint expected by the orchestrator.

        Expected params:
        - company_domain: str   (preferred; already provided by planner)
        - domain: str
        - website: str          (we'll extract the host)

        Returns:
            ConnectorResult({
              "people": [...],
              "organization": None,
              "pagination": {...}
            })
        """
        if not self.api_key:
            return ConnectorResult({})

        company_domain = kwargs.get("company_domain") or kwargs.get("domain")
        website = kwargs.get("website")
        
        domain = self._normalise_domain(company_domain or website)

        if not domain:
            # Planner only calls Apollo when a domain is available; without it we
            # cannot safely target a specific employer.
            return ConnectorResult({})

        cache_key = f"apollo:domain={domain}"
        cached = await cached_get(cache_key)
        if cached is not None:
            return ConnectorResult(cached)

        result_data = await self._fetch_for_domain(domain)

        # Cache for 7 days – leadership for a domain is relatively stable.
        await cached_get(cache_key, set_value=result_data, ttl=60 * 60 * 24 * 7)

        return ConnectorResult(result_data)

# ----------------------------------------------------------------------
# Synchronous convenience entrypoint (module-level)
# ----------------------------------------------------------------------

async def _fetch_single_domain_async(company_domain: str) -> Dict[str, Any]:
    connector = ApolloConnector()
    return await connector._fetch_for_domain(company_domain)

def fetch(company_domain: str) -> Dict[str, Any]:
    """
    Convenience synchronous entrypoint:
        from app.services.connectors import apollo
        result = apollo.fetch("emergencequantum.com")

    This returns the same normalised structure as ApolloConnector.fetch().
    """
    if not company_domain:
        return {}

    try:
        return asyncio.run(_fetch_single_domain_async(company_domain))
    except RuntimeError:
        # If an event loop is already running (e.g. in a notebook), fall back
        # to creating and using a dedicated loop.
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(_fetch_single_domain_async(company_domain))
        finally:
            loop.close()
