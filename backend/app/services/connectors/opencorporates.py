from __future__ import annotations

from typing import Any, Dict, Optional, List, Tuple

import logging

import httpx

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from .base import BaseConnector, ConnectorResult

from ...core.config import get_settings

from ..caching import cached_get

logger = logging.getLogger(__name__)

settings = get_settings()

class OpenCorporatesConnector(BaseConnector):
    """
    OpenCorporates connector for registry-grade corporate data.

    Use cases:
    - Resolve legal entity name, number, jurisdiction, status, incorporation date.
    - Provide authoritative corporate identifiers & registered address.

    Only used (via Writer policy) for the Founding Details section.
    """

    name = "open_corporates"

    def __init__(self) -> None:
        self.api_token: Optional[str] = settings.OPENCORPORATES_API_TOKEN
        self.base_url: str = settings.OPENCORPORATES_BASE_URL
        self.timeout: int = settings.OPENCORPORATES_TIMEOUT_SECONDS
        self.max_results: int = settings.OPENCORPORATES_MAX_RESULTS

    def _headers(self) -> Dict[str, str]:
        if not self.api_token:
            return {}
        return {
            "X-API-TOKEN": self.api_token,  # per OC auth docs
            "Accept": "application/json",
        }

    async def _search_company(
        self,
        client: httpx.AsyncClient,
        company_name: str,
        jurisdiction_code: str | None = None,
        country_code: str | None = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Call GET /companies/search and return the single best match's 'company' object
        or None if no reasonable match.

        Uses order=score, filters out inactive companies.
        """
        params: Dict[str, Any] = {
            "q": company_name,
            "order": "score",
            "per_page": self.max_results,
        }

        if jurisdiction_code:
            params["jurisdiction_code"] = jurisdiction_code
        if country_code:
            params["country_code"] = country_code

        try:
            resp = await client.get(
                f"{self.base_url}/companies/search",
                headers=self._headers(),
                params=params,
                timeout=self.timeout,
            )
        except httpx.HTTPError:
            # Network errors will be caught by tenacity if raised, but here we return None to retry logic
            # Actually, tenacity wraps the public method. If we catch here, we might hide errors.
            # Let's let tenacity handle connectivity issues at the fetch() level if possible,
            # but for internal helpers we should just let exceptions bubble up or handle specific status codes.
            raise

        if resp.status_code == 404:
            return None
        
        if 400 <= resp.status_code < 500:
            logger.warning(
                "OpenCorporates search returned %s: %s",
                resp.status_code,
                resp.text[:200],
            )
            return None

        try:
            resp.raise_for_status()
        except httpx.HTTPError:
            raise

        data = resp.json() or {}
        results = data.get("results", {})
        companies_list = results.get("companies") or []

        # Each element is {"company": {...}} per OC docs.
        best_match = None
        normalized_q = company_name.lower().strip()

        for wrapper in companies_list:
            comp = wrapper.get("company") or {}
            # Filter inactive if you prefer active entities
            # "inactive" field is boolean in OC responses usually
            if comp.get("inactive") is True:
                continue
            
            # Exact name match preference
            if (comp.get("name") or "").lower().strip() == normalized_q:
                best_match = comp
                break
            
            # Otherwise take the first active result if we haven't found one yet
            if not best_match:
                best_match = comp

        return best_match

    async def _fetch_company(
        self,
        client: httpx.AsyncClient,
        jurisdiction_code: str,
        company_number: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Call GET /companies/:jurisdiction_code/:company_number and
        return the normalized 'company' payload.
        """
        try:
            resp = await client.get(
                f"{self.base_url}/companies/{jurisdiction_code}/{company_number}",
                headers=self._headers(),
                timeout=self.timeout,
            )
        except httpx.HTTPError:
            raise

        if resp.status_code == 404:
            return None
        
        if 400 <= resp.status_code < 500:
            logger.warning(
                "OpenCorporates fetch returned %s: %s",
                resp.status_code,
                resp.text[:200],
            )
            return None

        try:
            resp.raise_for_status()
        except httpx.HTTPError:
            raise

        data = resp.json() or {}
        # Response format: { "results": { "company": { ... } } }
        raw_company = data.get("results", {}).get("company") or {}

        if not raw_company:
            return None

        identifiers: List[Dict[str, Any]] = []
        for item in raw_company.get("identifiers", []):
            ident = item.get("identifier") or {}
            if ident:
                identifiers.append({
                    "uid": ident.get("uid"),
                    "system_code": ident.get("identifier_system_code"),
                    "system_name": ident.get("identifier_system_name"),
                })

        filings: List[Dict[str, Any]] = []
        for f in raw_company.get("filings", []):
            filing = f.get("filing") or {}
            if not filing:
                continue
            filings.append({
                "provider": "open_corporates",
                "date": filing.get("date"),
                "title": filing.get("title"),
                "description": filing.get("description"),
                "opencorporates_url": filing.get("opencorporates_url"),
            })

        normalized = {
            "name": raw_company.get("name"),
            "company_number": raw_company.get("company_number"),
            "jurisdiction_code": raw_company.get("jurisdiction_code"),
            "company_type": raw_company.get("company_type"),
            "current_status": raw_company.get("current_status"),
            "incorporation_date": raw_company.get("incorporation_date"),
            "dissolution_date": raw_company.get("dissolution_date"),
            "registered_address": raw_company.get("registered_address"),
            "registered_address_in_full": raw_company.get("registered_address_in_full"),
            "registry_url": raw_company.get("registry_url"),
            "opencorporates_url": raw_company.get("opencorporates_url"),
            "identifiers": identifiers,
            "previous_names": raw_company.get("previous_names") or [],
            "source": raw_company.get("source") or {},
            "filings": filings,
        }
        return normalized

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    async def fetch(self, **kwargs: Any) -> ConnectorResult:
        """
        Expected kwargs:
          - company_name: str (preferred)
          - jurisdiction_code: Optional[str]
          - country_code: Optional[str]
          - company_number: Optional[str]

        Returns:
            ConnectorResult({
              "company": { ...normalized OpenCorporates company... }
            })
        """
        if not self.api_token:
            # It's valid to be unconfigured, just return empty.
            # Logger info might be too noisy if called often, debug is better.
            logger.debug("OPENCORPORATES_API_TOKEN not configured; skipping.")
            return ConnectorResult({})

        company_number = kwargs.get("company_number")
        jurisdiction_code = kwargs.get("jurisdiction_code")
        country_code = kwargs.get("country_code")
        company_name = (kwargs.get("company_name") or "").strip()

        # Cache key: prefer number+jurisdiction, else name-based
        cache_key = f"opencorporates:company:{jurisdiction_code or ''}:{company_number or company_name.lower()}"
        cached = await cached_get(cache_key)
        if cached is not None:
            return ConnectorResult(cached)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            company = None
            
            if company_number and jurisdiction_code:
                company = await self._fetch_company(client, jurisdiction_code, company_number)
            elif company_name:
                search_hit = await self._search_company(
                    client,
                    company_name=company_name,
                    jurisdiction_code=jurisdiction_code,
                    country_code=country_code,
                )
                
                if search_hit:
                    # Fetch full details using the search hit's coordinates
                    # The search hit itself is a summary, but details endpoint has filings/officers.
                    # Actually search results in OC often have basic info, but details endpoint is richer.
                    # The plan suggests calling fetch_company after search.
                    company = await self._fetch_company(
                        client,
                        search_hit["jurisdiction_code"],
                        search_hit["company_number"],
                    )
            else:
                logger.debug("OpenCorporates requires company_name or (jurisdiction_code+company_number).")
                return ConnectorResult({})

        if not company:
            return ConnectorResult({})

        result = {"company": company}
        # cache e.g. 7 days – registry data is stable
        await cached_get(cache_key, set_value=result, ttl=60 * 60 * 24 * 7)

        return ConnectorResult(result)

