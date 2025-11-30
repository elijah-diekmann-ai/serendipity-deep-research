from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from .base import BaseConnector, ConnectorResult
from ..caching import cached_get
from ...core.config import get_settings

settings = get_settings()


class CompaniesHouseConnector(BaseConnector):
    name = "companies_house"

    def __init__(self) -> None:
        self.api_key = settings.COMPANIES_HOUSE_API_KEY
        self.base_url = "https://api.company-information.service.gov.uk"
        self.search_items_per_page = 20
        self.officers_items_per_page = 50
        self.filings_items_per_page = 50

    async def _get_json_allow_404(
        self,
        client: httpx.AsyncClient,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Helper that:
        - returns None for 404 or non-retriable 4xx
        - handles 429 with a short backoff
        - raises for 5xx so Tenacity can retry
        """
        try:
            resp = await client.get(url, params=params)
        except httpx.HTTPError as e:
            # network / timeout errors are retriable
            raise e

        if resp.status_code == 404:
            return None

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else 5
            await asyncio.sleep(delay)
            try:
                resp = await client.get(url, params=params)
            except httpx.HTTPError as e:
                raise e

        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            # treat client-side errors as "no data"
            return None

        resp.raise_for_status()
        return resp.json()

    async def _fetch_company_details(
        self,
        client: httpx.AsyncClient,
        company_number: str,
    ) -> tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        profile_url = f"{self.base_url}/company/{company_number}"
        officers_url = f"{self.base_url}/company/{company_number}/officers"
        filings_url = f"{self.base_url}/company/{company_number}/filing-history"

        profile = await self._get_json_allow_404(client, profile_url)
        if not profile:
            return None, [], []

        officers_data = await self._get_json_allow_404(
            client,
            officers_url,
            params={"items_per_page": self.officers_items_per_page},
        )
        officers = officers_data.get("items", []) if officers_data else []

        filings_data = await self._get_json_allow_404(
            client,
            filings_url,
            params={
                "items_per_page": self.filings_items_per_page,
                "category": "accounts",  # focus on financial filings
            },
        )
        filings = filings_data.get("items", []) if filings_data else []

        return profile, officers, filings

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    async def fetch(self, query: str) -> ConnectorResult:
        if not self.api_key:
            return ConnectorResult({})

        cache_key = f"ch:{query}"
        cached = await cached_get(cache_key)
        if cached:
            return ConnectorResult(cached)

        async with httpx.AsyncClient(timeout=30, auth=(self.api_key, "")) as client:
            search_params: Dict[str, Any] = {
                "q": query,
                "items_per_page": self.search_items_per_page,
                # Prefer active companies when searching
                "restrictions": "active-companies",
            }
            search_url = f"{self.base_url}/search/companies"
            search_data = await self._get_json_allow_404(client, search_url, params=search_params)

            if not search_data:
                return ConnectorResult({})

            items = search_data.get("items", [])
            if not items:
                return ConnectorResult({})

            chosen_company: Optional[Dict[str, Any]] = None
            chosen_officers: List[Dict[str, Any]] = []
            chosen_filings: List[Dict[str, Any]] = []

            # Iterate results, preferring an active company
            for idx, company in enumerate(items):
                company_number = company.get("company_number")
                if not company_number:
                    continue

                profile, officers, filings = await self._fetch_company_details(client, company_number)
                if not profile:
                    continue

                status = profile.get("company_status")
                # First valid profile as fallback
                if chosen_company is None:
                    chosen_company = profile
                    chosen_officers = officers
                    chosen_filings = filings

                # Prefer active companies and stop early if found
                if status == "active":
                    chosen_company = profile
                    chosen_officers = officers
                    chosen_filings = filings
                    break

            if not chosen_company:
                return ConnectorResult({})

            result_data = {
                "company": chosen_company,
                "officers": chosen_officers,
                "filings": chosen_filings,
                "source": "companies_house",
            }

            await cached_get(cache_key, set_value=result_data, ttl=60 * 60 * 24 * 7)
            return ConnectorResult(result_data)
