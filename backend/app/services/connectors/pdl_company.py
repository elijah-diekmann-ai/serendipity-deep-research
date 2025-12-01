# backend/app/services/connectors/pdl_company.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
import logging
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

from .base import BaseConnector, ConnectorResult
from ..caching import cached_get
from ...core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class PDLCompanyConnector(BaseConnector):
    """
    PDL Company connector for accessing the Company Enrichment API.

    Responsibilities:
    - Fetch company firmographics and funding data using domain or name.
    - Return a structured result containing:
        * company: Normalized company profile.
        * funding_rollup: Aggregated funding metrics.
        * funding_details: Granular round data (if available).
        * snippets: Synthetic text snippets for the Writer.
    """

    name = "pdl_company"

    def __init__(self) -> None:
        self.base_url = "https://api.peopledatalabs.com/v5/company/enrich"

    def _headers(self) -> Dict[str, str]:
        return {
            "X-Api-Key": settings.PDL_API_KEY,
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip",
        }

    def _build_snippets(
        self,
        company_data: Dict[str, Any],
        funding_rollup: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Generate synthetic snippets from structured PDL data for the Writer to consume.
        """
        snippets: List[Dict[str, Any]] = []

        # 1. Firmographics Snippet
        # Fields: founded, location, size, website
        name = company_data.get("name") or "Target Company"
        founded = company_data.get("founded")
        website = company_data.get("website")
        
        # Location: try to get a nice string
        loc = company_data.get("location", {})
        # PDL often returns location as a dict, but sometimes keys vary.
        # We prioritize 'name' or constructed string.
        loc_name = (
            company_data.get("location_name") 
            or loc.get("name")
            or ", ".join(filter(None, [loc.get("locality"), loc.get("region"), loc.get("country")]))
        )
        
        # Size / Employees
        # PDL returns 'size' as a range (e.g. "11-50") and 'employee_count' as exact integer
        emp_count = company_data.get("employee_count")
        size_range = company_data.get("size")

        parts = []
        if founded:
            parts.append(f"Founded: {founded}")
        if loc_name:
            parts.append(f"HQ: {loc_name} (vendor aggregate)")
        if website:
            parts.append(f"Website: {website}")
        if emp_count:
            parts.append(f"Employee Count: {emp_count}")
        elif size_range:
            parts.append(f"Size Range: {size_range}")

        if parts:
            snippets.append({
                "provider": "pdl_company",
                "title": f"PDL company profile (founding/HQ) for {name}",
                "snippet": "; ".join(parts) + ".",
                "url": None,
            })

        # 2. Funding Roll-up Snippet
        # Use the rollup dict provided
        if funding_rollup:
            f_parts = []
            total = funding_rollup.get("total_funding_raised")
            if total:
                # Simple formatting
                f_parts.append(f"total_funding_raised=${total:,.2f}")
            
            rounds = funding_rollup.get("number_funding_rounds")
            if rounds:
                f_parts.append(f"rounds={rounds}")
            
            latest = funding_rollup.get("latest_funding_stage")
            if latest:
                f_parts.append(f"latest_stage={latest}")
                
            last_date = funding_rollup.get("last_funding_date")
            if last_date:
                f_parts.append(f"last_funding_date={last_date}")

            if f_parts:
                snippets.append({
                    "provider": "pdl_company",
                    "title": f"PDL company funding roll-up for {name}",
                    "snippet": "PDL aggregated: " + ", ".join(f_parts) + ".",
                    "url": None,
                })

        return snippets

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
    )
    async def fetch(self, **params: Any) -> ConnectorResult:
        """
        Fetch company enrichment data.

        Params:
            website (str): Preferred lookup key.
            company_name (str): Fallback lookup key.
        """
        if not settings.PDL_API_KEY:
            return ConnectorResult({})

        website = params.get("website")
        company_name = params.get("company_name")
        
        # Cache key construction
        cache_key = f"pdl_company:{website or company_name}"
        cached = await cached_get(cache_key)
        if cached:
            return ConnectorResult(cached)

        # Build query params
        query_params = {}
        if website:
            query_params["website"] = website
        elif company_name:
            query_params["name"] = company_name
        else:
            # No valid identifier provided
            return ConnectorResult({})
        
        # Optional: Ask for specific fields to reduce payload? 
        # PDL docs suggest 'pretty=true' for debug, but we want raw JSON.
        # For now, we just ask for everything by default to get funding data.

        async with httpx.AsyncClient(timeout=30) as client:
            try:
                resp = await client.get(
                    self.base_url,
                    headers=self._headers(),
                    params=query_params,
                )
            except httpx.HTTPError:
                return ConnectorResult({})

            if resp.status_code == 404:
                return ConnectorResult({})

            if resp.status_code == 429:
                # Let retry handler catch this by raising an exception
                resp.raise_for_status()
            
            if 400 <= resp.status_code < 500:
                # Client error (e.g. 400 Bad Request), likely invalid input
                return ConnectorResult({})

            resp.raise_for_status()
            data = resp.json()
            
            # Extract relevant sections
            # Top-level fields are the company object
            # Funding fields are also top-level but we group them for clarity
            
            # 1. Company Object (Subset)
            company_obj = {
                "name": data.get("name"),
                "website": data.get("website"),
                "founded": data.get("founded"),
                "location_name": data.get("location", {}).get("name") or data.get("location_name"),
                "linkedin_url": data.get("linkedin_url"),
                "employee_count": data.get("employee_count"),
                "size": data.get("size"),
                "industry": data.get("industry"),
                "summary": data.get("summary"),
            }

            # 2. Funding Roll-up
            # Check for total_funding_raised, number_funding_rounds, etc.
            funding_rollup = {}
            if "total_funding_raised" in data:
                funding_rollup["total_funding_raised"] = data["total_funding_raised"]
            if "number_funding_rounds" in data:
                funding_rollup["number_funding_rounds"] = data["number_funding_rounds"]
            if "latest_funding_stage" in data:
                funding_rollup["latest_funding_stage"] = data["latest_funding_stage"]
            if "last_funding_date" in data:
                funding_rollup["last_funding_date"] = data["last_funding_date"]

            # 3. Funding Details
            # Some PDL tiers provide a 'funding_details' list or similar structure.
            # We'll capture it if present.
            funding_details = data.get("funding_details", [])

            # 4. Snippets
            snippets = self._build_snippets(company_obj, funding_rollup)

            result = {
                "company": company_obj,
                "funding_rollup": funding_rollup,
                "funding_details": funding_details,
                "snippets": snippets,
            }

            # Cache for 7 days
            await cached_get(cache_key, set_value=result, ttl=60 * 60 * 24 * 7)
            
            return ConnectorResult(result)

