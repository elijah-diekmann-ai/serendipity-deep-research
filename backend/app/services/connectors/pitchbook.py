# backend/app/services/connectors/pitchbook.py

# No subscription so idle for now (costly)

from __future__ import annotations

from typing import Any, Dict, List, Optional

import logging

import httpx

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from .base import BaseConnector, ConnectorResult

from ...core.config import get_settings

from ..caching import cached_get

logger = logging.getLogger(__name__)

settings = get_settings()


class PitchbookConnector(BaseConnector):
    """
    PitchBook connector focused on funding / deal data for a single company.

    When configured, it should:
    - Resolve the target company in PitchBook (by name and/or domain).
    - Fetch funding/deal records.
    - Normalise them into:
        - 'funding_rounds': a list of structured rounds
        - 'snippets': text snippets describing those rounds for the Writer.
    """

    name = "pitchbook"

    def __init__(self) -> None:
        self.api_key: Optional[str] = settings.PITCHBOOK_API_KEY
        self.base_url: str = settings.PITCHBOOK_BASE_URL
        self.timeout: int = settings.PITCHBOOK_TIMEOUT_SECONDS
        self.max_results: int = settings.PITCHBOOK_MAX_RESULTS

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    async def fetch(self, **kwargs: Any) -> ConnectorResult:
        """
        Expected kwargs:
          - company_name: Optional[str]
          - company_domain: Optional[str]

        Returns:
            ConnectorResult({
              "funding_rounds": [...],   # structured rows
              "snippets": [...],         # writer-ready snippets
            })
        """
        if not self.api_key:
            # Current state: no subscription, so this is effectively idle.
            logger.debug("PITCHBOOK_API_KEY not configured; returning empty result.")
            return ConnectorResult({})

        company_name: str | None = (kwargs.get("company_name") or "").strip() or None
        company_domain: str | None = (kwargs.get("company_domain") or "").strip() or None

        if not company_name and not company_domain:
            logger.debug("PitchBook fetch requires company_name or company_domain.")
            return ConnectorResult({})

        cache_key = f"pitchbook:fundraising:{company_domain or ''}|{company_name or ''}|size={self.max_results}"
        cached = await cached_get(cache_key)
        if cached is not None:
            return ConnectorResult(cached)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            # 1) Resolve company in PitchBook
            # pb_company_id = await self._resolve_company(client, company_name, company_domain)

            # 2) Fetch deals/funding rounds
            # raw_rounds = await self._fetch_deals_for_company(client, pb_company_id)

            # 3) Normalise to internal format + snippets
            # funding_rounds, snippets = self._normalise_rounds(raw_rounds)

            result = {
                "funding_rounds": [],  # placeholder until implemented
                "snippets": [],        # placeholder until implemented
            }

        await cached_get(cache_key, set_value=result, ttl=60 * 60 * 24 * 7)
        return ConnectorResult(result)

