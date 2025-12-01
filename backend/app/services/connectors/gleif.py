# backend/app/services/connectors/gleif.py

from __future__ import annotations

from typing import Any, Dict, Optional, List

import logging

import httpx

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from .base import BaseConnector, ConnectorResult

from ...core.config import get_settings

from ..caching import cached_get

logger = logging.getLogger(__name__)

settings = get_settings()

class GLEIFConnector(BaseConnector):
    name = "gleif"

    def __init__(self) -> None:
        self.base_url: str = getattr(
            settings,
            "GLEIF_BASE_URL",
            "https://api.gleif.org/api/v1",
        )
        self.timeout: int = int(
            getattr(settings, "GLEIF_TIMEOUT_SECONDS", 20) or 20
        )
        self.max_results: int = int(
            getattr(settings, "GLEIF_MAX_RESULTS", 3) or 3
        )

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(httpx.HTTPError),
    )
    async def fetch(self, **kwargs: Any) -> ConnectorResult:
        """
        Expected kwargs:
          - company_name: str (preferred)
          - country_code: Optional[str]
          - lei: Optional[str]
          - bic: Optional[str]

        Returns:
            ConnectorResult({
              "company": { ...normalized GLEIF company... },
              "snippets": [ ... ]
            })
        """
        company_name = (kwargs.get("company_name") or "").strip()
        country_code = kwargs.get("country_code")
        lei = kwargs.get("lei")
        bic = kwargs.get("bic")

        if not any([company_name, lei, bic]):
            # Valid case (no search params provided), just return empty
            return ConnectorResult({})

        cache_key = (
            f"gleif:lei-records:"
            f"name={company_name or ''}|"
            f"country={country_code or ''}|"
            f"lei={lei or ''}|"
            f"bic={bic or ''}|"
            f"size={self.max_results}"
        )

        cached = await cached_get(cache_key)
        if cached is not None:
            return ConnectorResult(cached)

        # Build query params using GLEIF filter syntax
        params: Dict[str, Any] = {
            "page[size]": self.max_results,
            "page[number]": 1,
        }

        if lei:
            params["filter[lei]"] = lei
        elif bic:
            params["filter[bic]"] = bic
        elif company_name:
            params["filter[entity.legalName]"] = company_name
            # Prefer issued (active) LEIs when searching by name
            params["filter[registration.status]"] = "ISSUED"

        if country_code:
            params["filter[entity.legalAddress.country]"] = country_code

        domain_hint = (kwargs.get("company_domain") or "").strip().lower()

        def _domain_tokens(domain: str) -> List[str]:
            if not domain:
                return []
            stripped = domain
            if "://" in stripped:
                stripped = stripped.split("://", 1)[1]
            stripped = stripped.split("/", 1)[0]
            stripped = stripped.replace("www.", "")
            tokens = [tok for tok in stripped.split(".") if tok]
            # drop common tlds
            blacklist = {"com", "net", "org", "io", "co", "ai"}
            return [tok for tok in tokens if tok not in blacklist]

        domain_tokens = _domain_tokens(domain_hint)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                resp = await client.get(
                    f"{self.base_url}/lei-records",
                    params=params,
                )
            except httpx.HTTPError:
                raise

            if resp.status_code == 404:
                return ConnectorResult({})
            
            if 400 <= resp.status_code < 500:
                logger.warning(
                    "GLEIF search returned %s: %s",
                    resp.status_code,
                    resp.text[:200],
                )
                return ConnectorResult({})

            try:
                resp.raise_for_status()
            except httpx.HTTPError:
                raise

            body = resp.json()
            records = body.get("data") or []

            if not records:
                return ConnectorResult({})

            normalized_q = company_name.lower() if company_name else ""

            def _score_candidate(
                legal_name: str,
                registration: Dict[str, Any],
                jurisdiction: Optional[str],
                legal_address: Dict[str, Any],
            ) -> float:
                score = 0.0
                status = (registration or {}).get("status")
                if status == "ISSUED":
                    score += 2.0

                if normalized_q and legal_name == normalized_q:
                    score += 3.0
                elif normalized_q and normalized_q in legal_name:
                    score += 1.5

                if country_code and jurisdiction and jurisdiction.upper().startswith(country_code.upper()):
                    score += 1.5

                addr_country = (legal_address or {}).get("country")
                if country_code and addr_country and addr_country.upper().startswith(country_code.upper()):
                    score += 1.0

                if domain_tokens:
                    for tok in domain_tokens:
                        if tok and tok in legal_name:
                            score += 1.0
                            break

                return score

            candidates: List[Dict[str, Any]] = []
            snippets: List[Dict[str, Any]] = []
            best_candidate: Optional[Dict[str, Any]] = None
            best_score = float("-inf")

            for rec in records:
                attrs = rec.get("attributes") or {}
                entity = attrs.get("entity") or {}
                legal_address = entity.get("legalAddress") or {}
                hq_address = entity.get("headquartersAddress") or {}
                reg = attrs.get("registration") or {}
                ra = entity.get("registrationAuthority") or {}

                legal_name_val = entity.get("legalName", {}).get("name") or ""
                legal_name_lower = legal_name_val.lower()
                jurisdiction = entity.get("legalJurisdiction")

                candidate: Dict[str, Any] = {
                    "lei": attrs.get("lei"),
                    "legal_name": legal_name_val,
                    "legal_jurisdiction": jurisdiction,
                    "entity_category": entity.get("category"),
                    "entity_status": entity.get("status"),
                    "legal_address": {
                        "city": legal_address.get("city"),
                        "region": legal_address.get("region"),
                        "country": legal_address.get("country"),
                        "postal_code": legal_address.get("postalCode"),
                        "lines": legal_address.get("addressLines") or [],
                    },
                    "headquarters_address": {
                        "city": hq_address.get("city"),
                        "region": hq_address.get("region"),
                        "country": hq_address.get("country"),
                        "postal_code": hq_address.get("postalCode"),
                        "lines": hq_address.get("addressLines") or [],
                    },
                    "registration_authority_id": ra.get("registrationAuthorityID"),
                    "registration_authority_entity_id": ra.get("registrationAuthorityEntityID"),
                    "registration": {
                        "status": reg.get("status"),
                        "initial_registration_date": reg.get("initialRegistrationDate"),
                        "last_update_date": reg.get("lastUpdateDate"),
                        "next_renewal_date": reg.get("nextRenewalDate"),
                        "managing_lou": reg.get("managingLOU"),
                    },
                }

                score = _score_candidate(legal_name_lower, reg, jurisdiction, candidate["legal_address"])
                candidate["base_score"] = score
                candidates.append(candidate)

                snippet_lines = []
                if candidate["legal_name"]:
                    snippet_lines.append(f"Legal name: {candidate['legal_name']}")
                if candidate["lei"]:
                    snippet_lines.append(f"LEI: {candidate['lei']}")
                if candidate["legal_jurisdiction"]:
                    snippet_lines.append(f"Legal jurisdiction: {candidate['legal_jurisdiction']}")

                ra_id = candidate.get("registration_authority_id")
                ra_entity_id = candidate.get("registration_authority_entity_id")
                if ra_id or ra_entity_id:
                    snippet_lines.append(
                        f"Registration authority: {ra_id or 'N/A'} "
                        f"(local ID: {ra_entity_id or 'N/A'})"
                    )

                city = candidate["legal_address"].get("city")
                region = candidate["legal_address"].get("region")
                country = candidate["legal_address"].get("country")
                postal_code = candidate["legal_address"].get("postal_code")

                if any([city, region, country, postal_code]):
                    addr_str = f"{city or ''}, {region or ''}, {country or ''} {postal_code or ''}".strip().replace(" ,", ",")
                    snippet_lines.append(f"Registered address: {addr_str}")

                reg_info = candidate.get("registration") or {}
                if reg_info.get("status"):
                    snippet_lines.append(f"LEI registration status: {reg_info['status']}")
                if reg_info.get("initial_registration_date"):
                    snippet_lines.append(
                        f"LEI first issued: {reg_info['initial_registration_date']}"
                    )

                snippets.append(
                    {
                        "provider": "gleif",
                        "title": f"GLEIF LEI record for {candidate.get('legal_name') or 'entity'}",
                        "snippet": "\n".join(snippet_lines),
                        "url": f"https://search.gleif.org/#/record/{candidate['lei']}" if candidate.get("lei") else None,
                    }
                )

                if score > best_score:
                    best_score = score
                    best_candidate = candidate

            if not candidates or not best_candidate:
                return ConnectorResult({})

            result_data = {
                "company": best_candidate,
                "candidates": candidates,
                "snippets": snippets,
            }

            # 14 days: LEI data stable
            await cached_get(cache_key, set_value=result_data, ttl=60 * 60 * 24 * 14)

            return ConnectorResult(result_data)

