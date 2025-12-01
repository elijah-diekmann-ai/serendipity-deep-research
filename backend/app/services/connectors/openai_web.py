# backend/app/services/connectors/openai_web.py

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .base import BaseConnector, ConnectorResult
from ..caching import cached_get
from ...core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


class OpenAIWebSearchConnector(BaseConnector):
    """
    Connector that uses OpenAI's `web_search` tool via the Responses API.

    This is designed as a *reasoning-first* complement to Exa:
    - Exa is used for high-recall similarity and date-filtered news / filings.
    - This connector is used for tasks that require broad world knowledge,
      filtering, and strategic reasoning – starting with Competitor discovery.

    The connector normalises its output into the same "snippets" structure used
    elsewhere so that entity_resolution + Writer can treat OpenAI and Exa
    results uniformly.

    Returned payload shape:
        {
          "snippets": [
             {
               "url": ...,
               "title": ...,
               "snippet": ...,
               "domain": ...,
               "provider": "openai-web",
               "published_date": null
             },
             ...
          ],
          # Optional structured extras used by downstream components:
          "competitors": [ ... ],
          "founding_facts": { ... }
        }
    """

    name = "openai_web"

    def __init__(self) -> None:
        # We keep OpenAI credentials separate from the generic LLM client
        # (which may be routed via OpenRouter).
        self._api_key: Optional[str] = getattr(settings, "OPENAI_API_KEY", None) or os.getenv("OPENAI_API_KEY")

        # Optional: distinct model for web search; falls back to a sensible default.
        self._model: str = (
            getattr(settings, "OPENAI_WEB_MODEL", None)
            or os.getenv("OPENAI_WEB_MODEL")
            or "gpt-5"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _has_credentials(self) -> bool:
        return bool(self._api_key and OpenAI is not None)

    def _build_competitor_prompt(
        self,
        company_name: str,
        website: str,
        context: str,
    ) -> str:
        """
        Prompt for agentic competitor discovery.
        """
        target_desc_lines = []
        if company_name:
            target_desc_lines.append(f"- Name: {company_name}")
        if website:
            target_desc_lines.append(f"- Website: {website}")
        if context:
            target_desc_lines.append(f"- Additional context: {context}")
        target_block = "\\n".join(target_desc_lines) if target_desc_lines else "N/A"

        return (
            "You are a strategy consultant helping a venture investor understand the competitive landscape "
            "around a single target company.\n\n"
            "Use the web_search tool to identify the 5–10 most relevant competing companies globally.\n"
            "Focus on *true* competitors that a sophisticated buyer might evaluate in the same short list, "
            "not generic companies in the broad industry and not investors, directories, or customers.\n\n"
            "For each competitor you keep, you MUST:\n"
            "- Confirm that they actually sell a product or service that could realistically substitute for "
            "the target's offering for at least some customers.\n"
            "- Prefer companies of similar layer (infrastructure vs. application), similar business model, and "
            "similar buyer persona.\n"
            "- Capture any visible signals on technology stack, data/AI usage, IP, or supply-chain position, "
            "so that an analyst can reason about moats and strategic risk.\n\n"
            "Return your answer as a single JSON object with this exact shape:\n"
            "{\n"
            '  "competitors": [\n'
            "    {\n"
            '      "name": "Competitor name",\n'
            '      "website": "https://...",\n'
            '      "category": "direct" | "adjacent" | "substitute",\n'
            '      "summary": "1-3 sentence comparison of what they do and how they differ from the target.",\n'
            '      "why_relevant": "Short phrase on the overlap with the target\'s product, customers or use cases.",\n'
            '      "tech_and_moat": "Short phrase on technology, data, IP, and moat strength (or weakness).",\n'
            '      "geo_focus": "Primary geography if obvious, else null."\n'
            "    }\n"
            "  ]\n"
            "}\n\n"
            "CRITICAL RULES:\n"
            "- Only include competitors that you can justify from the web search results.\n"
            "- Do NOT include the target company itself.\n"
            "- If you are uncertain whether an entity is a competitor, leave it out.\n"
            "- The response must be valid JSON. Do not include comments, markdown, or prose outside the JSON.\n\n"
            f"TARGET COMPANY INFORMATION:\\n{target_block}\n"
        )

    def _build_founding_prompt(
        self,
        company_name: str,
        website: str,
        context: str,
    ) -> str:
        """
        Prompt for finding strict legal/founding facts when registries (GLEIF) are missing.
        """
        target_desc_lines = []
        if company_name:
            target_desc_lines.append(f"- Name: {company_name}")
        if website:
            target_desc_lines.append(f"- Website: {website}")
        if context:
            target_desc_lines.append(f"- Additional context: {context}")
        target_block = "\\n".join(target_desc_lines) if target_desc_lines else "N/A"

        return (
            "You are a corporate research assistant helping to establish the legal identity of a company.\n\n"
            "Use the web_search tool to find definitive legal/corporate facts about the target company.\n"
            "Prioritise the following sources for evidence:\n"
            "- The company's own legal pages (Terms, Privacy, Imprint/Impressum, Legal).\n"
            "- University tech-transfer or spin-out pages (if applicable).\n"
            "- SEC/EDGAR filings (10-K, S-1) or other credible government/regulatory portals.\n"
            "- Government grant portals (SBIR, NIH, NSF, etc.).\n\n"
            "Extract the following fields if visible in credible sources:\n"
            "- legal_name: The full legal entity name (e.g. 'Acme Robotics, Inc.').\n"
            "- incorporation_date: The date of incorporation (YYYY-MM-DD) if explicitly stated.\n"
            "- jurisdiction: Country and state/region of incorporation.\n"
            "- registered_address: The full registered office address.\n"
            "- registration_numbers: Any corporate IDs (company number, EIN, ABN, CIK, etc.) with system name.\n"
            "- hq: The headquarters city/region/country.\n"
            "- origin_context: Brief note if it is a spin-out, carve-out, or university project.\n"
            "- ownership_notes: Brief note on ownership structure if visible.\n\n"
            "Also capture the specific URLs where you found these facts as 'evidence'.\n\n"
            "Return your answer as a single JSON object with this exact shape:\n"
            "{\n"
            '  "founding_facts": {\n'
            '    "legal_name": "...",\n'
            '    "incorporation_date": "YYYY-MM-DD" | null,\n'
            '    "jurisdiction": "..." | null,\n'
            '    "registered_address": "..." | null,\n'
            '    "registration_numbers": [{"system": "...", "id": "..."}] | [],\n'
            '    "hq": "..." | null,\n'
            '    "origin_context": "..." | null,\n'
            '    "ownership_notes": "..." | null\n'
            "  },\n"
            '  "evidence": [\n'
            '    {"url": "...", "title": "...", "snippet": "..."}\n'
            "  ]\n"
            "}\n\n"
            "CRITICAL RULES:\n"
            "- Only return facts you can verify with a citation.\n"
            "- If a field is not found, set it to null (or empty list).\n"
            "- The response must be valid JSON. Do not include comments, markdown, or prose outside the JSON.\n\n"
            f"TARGET COMPANY INFORMATION:\\n{target_block}\n"
        )

    def _parse_competitor_json(self, raw: str) -> List[Dict[str, Any]]:
        """
        Robustly extract the 'competitors' list from a JSON-ish string.
        """
        if not raw:
            return []

        data: Dict[str, Any] = {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            # Try to salvage by extracting the outermost {...} block.
            start = raw.find("{")
            end = raw.rfind("}")
            if start != -1 and end != -1 and end > start:
                try:
                    data = json.loads(raw[start : end + 1])
                except json.JSONDecodeError:
                    logger.warning("OpenAIWebSearchConnector: failed to parse competitor JSON.")
                    return []

        comps = data.get("competitors")
        if not isinstance(comps, list):
            return []

        normalised: List[Dict[str, Any]] = []
        for c in comps:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name") or "").strip()
            if not name:
                continue
            website = c.get("website")
            if isinstance(website, str):
                website = website.strip() or None
            else:
                website = None

            category = str((c.get("category") or "direct")).strip().lower()
            if category not in {"direct", "adjacent", "substitute"}:
                category = "direct"

            summary = str(c.get("summary") or "").strip()
            why_relevant = str(c.get("why_relevant") or "").strip()
            tech_and_moat = str(c.get("tech_and_moat") or "").strip()
            geo_focus = c.get("geo_focus")
            if isinstance(geo_focus, str):
                geo_focus = geo_focus.strip() or None
            else:
                geo_focus = None

            normalised.append(
                {
                    "name": name,
                    "website": website,
                    "category": category,
                    "summary": summary,
                    "why_relevant": why_relevant,
                    "tech_and_moat": tech_and_moat,
                    "geo_focus": geo_focus,
                }
            )
        return normalised

    def _parse_founding_json(self, raw: str) -> Dict[str, Any]:
        """
        Robustly extract 'founding_facts' and 'evidence' from a JSON-ish string.
        """
        if not raw:
            return {}

        data: Dict[str, Any] = {}
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            start = raw.find("{")
            end = raw.rfind("}")
            if start != -1 and end != -1 and end > start:
                try:
                    data = json.loads(raw[start : end + 1])
                except json.JSONDecodeError:
                    logger.warning("OpenAIWebSearchConnector: failed to parse founding JSON.")
                    return {}

        # Basic validation
        founding_facts = data.get("founding_facts")
        evidence = data.get("evidence")

        if not isinstance(founding_facts, dict):
            founding_facts = {}
        if not isinstance(evidence, list):
            evidence = []

        return {"founding_facts": founding_facts, "evidence": evidence}

    def _competitors_to_snippets(self, competitors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Turn structured competitors into snippet objects for downstream use.
        """
        snippets: List[Dict[str, Any]] = []
        for comp in competitors:
            name = comp.get("name") or "Competitor"
            website = comp.get("website") or ""
            summary = comp.get("summary") or ""
            why_relevant = comp.get("why_relevant") or ""
            tech_and_moat = comp.get("tech_and_moat") or ""
            geo_focus = comp.get("geo_focus")

            parts: List[str] = []
            if summary:
                parts.append(summary)
            if why_relevant:
                parts.append(f"Relevance vs target: {why_relevant}")
            if tech_and_moat:
                parts.append(f"Tech & moat: {tech_and_moat}")
            if geo_focus:
                parts.append(f"Geo focus: {geo_focus}")

            snippet_text = " ".join(p for p in parts if p).strip()
            domain = None
            if website:
                try:
                    parsed = urlparse(website if "://" in website else "https://" + website)
                    domain = parsed.netloc or None
                except Exception:
                    domain = None

            snippets.append(
                {
                    "url": website or None,
                    "title": name,
                    "snippet": snippet_text,
                    "domain": domain,
                    "provider": "openai-web",
                    "published_date": None,
                }
            )
        return snippets

    def _founding_evidence_to_snippets(self, evidence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Turn founding evidence list into standard snippets.
        """
        snippets: List[Dict[str, Any]] = []
        for item in evidence:
            if not isinstance(item, dict):
                continue
            url = item.get("url")
            title = item.get("title") or "Founding Evidence"
            snippet_text = item.get("snippet") or ""

            domain = None
            if url:
                try:
                    parsed = urlparse(url if "://" in url else "https://" + url)
                    domain = parsed.netloc or None
                except Exception:
                    domain = None

            snippets.append(
                {
                    "url": url,
                    "title": title,
                    "snippet": snippet_text,
                    "domain": domain,
                    "provider": "openai-web",
                    "published_date": None,
                }
            )
        return snippets

    # ------------------------------------------------------------------
    # Core fetch
    # ------------------------------------------------------------------

    async def fetch(self, **params: Any) -> ConnectorResult:
        """
        Unified entrypoint expected by the orchestrator.

        Supported modes (params["mode"]):
        - "competitors": high-level competitor discovery for the target company.
        - "founding": deep search for legal/corporate identity facts.

        Additional params:
        - company_name: str
        - website: str
        - context: str (optional free-form description from the user)

        Returns:
            ConnectorResult({"snippets": [...], "competitors": [...], "founding_facts": {...}})
        """
        if not self._has_credentials():
            logger.info(
                "OpenAIWebSearchConnector disabled (no API key or OpenAI client). Returning empty result."
            )
            return ConnectorResult({})

        mode = (params.get("mode") or "competitors").lower()
        company_name = str(params.get("company_name") or "").strip()
        website = str(params.get("website") or "").strip()
        context = str(params.get("context") or "").strip()

        cache_key_parts = ["openai_web", mode]
        if company_name:
            cache_key_parts.append(f"name:{company_name.lower()}")
        if website:
            cache_key_parts.append(f"site:{website.lower()}")
        cache_key = "|".join(cache_key_parts)

        cached = await cached_get(cache_key)
        if cached is not None:
            return ConnectorResult(cached)

        # Dispatch prompt generation based on mode
        if mode == "competitors":
            prompt = self._build_competitor_prompt(company_name, website, context)
        elif mode == "founding":
            prompt = self._build_founding_prompt(company_name, website, context)
        else:
            logger.warning(
                "OpenAIWebSearchConnector called with unsupported mode '%s'; returning empty result.",
                mode,
            )
            return ConnectorResult({})

        def _call_openai_sync() -> Dict[str, Any]:
            assert OpenAI is not None  # guarded in _has_credentials
            client = OpenAI(api_key=self._api_key)

            try:
                response = client.responses.create(
                    model=self._model,
                    reasoning={"effort": "low"},
                    tools=[{"type": "web_search"}],
                    tool_choice="auto",
                    input=prompt,
                )
            except Exception as e:
                logger.exception("OpenAI web_search call failed: %s", e)
                return {}

            # Prefer the convenient helper if available
            raw_text: Optional[str] = getattr(response, "output_text", None)
            if not raw_text:
                # Fallback to the first text block in the output
                try:
                    # responses API uses "output" list in newer versions
                    if getattr(response, "output", None):
                        first_item = response.output[0]
                        if first_item and first_item.content:
                            raw_text = first_item.content[0].text  # type: ignore[attr-defined]

                    # Older preview: check top-level choices/messages if present
                    if not raw_text and getattr(response, "choices", None):
                        raw_text = response.choices[0].message.content  # type: ignore[attr-defined]
                except Exception:
                    raw_text = None

            if mode == "competitors":
                competitors = self._parse_competitor_json(raw_text or "")
                snippets = self._competitors_to_snippets(competitors)
                return {
                    "snippets": snippets,
                    "competitors": competitors,
                }
            elif mode == "founding":
                parsed = self._parse_founding_json(raw_text or "")
                snippets = self._founding_evidence_to_snippets(parsed.get("evidence", []))
                return {
                    "snippets": snippets,
                    "founding_facts": parsed.get("founding_facts", {}),
                }
            return {}

        result: Dict[str, Any] = await asyncio.to_thread(_call_openai_sync)

        if result:
            # Cache for 24h – competitor set and founding facts are relatively stable.
            await cached_get(cache_key, set_value=result, ttl=60 * 60 * 24)

        return ConnectorResult(result)
