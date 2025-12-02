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
from ..llm_costs import cost_for_tokens, cost_for_web_search_calls
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
          "founding_facts": { ... },
          "people_web": [ ... ]  # New in leadership mode
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

    def _build_person_prompt(
        self,
        person_name: str,
        company_name: str,
        context: str,
    ) -> str:
        """
        Prompt for gathering biography and career history for a specific person.
        """
        target_desc_lines = []
        if person_name:
            target_desc_lines.append(f"- Name: {person_name}")
        if company_name:
            target_desc_lines.append(f"- Associated Company: {company_name}")
        if context:
            target_desc_lines.append(f"- Additional context: {context}")
        target_block = "\\n".join(target_desc_lines) if target_desc_lines else "N/A"

        return (
            "You are an executive recruiter researcher profiling a specific individual.\n\n"
            "Use the web_search tool to find a detailed biography and career history for the target person.\n"
            "Focus on:\n"
            "- Current role and responsibilities.\n"
            "- Career timeline (previous roles, companies, dates).\n"
            "- Education (degrees, institutions, years).\n"
            "- Notable achievements, boards, or investments.\n\n"
            "Return a JSON object with this exact shape:\n"
            "{\n"
            '  "person": {\n'
            '    "name": "Full Name",\n'
            '    "current_role": "...",\n'
            '    "current_company": "...",\n'
            '    "summary": "2-4 sentence professional bio.",\n'
            '    "linkedin_url": "https://..." | null\n'
            "  },\n"
            '  "timeline": [\n'
            '    {"role": "...", "company": "...", "start": "YYYY", "end": "YYYY", "description": "..."}\n'
            "  ],\n"
            '  "evidence": [\n'
            '    {"url": "...", "title": "...", "snippet": "..."}\n'
            "  ]\n"
            "}\n\n"
            "CRITICAL RULES:\n"
            "- Be careful with common names; ensure the facts match the company/context provided.\n"
            "- If you find conflicting info, use the most recent/credible source.\n"
            "- The response must be valid JSON.\n\n"
            f"TARGET PERSON INFORMATION:\\n{target_block}\n"
        )

    def _parse_person_json(self, raw: str) -> Dict[str, Any]:
        """
        Robustly extract person bio data.
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
                    return {}

        return data

    def _person_bio_to_snippets(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert person JSON to snippets.
        """
        snippets: List[Dict[str, Any]] = []
        
        # Evidence snippets
        evidence = data.get("evidence")
        if isinstance(evidence, list):
            for item in evidence:
                if not isinstance(item, dict):
                    continue
                url = item.get("url")
                title = item.get("title") or "Biography Evidence"
                snippet_text = item.get("snippet") or ""
                
                domain = None
                if url:
                    try:
                        parsed = urlparse(url if "://" in url else "https://" + url)
                        domain = parsed.netloc or None
                    except Exception:
                        pass

                snippets.append({
                    "url": url,
                    "title": title,
                    "snippet": snippet_text,
                    "domain": domain,
                    "provider": "openai-web",
                    "published_date": None,
                })
                
        # Synthetic snippet from summary
        person = data.get("person")
        if isinstance(person, dict):
            summary = person.get("summary")
            if summary:
                snippets.append({
                    "url": None,
                    "title": f"OpenAI-generated Bio for {person.get('name')}",
                    "snippet": summary,
                    "domain": "openai-web",
                    "provider": "openai-web",
                    "published_date": None,
                })
                
        return snippets

    def _build_leadership_prompt(
        self,
        company_name: str,
        website: str,
        context: str,
    ) -> str:
        """
        Prompt for identifying founders and key executives (fallback to PDL).
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
            "You are a corporate research assistant focusing on founders and leadership.\n\n"
            "Use the web_search tool to identify the company's founders, CEO, CTO, and other key leaders.\n"
            "Return a JSON object:\n"
            "{\n"
            '  "people": [\n'
            "    {\n"
            '      "name": "Full name",\n'
            '      "role": "Primary role/title",\n'
            '      "summary": "2-4 sentence biography focusing on current role, prior employers, and domain expertise.",\n'
            '      "evidence": [\n'
            '         {"url": "https://...", "title": "...", "snippet": "short supporting quote or description"}\n'
            "      ]\n"
            "    }\n"
            "  ]\n"
            "}\n\n"
            "Do not include the target company itself as a person. Only include individuals.\n"
            "The response must be valid JSON with no extra commentary.\n\n"
            f"TARGET COMPANY INFORMATION:\\n{target_block}\n"
        )

    def _build_news_prompt(
        self,
        company_name: str,
        website: str,
        context: str,
    ) -> str:
        """
        Prompt for gathering top news items via agentic search.
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
            "You are a market intelligence researcher tracking a specific company.\n\n"
            "Use the web_search tool to gather the 5–15 most important news items about the target company "
            "over roughly the last 12–24 months.\n"
            "Focus on material events: funding, product launches, partnerships, regulatory actions, M&A, "
            "layoffs, leadership changes, expansions/closures.\n\n"
            "Return strict JSON with this exact shape:\n"
            "{\n"
            '  "news": [\n'
            "    {\n"
            '      "date": "YYYY-MM-DD",\n'
            '      "title": "Event title",\n'
            '      "url": "https://...",\n'
            '      "source": "domain.com",\n'
            '      "kind": "funding | product | partnership | regulatory | m&a | layoffs | leadership | other",\n'
            '      "summary": "1–3 sentence summary of the event."\n'
            "    }\n"
            "  ]\n"
            "}\n\n"
            "CRITICAL RULES:\n"
            "- Include only news specifically about the target company.\n"
            "- If dates are fuzzy, use the best available approximation (YYYY-MM-DD).\n"
            "- The response must be valid JSON. Do not include comments or markdown.\n\n"
            f"TARGET COMPANY INFORMATION:\\n{target_block}\n"
        )

    def _parse_news_json(self, raw: str) -> List[Dict[str, Any]]:
        """
        Robustly extract 'news' list from JSON-ish string.
        """
        if not raw:
            return []

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
                    logger.warning("OpenAIWebSearchConnector: failed to parse news JSON.")
                    return []
            else:
                return []

        news_items = data.get("news")
        if not isinstance(news_items, list):
            return []

        # Sanitize items
        clean_items: List[Dict[str, Any]] = []
        for item in news_items:
            if not isinstance(item, dict):
                continue
            clean_items.append({
                "date": str(item.get("date") or ""),
                "title": str(item.get("title") or "News Event"),
                "url": str(item.get("url") or ""),
                "source": str(item.get("source") or ""),
                "kind": str(item.get("kind") or "other"),
                "summary": str(item.get("summary") or "")
            })
        return clean_items

    def _news_to_snippets(self, news_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Turn structured news items into standard snippets.
        """
        snippets: List[Dict[str, Any]] = []
        for item in news_items:
            url = item.get("url")
            domain = None
            if url:
                try:
                    parsed = urlparse(url if "://" in url else "https://" + url)
                    domain = parsed.netloc or None
                except Exception:
                    pass
            
            # Fallback to 'source' field if URL parsing fails or is missing
            if not domain and item.get("source"):
                domain = item["source"]

            snippet_text = f"{item.get('date')} [{item.get('kind')}]: {item.get('summary')}"
            snippets.append({
                "url": url or None,
                "title": item.get("title"),
                "snippet": snippet_text,
                "domain": domain,
                "provider": "openai-web",
                "published_date": item.get("date") or None,
            })
        return snippets

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

    def _parse_leadership_json(self, raw: str) -> Dict[str, Any]:
        """
        Robustly extract 'people' and 'evidence' from leadership search JSON.
        """
        if not raw:
            return {"people": [], "evidence_snippets": []}

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
                    logger.warning("OpenAIWebSearchConnector: failed to parse leadership JSON.")
                    return {"people": [], "evidence_snippets": []}
            else:
                return {"people": [], "evidence_snippets": []}

        people = data.get("people")
        if not isinstance(people, list):
            people = []

        evidence_snippets: List[Dict[str, Any]] = []
        for p in people:
            if not isinstance(p, dict):
                continue
            
            ev_list = p.get("evidence")
            if not isinstance(ev_list, list):
                continue

            for ev in ev_list:
                if not isinstance(ev, dict):
                    continue

                url = ev.get("url")
                title = ev.get("title") or f"Leadership evidence for {p.get('name')}"
                snippet = ev.get("snippet") or ""

                domain = None
                if url:
                    try:
                        parsed = urlparse(url if "://" in url else "https://" + url)
                        domain = parsed.netloc or None
                    except Exception:
                        domain = None

                evidence_snippets.append({
                    "url": url,
                    "title": title,
                    "snippet": snippet,
                    "domain": domain,
                    "provider": "openai-web",
                    "published_date": None,
                })

        return {"people": people, "evidence_snippets": evidence_snippets}

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
        - "leadership": discovery of founders and key executives (fallback for PDL).

        Additional params:
        - company_name: str
        - website: str
        - context: str (optional free-form description from the user)

        Returns:
            ConnectorResult({
                "snippets": [...],
                "competitors": [...],       # in competitors mode
                "founding_facts": {...},    # in founding mode
                "people_web": [...]         # in leadership mode
            })
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
        elif mode == "leadership":
            prompt = self._build_leadership_prompt(company_name, website, context)
        elif mode == "person":
    
            person_name = str(params.get("person_name") or params.get("company_name") or "").strip()
            target_company = str(params.get("company") or "").strip() # Planner might pass 'company'
            prompt = self._build_person_prompt(person_name, target_company or website, context)
        elif mode == "news":
            prompt = self._build_news_prompt(company_name, website, context)
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

            def _attr(source: Any, key: str) -> Any:
                if source is None:
                    return None
                if isinstance(source, dict):
                    return source.get(key)
                return getattr(source, key, None)

            def _as_int(value: Any) -> int:
                try:
                    return int(value or 0)
                except (TypeError, ValueError):
                    return 0

            usage_obj = getattr(response, "usage", None)
            input_tokens = _as_int(_attr(usage_obj, "input_tokens"))
            output_tokens = _as_int(_attr(usage_obj, "output_tokens"))
            cached_tokens = _as_int(
                _attr(_attr(usage_obj, "input_tokens_details"), "cached_tokens")
            )
            reasoning_tokens = _as_int(
                _attr(_attr(usage_obj, "output_tokens_details"), "reasoning_tokens")
            )

            output_items = getattr(response, "output", None) or []
            web_search_calls = 0
            for item in output_items:
                item_type = getattr(item, "type", None)
                if item_type is None and isinstance(item, dict):
                    item_type = item.get("type")
                if item_type == "web_search_call":
                    web_search_calls += 1

            effective_model = getattr(response, "model", self._model)
            model_cost = cost_for_tokens(
                effective_model, input_tokens, output_tokens, cached_tokens
            )
            tool_cost = cost_for_web_search_calls(web_search_calls)
            cost_payload = {
                "model_cost_usd": model_cost,
                "web_search_tool_cost_usd": tool_cost,
                "total_cost_usd": model_cost + tool_cost,
            }
            usage_payload = {
                "model": effective_model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cached_input_tokens": cached_tokens,
                "reasoning_output_tokens": reasoning_tokens,
                "web_search_calls": web_search_calls,
            }

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

            payload: Dict[str, Any]
            if mode == "competitors":
                competitors = self._parse_competitor_json(raw_text or "")
                snippets = self._competitors_to_snippets(competitors)
                payload = {
                    "snippets": snippets,
                    "competitors": competitors,
                }
            elif mode == "founding":
                parsed = self._parse_founding_json(raw_text or "")
                snippets = self._founding_evidence_to_snippets(parsed.get("evidence", []))
                payload = {
                    "snippets": snippets,
                    "founding_facts": parsed.get("founding_facts", {}),
                }
            elif mode == "leadership":
                parsed = self._parse_leadership_json(raw_text or "")
                payload = {
                    "snippets": parsed.get("evidence_snippets", []),
                    "people_web": parsed.get("people", []),
                }
            elif mode == "person":
                parsed = self._parse_person_json(raw_text or "")
                snippets = self._person_bio_to_snippets(parsed)
                payload = {
                    "snippets": snippets,
                    "person_bio": parsed, # Return full structure for entity resolution if needed
                }
            elif mode == "news":
                news_items = self._parse_news_json(raw_text or "")
                snippets = self._news_to_snippets(news_items)
                payload = {
                    "snippets": snippets,
                    "news": news_items,
                }
            else:
                payload = {}

            if payload is not None:
                payload["usage"] = usage_payload
                payload["cost"] = cost_payload

            return payload

        result: Dict[str, Any] = await asyncio.to_thread(_call_openai_sync)

        if result:
            # Cache for 24h – competitor set and founding facts are relatively stable.
            cache_payload = {
                key: value for key, value in result.items() if key not in {"usage", "cost"}
            }
            await cached_get(cache_key, set_value=cache_payload, ttl=60 * 60 * 24)

        return ConnectorResult(result)