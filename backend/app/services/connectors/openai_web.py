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
          "competitors": [
             {
               "name": ...,
               "website": ...,
               "category": "direct" | "adjacent" | "substitute",
               "summary": ...,
               "why_relevant": ...,
               "tech_and_moat": ...,
               "geo_focus": ...,
             },
             ...
          ]
        }

    Notes:
    - We intentionally *do not* expose raw OpenAI web search result URLs directly
      here; instead the model consolidates them into a higher-level competitor
      list which is then turned into our snippets. This keeps the connector
      boundary clean while still providing the Writer with rich, reasoned input.
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

        We ask the model to:
        - Use web_search.
        - Focus on *true* competitors rather than generic companies in the same space.
        - Return a strict JSON structure that we can parse robustly.
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

    # ------------------------------------------------------------------
    # Core fetch
    # ------------------------------------------------------------------

    async def fetch(self, **params: Any) -> ConnectorResult:
        """
        Unified entrypoint expected by the orchestrator.

        Supported modes (params["mode"]):
        - "competitors": high-level competitor discovery for the target company.

        Additional params for mode=="competitors":
        - company_name: str
        - website: str
        - context: str (optional free-form description from the user)

        Returns:
            ConnectorResult({"snippets": [...], "competitors": [...]})
        """
        if not self._has_credentials():
            logger.info(
                "OpenAIWebSearchConnector disabled (no API key or OpenAI client). Returning empty result."
            )
            return ConnectorResult({})

        mode = (params.get("mode") or "competitors").lower()
        if mode != "competitors":
            # For now we only implement competitor discovery; the planner is
            # wired so that this connector is only used for that purpose.
            logger.warning(
                "OpenAIWebSearchConnector called with unsupported mode '%s'; returning empty result.",
                mode,
            )
            return ConnectorResult({})

        company_name = str(params.get("company_name") or "").strip()
        website = str(params.get("website") or "").strip()
        context = str(params.get("context") or "").strip()

        cache_key_parts = ["openai_web", "competitors"]
        if company_name:
            cache_key_parts.append(f"name:{company_name.lower()}")
        if website:
            cache_key_parts.append(f"site:{website.lower()}")
        cache_key = "|".join(cache_key_parts)

        cached = await cached_get(cache_key)
        if cached is not None:
            return ConnectorResult(cached)

        prompt = self._build_competitor_prompt(company_name, website, context)

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
                return {"snippets": [], "competitors": []}

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

            competitors = self._parse_competitor_json(raw_text or "")
            snippets = self._competitors_to_snippets(competitors)

            return {
                "snippets": snippets,
                "competitors": competitors,
            }

        result: Dict[str, Any] = await asyncio.to_thread(_call_openai_sync)

        # Cache for 24h – competitor set is relatively stable.
        await cached_get(cache_key, set_value=result, ttl=60 * 60 * 24)

        return ConnectorResult(result)

