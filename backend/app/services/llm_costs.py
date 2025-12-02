from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..core.config import get_settings


@dataclass(frozen=True)
class ModelRate:
    input_per_mtok: float
    output_per_mtok: float
    cached_input_per_mtok: Optional[float] = None


def _build_default_pricebook() -> Dict[str, ModelRate]:
    # Prices sourced from OpenAI API pricing (USD per 1M tokens).
    return {
        "gpt-5.1": ModelRate(
            input_per_mtok=1.250,
            output_per_mtok=10.000,
            cached_input_per_mtok=0.125,
        ),
        "gpt-5-mini": ModelRate(
            input_per_mtok=0.250,
            output_per_mtok=2.000,
            cached_input_per_mtok=0.025,
        ),
        "gpt-5-pro": ModelRate(
            input_per_mtok=15.000,
            output_per_mtok=120.000,
            cached_input_per_mtok=None,
        ),
    }


def _load_pricebook() -> Dict[str, ModelRate]:
    settings = get_settings()
    pricebook = _build_default_pricebook()
    override_raw = settings.LLM_PRICEBOOK_JSON
    if not override_raw:
        return pricebook

    try:
        override = json.loads(override_raw)
    except json.JSONDecodeError:
        return pricebook

    if not isinstance(override, dict):
        return pricebook

    for key, value in override.items():
        if not isinstance(value, dict):
            continue
        try:
            pricebook[key.strip().lower()] = ModelRate(
                input_per_mtok=float(value["input_per_mtok"]),
                output_per_mtok=float(value["output_per_mtok"]),
                cached_input_per_mtok=float(value.get("cached_input_per_mtok"))
                if value.get("cached_input_per_mtok") is not None
                else None,
            )
        except (KeyError, ValueError, TypeError):
            continue
    return pricebook


_PRICEBOOK: Dict[str, ModelRate] = _load_pricebook()
_WEB_SEARCH_COST_PER_CALL = get_settings().WEB_SEARCH_PER_CALL_USD


def normalize_model_name(model: str | None) -> str:
    m = (model or "").strip().lower()
    if "/" in m:
        m = m.split("/")[-1]
    if ":" in m:
        m = m.split(":")[0]
    return m


def cost_for_tokens(
    model: str | None,
    input_tokens: int,
    output_tokens: int,
    cached_input_tokens: int = 0,
) -> float:
    key = normalize_model_name(model)
    rate = _PRICEBOOK.get(key)
    if not rate:
        return 0.0

    paid_input = max(0, int(input_tokens) - max(0, int(cached_input_tokens)))
    cached_input = max(0, int(cached_input_tokens))
    output = max(0, int(output_tokens))

    total = 0.0
    total += (paid_input / 1_000_000) * rate.input_per_mtok
    total += (output / 1_000_000) * rate.output_per_mtok
    if cached_input:
        cached_rate = rate.cached_input_per_mtok or rate.input_per_mtok
        total += (cached_input / 1_000_000) * cached_rate
    return total


def cost_for_web_search_calls(call_count: int) -> float:
    return max(0, int(call_count)) * _WEB_SEARCH_COST_PER_CALL


class LLMCostTracker:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self._lock = threading.Lock()
        self._records: List[Dict[str, Any]] = []

    def add_record(
        self,
        provider: str,
        model: str | None,
        kind: str,
        *,
        section: str | None = None,
        input_tokens: int = 0,
        output_tokens: int = 0,
        cached_input_tokens: int = 0,
        reasoning_output_tokens: int = 0,
        web_search_calls: int = 0,
        tool_cost_usd: float | None = None,
        cost_usd: float | None = None,
    ) -> None:
        record = {
            "provider": provider or "unknown",
            "model": model or "",
            "kind": kind,
            "section": section,
            "input_tokens": int(input_tokens or 0),
            "output_tokens": int(output_tokens or 0),
            "cached_input_tokens": int(cached_input_tokens or 0),
            "reasoning_output_tokens": int(reasoning_output_tokens or 0),
            "web_search_calls": int(web_search_calls or 0),
            "tool_cost_usd": float(tool_cost_usd) if tool_cost_usd is not None else 0.0,
            "cost_usd": float(cost_usd) if cost_usd is not None else 0.0,
        }
        with self._lock:
            self._records.append(record)

    def summarize(self) -> dict:
        providers: Dict[str, Dict[str, Any]] = {}
        total_cost = 0.0

        with self._lock:
            records_snapshot = list(self._records)

        for rec in records_snapshot:
            provider_key = rec["provider"] or "unknown"
            provider_entry = providers.setdefault(
                provider_key,
                {
                    "model": rec["model"],
                    "cost_usd": 0.0,
                    "totals": {
                        "input": 0,
                        "output": 0,
                        "cached_input": 0,
                        "reasoning_output": 0,
                        "web_search_calls": 0,
                    },
                    "calls": [],
                },
            )

            provider_entry["model"] = provider_entry.get("model") or rec["model"]

            provider_entry["totals"]["input"] += rec["input_tokens"]
            provider_entry["totals"]["output"] += rec["output_tokens"]
            provider_entry["totals"]["cached_input"] += rec["cached_input_tokens"]
            provider_entry["totals"]["reasoning_output"] += rec[
                "reasoning_output_tokens"
            ]
            provider_entry["totals"]["web_search_calls"] += rec["web_search_calls"]

            tool_cost = rec.get("tool_cost_usd") or 0.0
            if tool_cost:
                tool_costs = provider_entry.setdefault("tool_costs", {})
                tool_costs["web_search_usd"] = tool_costs.get("web_search_usd", 0.0) + tool_cost
            call_cost = rec["cost_usd"] + tool_cost
            provider_entry["cost_usd"] += call_cost
            total_cost += call_cost

            provider_entry["calls"].append(
                {
                    "kind": rec["kind"],
                    "section": rec["section"],
                    "model": rec["model"],
                    "input": rec["input_tokens"],
                    "output": rec["output_tokens"],
                    "cached_input": rec["cached_input_tokens"],
                    "reasoning_output": rec["reasoning_output_tokens"],
                    "web_search_calls": rec["web_search_calls"],
                    "tool_cost_usd": tool_cost or None,
                    "cost_usd": call_cost,
                }
            )

        return {
            "providers": providers,
            "total_cost_usd": total_cost,
        }


