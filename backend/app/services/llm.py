from __future__ import annotations

from functools import lru_cache
from contextlib import contextmanager
from threading import BoundedSemaphore

from openai import OpenAI

from ..core.config import get_settings

_llm_semaphore: BoundedSemaphore | None = None


def _get_semaphore() -> BoundedSemaphore:
    """
    Lazy-initialised global semaphore for limiting concurrent LLM calls.
    """
    global _llm_semaphore
    if _llm_semaphore is None:
        settings = get_settings()
        _llm_semaphore = BoundedSemaphore(settings.LLM_MAX_CONCURRENCY)
    return _llm_semaphore


@contextmanager
def limit_llm_concurrency():
    """
    Simple context manager to bound concurrent calls to the LLM provider.

    Usage:

        with limit_llm_concurrency():
            client.chat.completions.create(...)

    This works for both sync and async call-sites (use inside the thread
    that actually performs the HTTP request).
    """
    sem = _get_semaphore()
    sem.acquire()
    try:
        yield
    finally:
        sem.release()


@lru_cache(maxsize=1)
def get_llm_client() -> OpenAI:
    """
    Centralised factory for the OpenAI‑compatible client used across the app.

    - If OPENROUTER_API_KEY is set, route requests via OpenRouter.
    - Otherwise, fall back to the standard OpenAI API using OPENAI_API_KEY.

    This is cached so all callers in a process share a single client instance.
    """
    settings = get_settings()

    if settings.OPENROUTER_API_KEY:
        # Sanitize key and add required headers for OpenRouter
        return OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=settings.OPENROUTER_API_KEY.strip(),
            default_headers={
                "HTTP-Referer": settings.FRONTEND_ORIGIN or "http://localhost:3000",
                "X-Title": "Serendipity Deep Research",
            },
        )

    if settings.OPENAI_API_KEY:
        return OpenAI(api_key=settings.OPENAI_API_KEY.strip())

    raise RuntimeError(
        "No LLM API key configured. Set either OPENAI_API_KEY or OPENROUTER_API_KEY."
    )
