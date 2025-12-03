from __future__ import annotations

import json
from typing import Any

import redis
from ..core.config import get_settings

settings = get_settings()


def _get_sync_redis() -> redis.Redis:
    """
    Create a fresh sync Redis client per call so Celery workers
    don't hold onto closed event loops.
    """
    return redis.from_url(
        str(settings.REDIS_URL),
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
    )


async def cached_get(
    key: str,
    set_value: Any | None = None,
    ttl: int | None = None,
) -> Any:
    """
    Async TTL cache backed by Redis.

    Usage:

        value = await cached_get("k")                  # read
        await cached_get("k", set_value=value, ttl=60) # write with TTL

    - On read: returns cached value (deserialized JSON) or None if missing/expired.
    - On write: stores value (serialized JSON) with optional TTL and returns it.
    """
    client = _get_sync_redis()
    try:
        if set_value is None:
            # Read path
            val = client.get(key)
            if val is not None:
                return json.loads(val)
            return None

        # Write path
        serialized = json.dumps(set_value)
        if ttl is not None:
            client.set(key, serialized, ex=ttl)
        else:
            client.set(key, serialized)
        return set_value

    except redis.RedisError:
        return None
    finally:
        try:
            client.close()
        except Exception:
            pass
