from __future__ import annotations

import json
from typing import Any, Optional

import redis.asyncio as redis
from ..core.config import get_settings

# Initialize settings to get REDIS_URL
settings = get_settings()

# Initialize Redis client
# decode_responses=True ensures we get str back, not bytes
_redis = redis.from_url(str(settings.REDIS_URL), decode_responses=True)


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
    try:
        if set_value is None:
            # Read path
            val = await _redis.get(key)
            if val is not None:
                return json.loads(val)
            return None

        # Write path
        serialized = json.dumps(set_value)
        if ttl is not None:
            await _redis.set(key, serialized, ex=ttl)
        else:
            await _redis.set(key, serialized)
        return set_value

    except redis.RedisError:
        # If Redis fails, we treat it as a cache miss or write failure
        return None
