import random
import asyncio
from typing import Any


def string_map(d: dict[str, Any] | None) -> dict[str, str]:
    """
    A helper method to map all key/value pairs in a dictionary to string.
    """
    if not d:
        return {}
    return {str(k): "" if v is None else str(v) for k, v in d.items()}


async def async_exponential_backoff(base_delay: float, attempt: int) -> None:
    """
    Exponential backoff plus random jitter delay for retry attempts.
    Do not use for low latency requirements.
    """
    sleep_time = base_delay * (2 ** (attempt - 1)) + random.uniform(0, base_delay)
    await asyncio.sleep(sleep_time)
