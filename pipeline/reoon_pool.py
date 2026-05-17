from __future__ import annotations
"""Multi-key Reoon pool. Each key has its own rate-limit tracking.
Round-robin selection — skip rate-limited keys.

Effective throughput = N keys × 20 RPM = up to N × 20 verifications/minute.
"""
import asyncio
import time
import logging
from config import settings

log = logging.getLogger("reoon_pool")


def _parse_keys() -> list[str]:
    """Read keys from REOON_API_KEYS (plural, comma-separated), falling back to REOON_API_KEY."""
    raw = settings.REOON_API_KEYS or settings.REOON_API_KEY
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    return keys


class ReoonKey:
    __slots__ = ("key", "ratelimit_until")
    def __init__(self, key: str):
        self.key = key
        self.ratelimit_until: float = 0.0  # epoch seconds

    @property
    def available(self) -> bool:
        return time.time() >= self.ratelimit_until

    def mark_ratelimited(self, seconds: int = 60):
        self.ratelimit_until = time.time() + seconds


class ReoonPool:
    """Round-robin pool of API keys. Thread-safe via asyncio lock."""
    def __init__(self):
        keys = _parse_keys()
        self.keys: list[ReoonKey] = [ReoonKey(k) for k in keys]
        self._idx = 0
        self._lock = asyncio.Lock()

    def __len__(self): return len(self.keys)
    def has_keys(self): return bool(self.keys)

    async def acquire(self) -> str | None:
        """Get an available key. Returns None if all are rate-limited (caller should retry later)."""
        if not self.keys:
            return None
        async with self._lock:
            # Try each key starting from current index
            for _ in range(len(self.keys)):
                k = self.keys[self._idx]
                self._idx = (self._idx + 1) % len(self.keys)
                if k.available:
                    return k.key
            return None

    def mark_ratelimited(self, key: str, seconds: int = 60):
        for k in self.keys:
            if k.key == key:
                k.mark_ratelimited(seconds)
                return


# Singleton
_pool: ReoonPool | None = None
def get_pool() -> ReoonPool:
    global _pool
    if _pool is None:
        _pool = ReoonPool()
    return _pool
