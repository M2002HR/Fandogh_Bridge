from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from time import monotonic


@dataclass(slots=True)
class RateLimitConfig:
    msg_per_min: int
    media_per_min: int


class InMemoryRateLimiter:
    def __init__(self, config: RateLimitConfig) -> None:
        self.config = config
        self._buckets: dict[tuple[int, str], deque[float]] = defaultdict(deque)

    def allow(self, user_id: int, kind: str) -> bool:
        now = monotonic()
        key = (user_id, kind)
        bucket = self._buckets[key]

        while bucket and now - bucket[0] >= 60:
            bucket.popleft()

        limit = self.config.msg_per_min if kind == "text" else self.config.media_per_min
        if len(bucket) >= limit:
            return False

        bucket.append(now)
        return True
