from bridge.rate_limit import InMemoryRateLimiter, RateLimitConfig


def test_rate_limiter_text_limit() -> None:
    limiter = InMemoryRateLimiter(RateLimitConfig(msg_per_min=2, media_per_min=1))

    assert limiter.allow(1, "text") is True
    assert limiter.allow(1, "text") is True
    assert limiter.allow(1, "text") is False


def test_rate_limiter_media_limit() -> None:
    limiter = InMemoryRateLimiter(RateLimitConfig(msg_per_min=10, media_per_min=1))

    assert limiter.allow(1, "media") is True
    assert limiter.allow(1, "media") is False
