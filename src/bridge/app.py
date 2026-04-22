from __future__ import annotations

from bridge.config import load_settings, sqlite_path
from bridge.db import init_db
from bridge.platforms.client import BotApiClient
from bridge.rate_limit import InMemoryRateLimiter, RateLimitConfig
from bridge.repository import Repository
from bridge.services.bridge_service import BridgeService
from bridge.types import Platform


async def build_service(env_file: str = ".env") -> BridgeService:
    settings = load_settings(env_file)
    db_path = sqlite_path(settings)
    await init_db(db_path)

    repository = Repository(
        db_path=db_path,
        bridge_id_prefix=settings.bridge_id_prefix,
        bridge_id_length=settings.bridge_id_length,
    )

    telegram_client = BotApiClient(
        platform=Platform.TELEGRAM,
        token=settings.telegram_bot_token,
        api_base_url=settings.telegram_api_base_url,
        file_base_url=settings.telegram_file_base_url,
        timeout_sec=float(settings.telegram_poll_timeout_sec + 15),
    )
    bale_client = BotApiClient(
        platform=Platform.BALE,
        token=settings.bale_bot_token,
        api_base_url=settings.bale_api_base_url,
        file_base_url=settings.bale_file_base_url,
        timeout_sec=float(settings.bale_poll_timeout_sec + 15),
    )

    limiter = InMemoryRateLimiter(
        RateLimitConfig(
            msg_per_min=settings.rate_limit_msg_per_min,
            media_per_min=settings.rate_limit_media_per_min,
        )
    )

    return BridgeService(
        settings=settings,
        repository=repository,
        telegram_client=telegram_client,
        bale_client=bale_client,
        rate_limiter=limiter,
    )
