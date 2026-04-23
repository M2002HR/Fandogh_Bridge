from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _str(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None else value


def _list(name: str, default: list[str]) -> list[str]:
    value = os.getenv(name)
    if not value:
        return default
    value = value.strip()
    if value.startswith("["):
        try:
            parsed = json.loads(value)
            return [str(item) for item in parsed]
        except json.JSONDecodeError:
            # Accept shell-sourced forms like [message] in addition to JSON.
            inner = value[1:-1].strip()
            if not inner:
                return default
            parts = [p.strip().strip("'\"") for p in inner.split(",") if p.strip()]
            return parts or default
    return [part.strip() for part in value.split(",") if part.strip()]


@dataclass(slots=True)
class Settings:
    app_env: str
    log_level: str
    tz: str
    app_lang: str

    telegram_bot_token: str
    telegram_api_base_url: str
    telegram_file_base_url: str
    telegram_poll_timeout_sec: int
    telegram_allowed_updates: list[str]

    bale_bot_token: str
    bale_api_base_url: str
    bale_file_base_url: str
    bale_poll_timeout_sec: int
    bale_allowed_updates: list[str]

    db_url: str

    media_tmp_dir: str
    media_max_download_mb: int
    media_max_upload_mb: int
    media_delete_after_send: bool

    queue_retry_enabled: bool
    queue_retry_max_hours: int
    queue_retry_base_sec: int
    queue_retry_max_sec: int
    queue_worker_interval_sec: int

    rate_limit_enabled: bool
    rate_limit_msg_per_min: int
    rate_limit_media_per_min: int

    message_max_text_len: int
    forward_caption_template: str

    bridge_id_prefix: str
    bridge_id_length: int

    show_sender_platform: bool
    show_sender_username: bool
    show_sender_display_name: bool

    metrics_enabled: bool
    healthcheck_enabled: bool

    admin_ids: list[str]
    telegram_admin_channel_id: str
    docker_image_tag: str


DEFAULT_ALLOWED_UPDATES = ["message", "callback_query"]


def load_settings(env_file: str = ".env") -> Settings:
    # Override pre-exported shell vars so `.env` remains source of truth.
    load_dotenv(env_file, override=True)

    settings = Settings(
        app_env=_str("APP_ENV", "development"),
        log_level=_str("LOG_LEVEL", "INFO"),
        tz=_str("TZ", "Asia/Tehran"),
        app_lang=_str("APP_LANG", "fa"),
        telegram_bot_token=_str("TELEGRAM_BOT_TOKEN", ""),
        telegram_api_base_url=_str("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
        telegram_file_base_url=_str("TELEGRAM_FILE_BASE_URL", "https://api.telegram.org/file"),
        telegram_poll_timeout_sec=_int("TELEGRAM_POLL_TIMEOUT_SEC", 30),
        telegram_allowed_updates=_list("TELEGRAM_ALLOWED_UPDATES", DEFAULT_ALLOWED_UPDATES),
        bale_bot_token=_str("BALE_BOT_TOKEN", ""),
        bale_api_base_url=_str("BALE_API_BASE_URL", "https://tapi.bale.ai"),
        bale_file_base_url=_str("BALE_FILE_BASE_URL", "https://tapi.bale.ai/file"),
        bale_poll_timeout_sec=_int("BALE_POLL_TIMEOUT_SEC", 30),
        bale_allowed_updates=_list("BALE_ALLOWED_UPDATES", DEFAULT_ALLOWED_UPDATES),
        db_url=_str("DB_URL", "sqlite+aiosqlite:///./data/bridge.db"),
        media_tmp_dir=_str("MEDIA_TMP_DIR", "./tmp_media"),
        media_max_download_mb=_int("MEDIA_MAX_DOWNLOAD_MB", 20),
        media_max_upload_mb=_int("MEDIA_MAX_UPLOAD_MB", 20),
        media_delete_after_send=_bool("MEDIA_DELETE_AFTER_SEND", True),
        queue_retry_enabled=_bool("QUEUE_RETRY_ENABLED", True),
        queue_retry_max_hours=_int("QUEUE_RETRY_MAX_HOURS", 24),
        queue_retry_base_sec=_int("QUEUE_RETRY_BASE_SEC", 5),
        queue_retry_max_sec=_int("QUEUE_RETRY_MAX_SEC", 300),
        queue_worker_interval_sec=_int("QUEUE_WORKER_INTERVAL_SEC", 2),
        rate_limit_enabled=_bool("RATE_LIMIT_ENABLED", True),
        rate_limit_msg_per_min=_int("RATE_LIMIT_MSG_PER_MIN", 40),
        rate_limit_media_per_min=_int("RATE_LIMIT_MEDIA_PER_MIN", 12),
        message_max_text_len=_int("MESSAGE_MAX_TEXT_LEN", 3500),
        forward_caption_template=_str(
            "FORWARD_CAPTION_TEMPLATE",
            "[{platform}] از {display_name} (@{username}) | ID: {bridge_id}",
        ),
        bridge_id_prefix=_str("BRIDGE_ID_PREFIX", "FDG"),
        bridge_id_length=_int("BRIDGE_ID_LENGTH", 10),
        show_sender_platform=_bool("SHOW_SENDER_PLATFORM", True),
        show_sender_username=_bool("SHOW_SENDER_USERNAME", True),
        show_sender_display_name=_bool("SHOW_SENDER_DISPLAY_NAME", True),
        metrics_enabled=_bool("METRICS_ENABLED", False),
        healthcheck_enabled=_bool("HEALTHCHECK_ENABLED", False),
        admin_ids=_list("ADMIN_IDS", []),
        telegram_admin_channel_id=_str("TELEGRAM_ADMIN_CHANNEL_ID", "").strip(),
        docker_image_tag=_str("DOCKER_IMAGE_TAG", "fandogh-bridge:latest"),
    )

    if not settings.telegram_bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN is required")
    if not settings.bale_bot_token:
        raise ValueError("BALE_BOT_TOKEN is required")

    Path(settings.media_tmp_dir).mkdir(parents=True, exist_ok=True)
    Path(_db_path_from_url(settings.db_url)).parent.mkdir(parents=True, exist_ok=True)
    return settings


def _db_path_from_url(db_url: str) -> str:
    prefix = "sqlite+aiosqlite:///"
    if not db_url.startswith(prefix):
        raise ValueError("Only sqlite+aiosqlite DB_URL is supported in v1")
    return db_url.removeprefix(prefix)


def sqlite_path(settings: Settings) -> str:
    return _db_path_from_url(settings.db_url)
