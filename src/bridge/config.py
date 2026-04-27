from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from bridge.db_url import parse_db_url


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
    log_format: str
    log_retention_days: int
    log_cleanup_interval_sec: int
    audit_events_enabled: bool
    audit_capture_full_text: bool
    tz: str
    app_lang: str

    telegram_bot_token: str
    telegram_api_base_url: str
    telegram_file_base_url: str
    telegram_poll_timeout_sec: int
    telegram_allowed_updates: list[str]
    telegram_enable_button_styles: bool
    telegram_button_style_mode: str
    telegram_set_commands_on_start: bool
    telegram_set_menu_button_on_start: bool

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
    bale_wallet_provider_token: str
    telegram_ton_pay_enabled: bool
    telegram_ton_pay_api_token: str
    telegram_ton_pay_api_base_url: str
    telegram_ton_pay_asset: str
    telegram_ton_pay_poll_interval_sec: int
    telegram_ton_pay_timeout_sec: int
    ton_rate_api_enabled: bool
    ton_rate_api_url: str
    ton_rate_api_symbol: str
    ton_rate_api_cache_sec: int
    ton_rate_api_timeout_sec: int
    usdt_fixed_toman_rate: int
    sales_config_path: str
    docker_image_tag: str


DEFAULT_TELEGRAM_ALLOWED_UPDATES = ["message", "callback_query", "pre_checkout_query"]
DEFAULT_BALE_ALLOWED_UPDATES = ["message", "callback_query", "pre_checkout_query"]


def load_settings(env_file: str = ".env") -> Settings:
    # Override pre-exported shell vars so `.env` remains source of truth.
    load_dotenv(env_file, override=True)

    settings = Settings(
        app_env=_str("APP_ENV", "development"),
        log_level=_str("LOG_LEVEL", "INFO"),
        log_format=_str("LOG_FORMAT", "json").strip().lower() or "json",
        log_retention_days=max(1, _int("LOG_RETENTION_DAYS", 30)),
        log_cleanup_interval_sec=max(60, _int("LOG_CLEANUP_INTERVAL_SEC", 3600)),
        audit_events_enabled=_bool("AUDIT_EVENTS_ENABLED", True),
        audit_capture_full_text=_bool("AUDIT_CAPTURE_FULL_TEXT", True),
        tz=_str("TZ", "Asia/Tehran"),
        app_lang=_str("APP_LANG", "fa"),
        telegram_bot_token=_str("TELEGRAM_BOT_TOKEN", ""),
        telegram_api_base_url=_str("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
        telegram_file_base_url=_str("TELEGRAM_FILE_BASE_URL", "https://api.telegram.org/file"),
        telegram_poll_timeout_sec=_int("TELEGRAM_POLL_TIMEOUT_SEC", 30),
        telegram_allowed_updates=_list("TELEGRAM_ALLOWED_UPDATES", DEFAULT_TELEGRAM_ALLOWED_UPDATES),
        telegram_enable_button_styles=_bool("TELEGRAM_ENABLE_BUTTON_STYLES", False),
        telegram_button_style_mode=_str("TELEGRAM_BUTTON_STYLE_MODE", "none").strip().lower() or "none",
        telegram_set_commands_on_start=_bool("TELEGRAM_SET_COMMANDS_ON_START", True),
        telegram_set_menu_button_on_start=_bool("TELEGRAM_SET_MENU_BUTTON_ON_START", True),
        bale_bot_token=_str("BALE_BOT_TOKEN", ""),
        bale_api_base_url=_str("BALE_API_BASE_URL", "https://tapi.bale.ai"),
        bale_file_base_url=_str("BALE_FILE_BASE_URL", "https://tapi.bale.ai/file"),
        bale_poll_timeout_sec=_int("BALE_POLL_TIMEOUT_SEC", 30),
        bale_allowed_updates=_list("BALE_ALLOWED_UPDATES", DEFAULT_BALE_ALLOWED_UPDATES),
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
        bale_wallet_provider_token=_str("BALE_WALLET_PROVIDER_TOKEN", "").strip(),
        telegram_ton_pay_enabled=_bool("TELEGRAM_TON_PAY_ENABLED", False),
        telegram_ton_pay_api_token=_str("TELEGRAM_TON_PAY_API_TOKEN", "").strip(),
        telegram_ton_pay_api_base_url=_str("TELEGRAM_TON_PAY_API_BASE_URL", "https://pay.crypt.bot/api").strip(),
        telegram_ton_pay_asset=_str("TELEGRAM_TON_PAY_ASSET", "TON").strip().upper() or "TON",
        telegram_ton_pay_poll_interval_sec=_int("TELEGRAM_TON_PAY_POLL_INTERVAL_SEC", 15),
        telegram_ton_pay_timeout_sec=_int("TELEGRAM_TON_PAY_TIMEOUT_SEC", 12),
        ton_rate_api_enabled=_bool("TON_RATE_API_ENABLED", True),
        ton_rate_api_url=_str("TON_RATE_API_URL", "https://data-api.binance.vision/api/v3/ticker/price").strip(),
        ton_rate_api_symbol=_str("TON_RATE_API_SYMBOL", "TONUSDT").strip().upper() or "TONUSDT",
        ton_rate_api_cache_sec=max(30, _int("TON_RATE_API_CACHE_SEC", 300)),
        ton_rate_api_timeout_sec=max(3, _int("TON_RATE_API_TIMEOUT_SEC", 8)),
        usdt_fixed_toman_rate=max(0, _int("USDT_FIXED_TOMAN_RATE", 150000)),
        sales_config_path=_str("SALES_CONFIG_PATH", "./config/sales_catalog.json").strip(),
        docker_image_tag=_str("DOCKER_IMAGE_TAG", "fandogh-bridge:latest"),
    )

    if settings.telegram_button_style_mode not in {"none", "auto"}:
        settings.telegram_button_style_mode = "none"
    if settings.log_format not in {"json", "plain"}:
        settings.log_format = "json"

    if not settings.telegram_bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN is required")
    if not settings.bale_bot_token:
        raise ValueError("BALE_BOT_TOKEN is required")

    Path(settings.media_tmp_dir).mkdir(parents=True, exist_ok=True)
    parsed_db = parse_db_url(settings.db_url)
    if parsed_db.backend == "sqlite" and parsed_db.sqlite_path:
        Path(parsed_db.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
    Path(settings.sales_config_path).parent.mkdir(parents=True, exist_ok=True)
    return settings


def _db_path_from_url(db_url: str) -> str:
    parsed = parse_db_url(db_url)
    if parsed.backend != "sqlite" or not parsed.sqlite_path:
        raise ValueError("DB_URL is not sqlite")
    return parsed.sqlite_path


def sqlite_path(settings: Settings) -> str:
    return _db_path_from_url(settings.db_url)
