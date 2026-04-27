from pathlib import Path

from bridge.platforms.client import BotApiClient
from bridge.rate_limit import InMemoryRateLimiter, RateLimitConfig
from bridge.sales import load_sales_catalog
from bridge.services.bridge_service import BridgeService
from bridge.types import Platform


class _DummyClient(BotApiClient):
    def __init__(self, platform: Platform) -> None:
        super().__init__(platform, token="t", api_base_url="https://example.org", file_base_url="https://example.org")

    async def get_updates(self, offset, timeout, allowed_updates):
        return []


class _Settings:
    telegram_poll_timeout_sec = 30
    bale_poll_timeout_sec = 30
    log_retention_days = 30
    log_cleanup_interval_sec = 3600
    audit_events_enabled = True
    audit_capture_full_text = True
    telegram_allowed_updates = ["message"]
    bale_allowed_updates = ["message"]
    metrics_enabled = False
    queue_retry_enabled = False
    queue_worker_interval_sec = 1
    media_max_download_mb = 20
    media_max_upload_mb = 20
    media_tmp_dir = "./tmp"
    media_delete_after_send = True
    queue_retry_base_sec = 5
    queue_retry_max_hours = 24
    queue_retry_max_sec = 300
    rate_limit_enabled = False
    message_max_text_len = 0
    forward_caption_template = "x"
    show_sender_platform = True
    show_sender_username = True
    show_sender_display_name = True
    admin_ids = []
    telegram_admin_channel_id = ""
    bale_wallet_provider_token = ""
    telegram_ton_pay_enabled = False
    telegram_ton_pay_api_token = ""
    telegram_ton_pay_api_base_url = "https://pay.crypt.bot/api"
    telegram_ton_pay_asset = "TON"
    telegram_ton_pay_poll_interval_sec = 15
    telegram_ton_pay_timeout_sec = 12
    ton_rate_api_enabled = False
    ton_rate_api_url = "https://api.binance.com/api/v3/ticker/price"
    ton_rate_api_symbol = "TONUSDT"
    ton_rate_api_cache_sec = 300
    ton_rate_api_timeout_sec = 8
    usdt_fixed_toman_rate = 150000
    telegram_enable_button_styles = False
    telegram_button_style_mode = "none"
    telegram_set_commands_on_start = False
    telegram_set_menu_button_on_start = False


class _Repo:
    pass


def _build_service() -> BridgeService:
    catalog = load_sales_catalog(str(Path(__file__).resolve().parents[1] / "config" / "sales_catalog.json"))
    return BridgeService(
        settings=_Settings(),
        sales_catalog=catalog,
        repository=_Repo(),
        telegram_client=_DummyClient(Platform.TELEGRAM),
        bale_client=_DummyClient(Platform.BALE),
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )


def test_extract_ton_rate_from_binance_payload() -> None:
    price = BridgeService._extract_ton_rate_price_from_payload({"symbol": "TONUSDT", "price": "3.2"}, "TONUSDT")
    assert price == 3.2


def test_price_reference_text_shows_ton_and_hides_bale_wallet_price() -> None:
    svc = _build_service()
    svc._ton_per_usdt = 0.25
    package = svc.sales_catalog.packages[0]
    text = svc._price_reference_text(package)
    assert "معادل تون:" not in text
    assert "قیمت مرجع:" in text
    assert "معادل کارت‌به‌کارت:" in text
    assert "قیمت کیف پول بله" not in text
