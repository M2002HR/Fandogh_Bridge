from __future__ import annotations

import copy
from pathlib import Path

import pytest

from bridge.db import init_db
from bridge.platforms.client import BotApiClient
from bridge.rate_limit import InMemoryRateLimiter, RateLimitConfig
from bridge.repository import Repository
from bridge.sales import load_sales_catalog
from bridge.services.bridge_service import BridgeService, FlowState
from bridge.services.ui import BTN_PAYMENT_HISTORY
from bridge.types import ContentType, IncomingMessage, Platform


class DummyClient(BotApiClient):
    def __init__(self, platform: Platform) -> None:
        super().__init__(platform, token="t", api_base_url="https://example.org", file_base_url="https://example.org")
        self.sent_messages: list[dict] = []
        self.sent_photos: list[dict] = []
        self.sent_voices: list[dict] = []
        self.sent_invoices: list[dict] = []
        self.precheckout_answers: list[dict] = []
        self.downloaded_files: list[dict] = []
        self.my_commands_payload: dict | None = None
        self.menu_button_payload: dict | None = None

    async def get_updates(self, offset, timeout, allowed_updates):
        return []

    async def send_message(self, chat_id: str, text: str, reply_markup=None, reply_to_message_id=None):
        self.sent_messages.append(
            {
                "chat_id": chat_id,
                "text": text,
                "reply_markup": reply_markup,
                "reply_to_message_id": reply_to_message_id,
            }
        )
        return {"message_id": 1}

    async def send_photo(self, chat_id: str, photo_file_id=None, photo_path=None, caption=None, reply_markup=None):
        self.sent_photos.append(
            {
                "chat_id": chat_id,
                "photo_file_id": photo_file_id,
                "photo_path": str(photo_path) if photo_path else None,
                "caption": caption,
                "reply_markup": reply_markup,
            }
        )
        return {"message_id": 77}

    async def send_voice(self, chat_id: str, voice_file_id=None, voice_path=None, caption=None, reply_markup=None):
        self.sent_voices.append(
            {
                "chat_id": chat_id,
                "voice_file_id": voice_file_id,
                "voice_path": str(voice_path) if voice_path else None,
                "caption": caption,
                "reply_markup": reply_markup,
            }
        )
        return {"message_id": 78}

    async def send_invoice(self, **kwargs):
        self.sent_invoices.append(kwargs)
        return {"message_id": 88}

    async def answer_pre_checkout_query(self, pre_checkout_query_id: str, ok: bool, error_message: str | None = None):
        self.precheckout_answers.append(
            {
                "pre_checkout_query_id": pre_checkout_query_id,
                "ok": ok,
                "error_message": error_message,
            }
        )
        return {"ok": True}

    async def answer_callback_query(self, callback_query_id: str, text: str | None = None):
        return {"ok": True}

    async def set_my_commands(self, commands, *, language_code=None, scope=None):
        self.my_commands_payload = {
            "commands": commands,
            "language_code": language_code,
            "scope": scope,
        }
        return {"ok": True}

    async def set_chat_menu_button(self, chat_id=None, menu_button=None):
        self.menu_button_payload = {
            "chat_id": chat_id,
            "menu_button": menu_button,
        }
        return {"ok": True}

    async def get_file(self, file_id: str):
        return {"file_path": f"{file_id}.jpg", "file_size": 32}

    async def download_file(self, file_path: str, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"receipt-bytes")
        self.downloaded_files.append({"file_path": file_path, "output_path": str(output_path)})
        return output_path

    async def aclose(self):
        await self.client.aclose()


class FakeCryptoPayClient:
    def __init__(self) -> None:
        self.created: list[dict] = []
        self.invoices: dict[int, dict] = {}

    async def create_invoice(self, *, amount_usd: float, payload: str, description: str, paid_btn_url: str | None = None):
        invoice_id = len(self.created) + 1000
        item = {
            "invoice_id": invoice_id,
            "bot_invoice_url": f"https://t.me/CryptoBot?start=invoice_{invoice_id}",
            "status": "active",
            "payload": payload,
        }
        self.created.append(item)
        self.invoices[invoice_id] = item
        return item

    async def get_invoices(self, invoice_ids: list[int]):
        return [self.invoices[i] for i in invoice_ids if i in self.invoices]

    async def aclose(self):
        return None


class DummySettings:
    telegram_poll_timeout_sec = 30
    bale_poll_timeout_sec = 30
    telegram_allowed_updates = ["message", "callback_query", "pre_checkout_query"]
    telegram_enable_button_styles = False
    telegram_button_style_mode = "none"
    telegram_set_commands_on_start = False
    telegram_set_menu_button_on_start = False
    bale_allowed_updates = ["message", "callback_query", "pre_checkout_query"]
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
    forward_caption_template = "[{platform}] {display_name} @{username} {bridge_id}"
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


def make_test_catalog():
    catalog = copy.deepcopy(load_sales_catalog(str(Path(__file__).resolve().parents[1] / "config" / "sales_catalog.json")))
    catalog.rules.enforce_credits = False
    catalog.usdt_rate_channel = ""
    return catalog


@pytest.mark.asyncio
async def test_active_session_relays_text(tmp_path, monkeypatch) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    user = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "bob", "Bob")
    await repo.mark_terms_accepted(user.id)
    await repo.mark_terms_accepted(target.id)
    await repo.complete_registration(user.id, "09120000001")
    await repo.complete_registration(target.id, "09120000002")
    await repo.set_active_session(user.id, target.id)

    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=DummyClient(Platform.TELEGRAM),
        bale_client=DummyClient(Platform.BALE),
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    called = {"relay": False}

    async def fake_relay(u, msg):
        called["relay"] = True

    monkeypatch.setattr(svc, "_relay_user_message", fake_relay)

    incoming = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=1,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=1,
        content_type=ContentType.TEXT,
        text="سلام",
    )

    fresh = await repo.get_user_by_id(user.id)
    assert fresh is not None
    await svc._handle_registered_menu_or_relay(fresh, incoming)
    assert called["relay"] is True

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_relay_success_reports_delivery_status(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    sender = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "bob", "Bob")
    await repo.mark_terms_accepted(sender.id)
    await repo.complete_registration(sender.id, "09120000001")
    await repo.mark_terms_accepted(target.id)
    await repo.complete_registration(target.id, "09120000002")
    await repo.set_active_session(sender.id, target.id)

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=2,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=2,
        content_type=ContentType.TEXT,
        text="hello",
    )
    user = await repo.get_user_by_id(sender.id)
    assert user is not None
    await svc._relay_user_message(user, incoming)

    assert bale.sent_messages
    assert tg.sent_messages[-1]["text"] == "✅ ارسال با موفقیت انجام شد."

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_payment_history_menu_and_detail_callback(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    user = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    await repo.mark_terms_accepted(user.id)
    await repo.complete_registration(user.id, "09120000001")
    await repo.create_payment_order(
        requester_user_id=user.id,
        beneficiary_user_id=user.id,
        identity_key="telegram:u1",
        package_id="starter-100",
        payment_method="telegram_stars",
        status="PENDING_REVIEW",
        amount_usd=0.01,
        amount_stars=1,
        invoice_payload="test-payload-1",
        account_id=None,
        receipt_file_id=None,
        receipt_file_platform=None,
        receipt_caption=None,
    )

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming_menu = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=70,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=70,
        content_type=ContentType.TEXT,
        text=BTN_PAYMENT_HISTORY,
    )
    fresh = await repo.get_user_by_id(user.id)
    assert fresh is not None
    await svc._handle_registered_menu_or_relay(fresh, incoming_menu)

    assert tg.sent_messages
    history_msg = tg.sent_messages[-1]
    assert "تاریخچه پرداخت" in history_msg["text"]
    assert history_msg["reply_markup"]["inline_keyboard"][0][0]["callback_data"].startswith("payh:open:")

    callback = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=71,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=71,
        content_type=ContentType.TEXT,
        is_callback=True,
        callback_data="payh:open:1:0",
        callback_query_id="cb-history-1",
    )
    await svc._handle_callback(fresh, callback)
    assert "جزئیات سفارش" in tg.sent_messages[-1]["text"]

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_deliver_adds_inline_reply_actions(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    source = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "bob", "Bob")
    await repo.mark_terms_accepted(source.id)
    await repo.mark_terms_accepted(target.id)
    await repo.complete_registration(source.id, "09120000001")
    await repo.complete_registration(target.id, "09120000002")

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    await svc._deliver(
        source_platform=source.platform,
        dest_user=target,
        content_type=ContentType.TEXT,
        text="hello",
        source_file_id=None,
        caption=None,
        reply_source_user_id=source.id,
    )

    assert bale.sent_messages
    markup = bale.sent_messages[-1]["reply_markup"]
    assert markup is not None
    first_row = markup["inline_keyboard"][0]
    callbacks = {button["callback_data"] for button in first_row}
    assert f"in:connect:{source.id}" in callbacks
    assert f"in:reply:{source.id}" in callbacks

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_callback_reply_sets_active_session(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    source = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "bob", "Bob")
    await repo.mark_terms_accepted(source.id)
    await repo.mark_terms_accepted(target.id)
    await repo.complete_registration(source.id, "09120000001")
    await repo.complete_registration(target.id, "09120000002")

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming = IncomingMessage(
        platform=target.platform,
        update_id=22,
        chat_id=target.chat_id,
        user_id=target.platform_user_id,
        username=target.username,
        display_name=target.display_name,
        message_id=5,
        content_type=ContentType.TEXT,
        is_callback=True,
        callback_data=f"in:reply:{source.id}",
        callback_query_id="cb-22",
    )

    await svc._handle_callback(target, incoming)
    active = await repo.get_active_target(target.id)
    assert active is not None
    assert active.id == source.id
    assert bale.sent_messages
    assert "آماده پاسخ" in bale.sent_messages[-1]["text"]

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


def test_normalize_telegram_channel_target() -> None:
    assert BridgeService._normalize_telegram_channel_target("1003790742908") == "-1003790742908"
    assert BridgeService._normalize_telegram_channel_target("-1003790742908") == "-1003790742908"
    assert BridgeService._normalize_telegram_channel_target("https://t.me/example_channel") == "@example_channel"


def test_extract_usdt_rate_from_channel_text() -> None:
    text = "قیمت تتر امروز: ۱۵۳,۲۰۰ تومان"
    assert BridgeService._extract_usdt_rate_from_text(text) == 153200


@pytest.mark.asyncio
async def test_request_identifier_accepts_username_only(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    requester = await repo.upsert_user_presence(Platform.BALE, "u1", "c1", "alice", "Alice")
    await repo.mark_terms_accepted(requester.id)
    await repo.complete_registration(requester.id, "09120000001")
    await repo.set_user_state(
        requester.id,
        "REQUEST_WAIT_IDENTIFIER",
        {"target_platform": Platform.TELEGRAM.value},
    )

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming = IncomingMessage(
        platform=Platform.BALE,
        update_id=7,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=7,
        content_type=ContentType.TEXT,
        text="@target_user",
    )

    user = await repo.get_user_by_id(requester.id)
    state = await repo.get_user_state(requester.id)
    assert user is not None
    assert state is not None
    handled = await svc._handle_registered_state(user, incoming, state)
    assert handled is True

    state = await repo.get_user_state(requester.id)
    assert state is not None
    assert state.state == "REQUEST_WAIT_NOTE"
    assert state.data["target_username"] == "target_user"
    assert state.data["target_phone"] is None

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_request_identifier_detects_registered_username(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    requester = await repo.upsert_user_presence(Platform.BALE, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.TELEGRAM, "u2", "c2", "target_user", "Target")
    await repo.mark_terms_accepted(requester.id)
    await repo.complete_registration(requester.id, "09120000001")
    await repo.mark_terms_accepted(target.id)
    await repo.complete_registration(target.id, "09120000002")
    await repo.set_user_state(
        requester.id,
        "REQUEST_WAIT_IDENTIFIER",
        {"target_platform": Platform.TELEGRAM.value},
    )

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming = IncomingMessage(
        platform=Platform.BALE,
        update_id=8,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=8,
        content_type=ContentType.TEXT,
        text="@target_user",
    )

    user = await repo.get_user_by_id(requester.id)
    state = await repo.get_user_state(requester.id)
    assert user is not None
    assert state is not None
    handled = await svc._handle_registered_state(user, incoming, state)
    assert handled is True

    new_state = await repo.get_user_state(requester.id)
    assert new_state is None
    assert bale.sent_messages
    assert "ثبت‌نام شده" in bale.sent_messages[-1]["text"]

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_registered_user_can_be_notified_when_requested_target_joins(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    requester = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "target_user", "Target")
    await repo.mark_terms_accepted(requester.id)
    await repo.complete_registration(requester.id, "09120000001")
    await repo.create_admin_request(
        requester_user_id=requester.id,
        target_platform=Platform.BALE,
        target_identifier="phone=09120000002 | username=@target_user",
        target_phone="09120000002",
        target_username="target_user",
        note="friend",
    )

    await repo.mark_terms_accepted(target.id)
    await repo.complete_registration(target.id, "09120000002")

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    fresh_target = await repo.get_user_by_id(target.id)
    assert fresh_target is not None
    await svc._notify_requesters_target_joined(fresh_target)

    assert tg.sent_messages
    assert "در ربات فندق عضو شد" in tg.sent_messages[-1]["text"]

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_requester_notified_on_real_registration_flow(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    requester = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "target_user", "Target")
    await repo.mark_terms_accepted(requester.id)
    await repo.complete_registration(requester.id, "09120000001")
    await repo.mark_terms_accepted(target.id)
    await repo.set_user_state(target.id, FlowState.REG_WAIT_PHONE, {})
    await repo.create_admin_request(
        requester_user_id=requester.id,
        target_platform=Platform.BALE,
        target_identifier="phone=09120000002 | username=@target_user",
        target_phone="09120000002",
        target_username="target_user",
        note="friend",
    )

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming = IncomingMessage(
        platform=Platform.BALE,
        update_id=61,
        chat_id="c2",
        user_id="u2",
        username="target_user",
        display_name="Target",
        message_id=61,
        content_type=ContentType.TEXT,
        text="09120000002",
    )
    user = await repo.get_user_by_id(target.id)
    state = await repo.get_user_state(target.id)
    assert user is not None
    assert state is not None
    await svc._handle_unregistered(user, incoming, state)

    assert tg.sent_messages
    assert any("در ربات فندق عضو شد" in item["text"] for item in tg.sent_messages)

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_connect_ambiguous_results_show_command_links(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    requester = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    bale_target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "sameuser", "Bale User")
    tg_target = await repo.upsert_user_presence(Platform.TELEGRAM, "u3", "c3", "sameuser", "Telegram User")
    for user, phone in (
        (requester, "09120000001"),
        (bale_target, "09120000002"),
        (tg_target, "09120000003"),
    ):
        await repo.mark_terms_accepted(user.id)
        await repo.complete_registration(user.id, phone)

    await repo.set_user_state(requester.id, "CONNECT_WAIT_IDENTIFIER", {"target_platform": ""})

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )


    incoming = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=31,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=31,
        content_type=ContentType.TEXT,
        text="@sameuser",
    )

    user = await repo.get_user_by_id(requester.id)
    state = await repo.get_user_state(requester.id)
    assert user is not None
    assert state is not None
    handled = await svc._handle_registered_state(user, incoming, state)
    assert handled is True
    assert tg.sent_messages
    text = tg.sent_messages[-1]["text"]
    assert "برای اتصال، یکی از گزینه‌های زیر را انتخاب کنید" in text
    assert f"/connect_user_{bale_target.id}" in text
    assert f"/connect_user_{tg_target.id}" in text

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_connect_user_command_sets_active_session(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    requester = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "bob", "Bob")
    await repo.mark_terms_accepted(requester.id)
    await repo.complete_registration(requester.id, "09120000001")
    await repo.mark_terms_accepted(target.id)
    await repo.complete_registration(target.id, "09120000002")
    await repo.set_user_state(requester.id, "CONNECT_WAIT_IDENTIFIER", {"target_platform": ""})

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=32,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=32,
        content_type=ContentType.TEXT,
        text="/connect_user_2",
    )

    await svc._process_incoming(incoming)
    active = await repo.get_active_target(requester.id)
    assert active is not None
    assert active.id == target.id
    assert tg.sent_messages
    assert "اتصال فعال شد" in tg.sent_messages[-1]["text"]

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_credit_enforcement_counts_text_segments_and_blocks_when_insufficient(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    sender = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "bob", "Bob")
    await repo.mark_terms_accepted(sender.id)
    await repo.complete_registration(sender.id, "09120000001")
    await repo.mark_terms_accepted(target.id)
    await repo.complete_registration(target.id, "09120000002")
    await repo.set_active_session(sender.id, target.id)

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    catalog = make_test_catalog()
    catalog.rules.enforce_credits = True
    catalog.rules.text_segment_chars = 200

    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=catalog,
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=40,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=40,
        content_type=ContentType.TEXT,
        text="x" * 201,
    )
    user = await repo.get_user_by_id(sender.id)
    assert user is not None

    await svc._handle_registered_menu_or_relay(user, incoming)
    assert bale.sent_messages == []
    assert "اعتبار شما برای این ارسال کافی نیست" in tg.sent_messages[-1]["text"]

    await repo.apply_credit_delta(
        identity_key="telegram:u1",
        user_id=sender.id,
        entry_type="TOPUP",
        text_units_delta=2,
        voice_minutes_delta=0,
        photo_count_delta=0,
        package_id=None,
        payment_order_id=None,
        note="test topup",
    )
    await svc._handle_registered_menu_or_relay(user, incoming)
    assert bale.sent_messages
    wallet = await repo.get_wallet("telegram:u1")
    assert wallet.text_units_remaining == 0

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_stars_payment_flow_requires_admin_approval_and_credits_requester(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    telegram_user = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    bale_user = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "alice_bale", "Alice Bale")
    for item in (telegram_user, bale_user):
        await repo.mark_terms_accepted(item.id)
        await repo.complete_registration(item.id, "09120000001")

    settings = DummySettings()
    settings.telegram_admin_channel_id = "-1003790742908"
    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=settings,
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    user = await repo.get_user_by_id(telegram_user.id)
    assert user is not None
    await svc._start_package_payment(user, "starter-100", "telegram_stars")
    assert tg.sent_invoices
    payload = tg.sent_invoices[-1]["payload"]
    starter = svc.sales_catalog.package_by_id("starter-100")
    assert starter is not None
    stars_price = svc._stars_price(starter)
    assert stars_price is not None

    pre_checkout = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=41,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=0,
        content_type=ContentType.PRE_CHECKOUT,
        payment_payload=payload,
        payment_currency="XTR",
        payment_total_amount=stars_price,
        pre_checkout_query_id="pcq-1",
    )
    await svc._handle_pre_checkout(user, pre_checkout)
    assert tg.precheckout_answers[-1]["ok"] is True

    successful = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=42,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=42,
        content_type=ContentType.SUCCESSFUL_PAYMENT,
        payment_payload=payload,
        payment_currency="XTR",
        payment_total_amount=stars_price,
        telegram_payment_charge_id="tg-charge-1",
        provider_payment_charge_id="provider-charge-1",
    )
    await svc._handle_successful_payment(user, successful)

    # before admin approval, wallet must remain unchanged
    pending_wallet = await repo.get_wallet("telegram:u1")
    assert pending_wallet.text_units_remaining == 0
    assert tg.sent_messages
    assert any("برای بررسی ادمین" in item["text"] for item in tg.sent_messages)

    order = await repo.get_payment_order(1)
    assert order is not None
    assert order.status == "PENDING_REVIEW"
    assert order.admin_channel_chat_id == "-1003790742908"

    admin_callback = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=43,
        chat_id="-1003790742908",
        user_id="999",
        username="admin",
        display_name="Admin",
        message_id=1,
        content_type=ContentType.TEXT,
        is_callback=True,
        chat_type="channel",
        callback_data=f"pay:approve:{order.id}",
        callback_query_id="cb-pay-stars-1",
    )
    await svc._handle_admin_callback(admin_callback)

    wallet = await repo.get_wallet("telegram:u1")
    assert wallet.text_units_remaining == 100
    assert wallet.voice_minutes_remaining == 10
    assert wallet.photo_count_remaining == 10
    assert any("تایید شد" in item["text"] or "شارژ شد" in item["text"] for item in tg.sent_messages)
    assert not any("شارژ شد" in item["text"] for item in bale.sent_messages)

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_manual_bank_receipt_from_bale_goes_to_admin_channel_and_approval_credits_wallet(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    bale_user = await repo.upsert_user_presence(Platform.BALE, "u1", "c1", "alice", "Alice")
    telegram_user = await repo.upsert_user_presence(Platform.TELEGRAM, "u2", "c2", "alice_tg", "Alice TG")
    for item in (bale_user, telegram_user):
        await repo.mark_terms_accepted(item.id)
        await repo.complete_registration(item.id, "09120000001")

    settings = DummySettings()
    settings.telegram_admin_channel_id = "-1003790742908"
    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=settings,
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    await repo.set_user_state(
        bale_user.id,
        FlowState.PAYMENT_MANUAL_WAIT_RECEIPT,
        {"package_id": "starter-100", "payment_method": "manual_bank_transfer", "account_id": "iran-main"},
    )
    state = await repo.get_user_state(bale_user.id)
    user = await repo.get_user_by_id(bale_user.id)
    assert state is not None
    assert user is not None

    receipt = IncomingMessage(
        platform=Platform.BALE,
        update_id=43,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=43,
        content_type=ContentType.PHOTO,
        source_file_id="receipt-1",
        caption="bank receipt",
    )
    handled = await svc._handle_registered_state(user, receipt, state)
    assert handled is True
    assert tg.sent_photos
    assert tg.sent_photos[-1]["chat_id"] == "-1003790742908"

    order = await repo.get_payment_order(1)
    assert order is not None
    assert order.status == "PENDING_MANUAL"
    assert order.admin_channel_chat_id == "-1003790742908"
    assert order.admin_channel_message_id == 77

    admin_callback = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=44,
        chat_id="-1003790742908",
        user_id="999",
        username="admin",
        display_name="Admin",
        message_id=77,
        content_type=ContentType.TEXT,
        is_callback=True,
        chat_type="channel",
        callback_data=f"pay:approve:{order.id}",
        callback_query_id="cb-pay-1",
    )
    await svc._handle_admin_callback(admin_callback)

    wallet = await repo.get_wallet("bale:u1")
    assert wallet.text_units_remaining == 100
    assert wallet.voice_minutes_remaining == 10
    assert wallet.photo_count_remaining == 10
    assert any("تایید شد" in item["text"] for item in bale.sent_messages)
    assert not any("تایید شد" in item["text"] for item in tg.sent_messages if item["chat_id"] == "c2")


@pytest.mark.asyncio
async def test_bale_wallet_payment_flow_credits_target_wallet(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    bale_user = await repo.upsert_user_presence(Platform.BALE, "u1", "c1", "alice", "Alice")
    await repo.mark_terms_accepted(bale_user.id)
    await repo.complete_registration(bale_user.id, "09120000001")

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    settings = DummySettings()
    settings.telegram_admin_channel_id = "-1003790742908"
    svc = BridgeService(
        settings=settings,
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    user = await repo.get_user_by_id(bale_user.id)
    assert user is not None
    starter = svc.sales_catalog.package_by_id("starter-100")
    assert starter is not None
    wallet_rial = svc._wallet_price_rial(starter)
    assert wallet_rial is not None
    await svc._start_package_payment(user, "starter-100", "bale_wallet")

    assert bale.sent_invoices
    invoice = bale.sent_invoices[-1]
    payload = invoice["payload"]
    assert invoice["provider_token"]
    assert invoice["prices"] == [{"label": starter.title[:32], "amount": wallet_rial}]

    pre_checkout = IncomingMessage(
        platform=Platform.BALE,
        update_id=90,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=0,
        content_type=ContentType.PRE_CHECKOUT,
        payment_payload=payload,
        payment_currency="IRR",
        payment_total_amount=wallet_rial,
        pre_checkout_query_id="pcq-bale-1",
    )
    await svc._handle_pre_checkout(user, pre_checkout)
    assert bale.precheckout_answers[-1]["ok"] is True

    successful = IncomingMessage(
        platform=Platform.BALE,
        update_id=91,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=91,
        content_type=ContentType.SUCCESSFUL_PAYMENT,
        payment_payload=payload,
        payment_currency="IRR",
        payment_total_amount=wallet_rial,
        telegram_payment_charge_id="bale-charge-1",
        provider_payment_charge_id="provider-bale-1",
    )
    await svc._handle_successful_payment(user, successful)

    pending_wallet = await repo.get_wallet("bale:u1")
    assert pending_wallet.text_units_remaining == 0
    order = await repo.get_payment_order(1)
    assert order is not None
    assert order.status == "PENDING_REVIEW"
    assert order.admin_channel_chat_id == "-1003790742908"
    assert tg.sent_messages
    assert tg.sent_messages[-1]["chat_id"] == "-1003790742908"

    admin_callback = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=92,
        chat_id="-1003790742908",
        user_id="999",
        username="admin",
        display_name="Admin",
        message_id=1,
        content_type=ContentType.TEXT,
        is_callback=True,
        chat_type="channel",
        callback_data=f"pay:approve:{order.id}",
        callback_query_id="cb-pay-bale-1",
    )
    await svc._handle_admin_callback(admin_callback)

    wallet = await repo.get_wallet("bale:u1")
    assert wallet.text_units_remaining == 100
    assert wallet.voice_minutes_remaining == 10
    assert wallet.photo_count_remaining == 10
    assert any("تایید شد" in item["text"] or "شارژ شد" in item["text"] for item in bale.sent_messages)

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_stars_payment_for_other_user_credits_beneficiary_only(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    payer = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "payer", "Payer")
    beneficiary = await repo.upsert_user_presence(Platform.TELEGRAM, "u2", "c2", "target", "Target")
    for item, phone in ((payer, "09120000001"), (beneficiary, "09120000002")):
        await repo.mark_terms_accepted(item.id)
        await repo.complete_registration(item.id, phone)

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    settings = DummySettings()
    settings.telegram_admin_channel_id = "-1003790742908"
    svc = BridgeService(
        settings=settings,
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    payer_user = await repo.get_user_by_id(payer.id)
    beneficiary_user = await repo.get_user_by_id(beneficiary.id)
    assert payer_user is not None
    assert beneficiary_user is not None
    starter = svc.sales_catalog.package_by_id("starter-100")
    assert starter is not None
    stars_price = svc._stars_price(starter)
    assert stars_price is not None

    await svc._start_package_payment(
        payer_user,
        "starter-100",
        "telegram_stars",
        beneficiary_user=beneficiary_user,
    )
    assert tg.sent_invoices
    payload = tg.sent_invoices[-1]["payload"]

    pre_checkout = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=110,
        chat_id="c1",
        user_id="u1",
        username="payer",
        display_name="Payer",
        message_id=0,
        content_type=ContentType.PRE_CHECKOUT,
        payment_payload=payload,
        payment_currency="XTR",
        payment_total_amount=stars_price,
        pre_checkout_query_id="pcq-stars-gift-1",
    )
    await svc._handle_pre_checkout(payer_user, pre_checkout)
    assert tg.precheckout_answers[-1]["ok"] is True

    success = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=111,
        chat_id="c1",
        user_id="u1",
        username="payer",
        display_name="Payer",
        message_id=111,
        content_type=ContentType.SUCCESSFUL_PAYMENT,
        payment_payload=payload,
        payment_currency="XTR",
        payment_total_amount=stars_price,
        telegram_payment_charge_id="tg-gift-1",
        provider_payment_charge_id="provider-gift-1",
    )
    await svc._handle_successful_payment(payer_user, success)

    order = await repo.get_payment_order(1)
    assert order is not None
    assert order.status == "PENDING_REVIEW"

    admin_callback = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=112,
        chat_id="-1003790742908",
        user_id="999",
        username="admin",
        display_name="Admin",
        message_id=1,
        content_type=ContentType.TEXT,
        is_callback=True,
        chat_type="channel",
        callback_data=f"pay:approve:{order.id}",
        callback_query_id="cb-pay-stars-gift-1",
    )
    await svc._handle_admin_callback(admin_callback)

    beneficiary_wallet = await repo.get_wallet("telegram:u2")
    payer_wallet = await repo.get_wallet("telegram:u1")
    assert beneficiary_wallet.text_units_remaining == 100
    assert payer_wallet.text_units_remaining == 0
    assert any(msg["chat_id"] == "c2" and "شارژ" in msg["text"] for msg in tg.sent_messages)
    assert any(msg["chat_id"] == "c1" and "برای" in msg["text"] for msg in tg.sent_messages)

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_telegram_ton_wallet_payment_creates_link_and_order(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    payer = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "payer", "Payer")
    await repo.mark_terms_accepted(payer.id)
    await repo.complete_registration(payer.id, "09120000001")

    settings = DummySettings()
    settings.telegram_ton_pay_enabled = True
    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    crypto = FakeCryptoPayClient()
    svc = BridgeService(
        settings=settings,
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
        crypto_pay_client=crypto,
    )

    user = await repo.get_user_by_id(payer.id)
    assert user is not None
    await svc._start_package_payment(user, "starter-100", "telegram_ton_wallet")

    assert tg.sent_messages
    last = tg.sent_messages[-1]
    assert "لینک پرداخت تون آماده شد" in last["text"]
    markup = last["reply_markup"]
    assert markup is not None
    assert markup["inline_keyboard"][0][0]["url"].startswith("https://t.me/CryptoBot")

    order = await repo.get_payment_order(1)
    assert order is not None
    assert order.payment_method == "telegram_ton_wallet"
    assert order.status == "INVOICE_SENT"
    assert order.account_id == "1000"

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_telegram_ton_wallet_paid_auto_credits(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    payer = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "payer", "Payer")
    await repo.mark_terms_accepted(payer.id)
    await repo.complete_registration(payer.id, "09120000001")

    settings = DummySettings()
    settings.telegram_ton_pay_enabled = True
    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    crypto = FakeCryptoPayClient()
    svc = BridgeService(
        settings=settings,
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
        crypto_pay_client=crypto,
    )

    user = await repo.get_user_by_id(payer.id)
    assert user is not None
    await svc._start_package_payment(user, "starter-100", "telegram_ton_wallet")
    order = await repo.get_payment_order(1)
    assert order is not None
    assert order.account_id is not None

    invoice_id = int(order.account_id)
    crypto.invoices[invoice_id]["status"] = "paid"
    crypto.invoices[invoice_id]["hash"] = "ton_tx_hash_1"

    await svc._process_ton_wallet_order(order)

    updated = await repo.get_payment_order(order.id)
    assert updated is not None
    assert updated.status == "APPROVED"
    wallet = await repo.get_wallet("telegram:u1")
    assert wallet.text_units_remaining == 100
    assert wallet.voice_minutes_remaining == 10
    assert wallet.photo_count_remaining == 10
    assert any("پرداخت تون" in msg["text"] for msg in tg.sent_messages)

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_contact_list_uses_connection_label(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    owner = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    target = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "bob", "Bob")
    await repo.mark_terms_accepted(owner.id)
    await repo.complete_registration(owner.id, "09120000001")
    await repo.mark_terms_accepted(target.id)
    await repo.complete_registration(target.id, "09120000002")
    await repo.add_contact(owner.id, target.id, "balash")

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    user = await repo.get_user_by_id(owner.id)
    assert user is not None
    await svc._show_contacts(user)
    assert tg.sent_messages
    text = tg.sent_messages[-1]["text"]
    assert "🔗 مشاهده پروفایل: /contact_" in text
    assert "⚡ اتصال: /connect_user_" in text

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_support_button_sends_support_details(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    owner = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    await repo.mark_terms_accepted(owner.id)
    await repo.complete_registration(owner.id, "09120000001")

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming = IncomingMessage(
        platform=Platform.TELEGRAM,
        update_id=55,
        chat_id="c1",
        user_id="u1",
        username="alice",
        display_name="Alice",
        message_id=55,
        content_type=ContentType.TEXT,
        text="💬 ارتباط با پشتیبانی",
    )
    user = await repo.get_user_by_id(owner.id)
    assert user is not None
    await svc._handle_registered_menu_or_relay(user, incoming)
    assert tg.sent_messages
    assert "@fandogh_manager" in tg.sent_messages[-1]["text"]

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()


@pytest.mark.asyncio
async def test_seen_callback_notifies_sender_with_reply_reference(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    sender = await repo.upsert_user_presence(Platform.TELEGRAM, "u1", "c1", "alice", "Alice")
    reader = await repo.upsert_user_presence(Platform.BALE, "u2", "c2", "bob", "Bob")
    await repo.mark_terms_accepted(sender.id)
    await repo.complete_registration(sender.id, "09120000001")
    await repo.mark_terms_accepted(reader.id)
    await repo.complete_registration(reader.id, "09120000002")

    tg = DummyClient(Platform.TELEGRAM)
    bale = DummyClient(Platform.BALE)
    svc = BridgeService(
        settings=DummySettings(),
        sales_catalog=make_test_catalog(),
        repository=repo,
        telegram_client=tg,
        bale_client=bale,
        rate_limiter=InMemoryRateLimiter(RateLimitConfig(msg_per_min=100, media_per_min=100)),
    )

    incoming = IncomingMessage(
        platform=Platform.BALE,
        update_id=60,
        chat_id="c2",
        user_id="u2",
        username="bob",
        display_name="Bob",
        message_id=8,
        content_type=ContentType.TEXT,
        is_callback=True,
        callback_data=f"in:seen:{sender.id}:123",
        callback_query_id="seen-1",
    )
    user = await repo.get_user_by_id(reader.id)
    assert user is not None
    await svc._handle_callback(user, incoming)

    assert tg.sent_messages
    assert tg.sent_messages[-1]["chat_id"] == "c1"
    assert "مشاهده شد" in tg.sent_messages[-1]["text"]
    assert tg.sent_messages[-1]["reply_to_message_id"] == 123

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()

    await svc.telegram_client.aclose()
    await svc.bale_client.aclose()
