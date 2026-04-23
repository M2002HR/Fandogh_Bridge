from __future__ import annotations

import pytest

from bridge.db import init_db
from bridge.platforms.client import BotApiClient
from bridge.rate_limit import InMemoryRateLimiter, RateLimitConfig
from bridge.repository import Repository
from bridge.services.bridge_service import BridgeService
from bridge.types import ContentType, IncomingMessage, Platform


class DummyClient(BotApiClient):
    def __init__(self, platform: Platform) -> None:
        super().__init__(platform, token="t", api_base_url="https://example.org", file_base_url="https://example.org")
        self.sent_messages: list[dict] = []

    async def get_updates(self, offset, timeout, allowed_updates):
        return []

    async def send_message(self, chat_id: str, text: str, reply_markup=None):
        self.sent_messages.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return {"message_id": 1}

    async def answer_callback_query(self, callback_query_id: str, text: str | None = None):
        return {"ok": True}

    async def aclose(self):
        await self.client.aclose()


class DummySettings:
    telegram_poll_timeout_sec = 30
    bale_poll_timeout_sec = 30
    telegram_allowed_updates = ["message", "callback_query"]
    bale_allowed_updates = ["message", "callback_query"]
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
    message_max_text_len = 3500
    forward_caption_template = "[{platform}] {display_name} @{username} {bridge_id}"
    show_sender_platform = True
    show_sender_username = True
    show_sender_display_name = True
    admin_ids = []
    telegram_admin_channel_id = ""


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
