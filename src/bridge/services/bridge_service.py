from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from bridge.config import Settings
from bridge.platforms.client import BotApiClient
from bridge.platforms.parser import parse_update
from bridge.rate_limit import InMemoryRateLimiter
from bridge.repository import Repository
from bridge.services.ui import (
    BTN_ACCEPT_TERMS,
    BTN_ADD_CONTACT,
    BTN_BACK,
    BTN_BLOCK,
    BTN_CONNECT,
    BTN_CONNECT_CONTACT,
    BTN_CONTACTS,
    BTN_DECLINE_TERMS,
    BTN_DELETE_CONTACT,
    BTN_END_SESSION,
    BTN_HELP,
    BTN_MANUAL_PHONE,
    BTN_MY_ID,
    BTN_PLATFORM_ANY,
    BTN_PLATFORM_BALE,
    BTN_PLATFORM_TELEGRAM,
    BTN_PROFILE,
    BTN_REGISTER,
    BTN_REQUEST_ADMIN,
    BTN_SHARE_PHONE,
    BTN_SKIP_NOTE,
    BTN_UNBLOCK,
    contact_actions_menu,
    main_menu,
    note_menu,
    phone_menu,
    platform_menu,
    pre_login_menu,
    reply_keyboard,
    terms_menu,
)
from bridge.types import (
    ContactEntry,
    ContentType,
    DeliveryStatus,
    IncomingMessage,
    Platform,
    PlatformApiError,
    User,
    UserState,
)
from bridge.utils import normalize_phone, parse_iso, utc_iso, utc_now

logger = logging.getLogger(__name__)


class FlowState:
    REG_WAIT_TERMS = "REG_WAIT_TERMS"
    REG_WAIT_PHONE = "REG_WAIT_PHONE"
    REG_WAIT_PHONE_MANUAL = "REG_WAIT_PHONE_MANUAL"

    CONNECT_WAIT_PLATFORM = "CONNECT_WAIT_PLATFORM"
    CONNECT_WAIT_IDENTIFIER = "CONNECT_WAIT_IDENTIFIER"

    ADD_CONTACT_WAIT_PLATFORM = "ADD_CONTACT_WAIT_PLATFORM"
    ADD_CONTACT_WAIT_IDENTIFIER = "ADD_CONTACT_WAIT_IDENTIFIER"
    ADD_CONTACT_WAIT_ALIAS = "ADD_CONTACT_WAIT_ALIAS"

    CONTACTS_WAIT_SELECTION = "CONTACTS_WAIT_SELECTION"
    CONTACT_ACTION_WAIT = "CONTACT_ACTION_WAIT"

    REQUEST_WAIT_PLATFORM = "REQUEST_WAIT_PLATFORM"
    REQUEST_WAIT_IDENTIFIER = "REQUEST_WAIT_IDENTIFIER"
    REQUEST_WAIT_NOTE = "REQUEST_WAIT_NOTE"


MENU_TEXTS = {
    BTN_REGISTER,
    BTN_HELP,
    BTN_BACK,
    BTN_ACCEPT_TERMS,
    BTN_DECLINE_TERMS,
    BTN_SHARE_PHONE,
    BTN_MANUAL_PHONE,
    BTN_CONNECT,
    BTN_ADD_CONTACT,
    BTN_CONTACTS,
    BTN_MY_ID,
    BTN_END_SESSION,
    BTN_REQUEST_ADMIN,
    BTN_PLATFORM_TELEGRAM,
    BTN_PLATFORM_BALE,
    BTN_PLATFORM_ANY,
    BTN_BLOCK,
    BTN_UNBLOCK,
    BTN_PROFILE,
    BTN_DELETE_CONTACT,
    BTN_CONNECT_CONTACT,
    BTN_SKIP_NOTE,
}


@dataclass(slots=True)
class Metrics:
    incoming_total: int = 0
    delivered_total: int = 0
    queued_total: int = 0
    failed_total: int = 0


class BridgeService:
    def __init__(
        self,
        settings: Settings,
        repository: Repository,
        telegram_client: BotApiClient,
        bale_client: BotApiClient,
        rate_limiter: InMemoryRateLimiter,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.telegram_client = telegram_client
        self.bale_client = bale_client
        self.rate_limiter = rate_limiter
        self.metrics = Metrics()
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(
                self._poll_loop(
                    Platform.TELEGRAM,
                    self.settings.telegram_poll_timeout_sec,
                    self.settings.telegram_allowed_updates,
                )
            ),
            asyncio.create_task(
                self._poll_loop(
                    Platform.BALE,
                    self.settings.bale_poll_timeout_sec,
                    self.settings.bale_allowed_updates,
                )
            ),
            asyncio.create_task(self._outbox_worker()),
        ]
        if self.settings.metrics_enabled:
            tasks.append(asyncio.create_task(self._metrics_worker()))

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.telegram_client.aclose()
            await self.bale_client.aclose()

    def _client(self, platform: Platform) -> BotApiClient:
        if platform == Platform.TELEGRAM:
            return self.telegram_client
        return self.bale_client

    async def _poll_loop(self, platform: Platform, timeout: int, allowed_updates: list[str]) -> None:
        offset: int | None = None
        client = self._client(platform)

        while not self._stop_event.is_set():
            try:
                updates = await client.get_updates(offset=offset, timeout=timeout, allowed_updates=allowed_updates)
                for update in updates:
                    offset = int(update["update_id"]) + 1
                    incoming = parse_update(platform, update)
                    if incoming is None:
                        continue
                    self.metrics.incoming_total += 1
                    await self._process_incoming(incoming)
            except PlatformApiError as exc:
                logger.exception("Poll error on %s: %s", platform.value, exc)
                await asyncio.sleep(2)
            except Exception as exc:
                logger.exception("Unexpected poll error on %s: %s", platform.value, exc)
                await asyncio.sleep(2)

    async def _process_incoming(self, incoming: IncomingMessage) -> None:
        user = await self.repository.upsert_user_presence(
            platform=incoming.platform,
            platform_user_id=incoming.user_id,
            chat_id=incoming.chat_id,
            username=incoming.username,
            display_name=incoming.display_name,
        )
        state = await self.repository.get_user_state(user.id)

        command, args = self._extract_command(incoming.text)
        if command == "start":
            await self.repository.clear_user_state(user.id)
            if user.is_registered:
                await self._send_main_menu(user, "👋 خوش آمدید. از منو گزینه موردنظر را انتخاب کنید.")
            else:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "👋 به پل فندقی خوش آمدید. برای استفاده، ابتدا ثبت‌نام را انجام دهید.",
                    keyboard=pre_login_menu(),
                )
            return

        if command == "help":
            await self._send_help(user)
            return

        if command == "id":
            if user.is_registered:
                await self._send_text(user.platform, user.chat_id, f"🆔 شناسه شما: {user.bridge_id}", keyboard=main_menu())
            else:
                await self._send_text(user.platform, user.chat_id, "ابتدا ثبت‌نام کنید.", keyboard=pre_login_menu())
            return

        if command == "end":
            if user.is_registered:
                await self.repository.clear_active_session(user.id)
                await self._send_main_menu(user, "⛔ اتصال فعال پایان یافت.")
            else:
                await self._send_text(user.platform, user.chat_id, "ابتدا ثبت‌نام کنید.", keyboard=pre_login_menu())
            return

        if not user.is_registered:
            await self._handle_unregistered(user, incoming, state)
            return

        if state and await self._handle_registered_state(user, incoming, state):
            return

        await self._handle_registered_menu_or_relay(user, incoming)

    async def _handle_unregistered(self, user: User, incoming: IncomingMessage, state: UserState | None) -> None:
        text = (incoming.text or "").strip()

        if state and state.state == FlowState.REG_WAIT_TERMS:
            if text == BTN_ACCEPT_TERMS:
                await self.repository.mark_terms_accepted(user.id)
                await self.repository.set_user_state(user.id, FlowState.REG_WAIT_PHONE, {})
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "📱 لطفاً شماره موبایل خودتان را ارسال کنید.",
                    keyboard=phone_menu(user.platform),
                )
                return
            if text in {BTN_DECLINE_TERMS, BTN_BACK}:
                await self.repository.clear_user_state(user.id)
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "ثبت‌نام لغو شد. هر زمان آماده بودید دوباره اقدام کنید.",
                    keyboard=pre_login_menu(),
                )
                return

            await self._send_text(
                user.platform,
                user.chat_id,
                "برای ادامه ثبت‌نام، شرایط را قبول کنید یا انصراف دهید.",
                keyboard=terms_menu(),
            )
            return

        if state and state.state == FlowState.REG_WAIT_PHONE:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_text(user.platform, user.chat_id, "بازگشت به منوی اولیه.", keyboard=pre_login_menu())
                return

            if text == BTN_MANUAL_PHONE:
                await self.repository.set_user_state(user.id, FlowState.REG_WAIT_PHONE_MANUAL, {})
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "شماره را به فرمت 09xxxxxxxxx ارسال کنید.",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return

            phone = incoming.phone_number or normalize_phone(text)
            if not phone:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "شماره معتبر نیست. از دکمه ارسال شماره استفاده کنید یا دستی وارد کنید.",
                    keyboard=phone_menu(user.platform),
                )
                return

            await self.repository.complete_registration(user.id, phone)
            await self.repository.clear_user_state(user.id)
            fresh = await self.repository.get_user_by_id(user.id)
            if not fresh:
                raise RuntimeError("Failed to reload user after registration")
            await self._send_main_menu(
                fresh,
                f"✅ ثبت‌نام کامل شد.\n🆔 شناسه شما: {fresh.bridge_id}\nاز منو ادامه دهید.",
            )
            return

        if state and state.state == FlowState.REG_WAIT_PHONE_MANUAL:
            if text == BTN_BACK:
                await self.repository.set_user_state(user.id, FlowState.REG_WAIT_PHONE, {})
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "📱 لطفاً شماره موبایل خودتان را ارسال کنید.",
                    keyboard=phone_menu(user.platform),
                )
                return

            phone = normalize_phone(text)
            if not phone:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "شماره معتبر نیست. فرمت صحیح: 09xxxxxxxxx",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return

            await self.repository.complete_registration(user.id, phone)
            await self.repository.clear_user_state(user.id)
            fresh = await self.repository.get_user_by_id(user.id)
            if not fresh:
                raise RuntimeError("Failed to reload user after registration")
            await self._send_main_menu(
                fresh,
                f"✅ ثبت‌نام کامل شد.\n🆔 شناسه شما: {fresh.bridge_id}\nاز منو ادامه دهید.",
            )
            return

        if text == BTN_HELP:
            await self._send_help(user)
            return

        if text in {BTN_REGISTER, "/register"}:
            await self.repository.set_user_state(user.id, FlowState.REG_WAIT_TERMS, {})
            await self._send_text(user.platform, user.chat_id, self._terms_text(), keyboard=terms_menu())
            return

        await self._send_text(
            user.platform,
            user.chat_id,
            "برای شروع، روی «📝 ثبت‌نام» بزنید.",
            keyboard=pre_login_menu(),
        )

    async def _handle_registered_state(self, user: User, incoming: IncomingMessage, state: UserState) -> bool:
        text = (incoming.text or "").strip()

        if state.state == FlowState.CONNECT_WAIT_PLATFORM:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True
            target_platform = self._parse_platform_choice(text)
            if text == BTN_PLATFORM_ANY:
                target_platform = None
            if text not in {BTN_PLATFORM_ANY, BTN_PLATFORM_TELEGRAM, BTN_PLATFORM_BALE}:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "یکی از گزینه‌های پلتفرم را انتخاب کنید.",
                    keyboard=platform_menu(),
                )
                return True

            data = {"target_platform": target_platform.value if target_platform else ""}
            await self.repository.set_user_state(user.id, FlowState.CONNECT_WAIT_IDENTIFIER, data)
            await self._send_text(
                user.platform,
                user.chat_id,
                "شناسه مقصد را بفرستید: bridge_id یا شماره یا @username",
                keyboard=reply_keyboard([[BTN_BACK]]),
            )
            return True

        if state.state == FlowState.CONNECT_WAIT_IDENTIFIER:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            if text == BTN_REQUEST_ADMIN:
                target_platform = _platform_from_data(state.data.get("target_platform"))
                last_identifier = str(state.data.get("last_identifier") or "").strip()
                if not last_identifier:
                    await self._send_text(user.platform, user.chat_id, "ابتدا شناسه مقصد را وارد کنید.")
                    return True
                if not target_platform:
                    target_platform = _default_target_platform(user.platform)
                await self.repository.set_user_state(
                    user.id,
                    FlowState.REQUEST_WAIT_NOTE,
                    {
                        "target_platform": target_platform.value,
                        "target_identifier": last_identifier,
                    },
                )
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "یک توضیح کوتاه برای ادمین بنویسید (یا بدون توضیح).",
                    keyboard=note_menu(),
                )
                return True

            if incoming.content_type != ContentType.TEXT:
                await self._send_text(user.platform, user.chat_id, "لطفاً شناسه متنی ارسال کنید.")
                return True

            identifier = text
            target_platform = _platform_from_data(state.data.get("target_platform"))
            users = await self.repository.find_registered_users_by_identifier(identifier, target_platform)

            if not users:
                await self.repository.set_user_state(
                    user.id,
                    FlowState.CONNECT_WAIT_IDENTIFIER,
                    {
                        "target_platform": target_platform.value if target_platform else "",
                        "last_identifier": identifier,
                    },
                )
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "❗️مخاطبی با این مشخصات پیدا نشد.\nبرای اطلاع‌رسانی، درخواست ادمین ثبت کنید.",
                    keyboard=reply_keyboard([[BTN_REQUEST_ADMIN], [BTN_BACK]]),
                )
                return True

            if len(users) > 1:
                candidates = "\n".join([f"- {u.bridge_id} | {u.platform.value} | @{u.username or '-'}" for u in users[:8]])
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    f"چند کاربر پیدا شد. دقیق‌تر وارد کنید:\n{candidates}",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return True

            target = users[0]
            if target.id == user.id:
                await self._send_text(user.platform, user.chat_id, "نمی‌توانید به خودتان وصل شوید.", keyboard=reply_keyboard([[BTN_BACK]]))
                return True

            await self.repository.set_active_session(user.id, target.id)
            await self.repository.clear_user_state(user.id)
            await self._send_main_menu(
                user,
                f"🔌 اتصال فعال شد: {target.display_name or '-'} ({target.bridge_id})\nحالا متن/عکس/ویس بفرستید.",
            )
            return True

        if state.state == FlowState.ADD_CONTACT_WAIT_PLATFORM:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            target_platform = self._parse_platform_choice(text)
            if text == BTN_PLATFORM_ANY:
                target_platform = None
            if text not in {BTN_PLATFORM_ANY, BTN_PLATFORM_TELEGRAM, BTN_PLATFORM_BALE}:
                await self._send_text(user.platform, user.chat_id, "یکی از گزینه‌های پلتفرم را انتخاب کنید.", keyboard=platform_menu())
                return True

            await self.repository.set_user_state(
                user.id,
                FlowState.ADD_CONTACT_WAIT_IDENTIFIER,
                {"target_platform": target_platform.value if target_platform else ""},
            )
            await self._send_text(
                user.platform,
                user.chat_id,
                "شناسه مخاطب را بفرستید: bridge_id یا شماره یا @username",
                keyboard=reply_keyboard([[BTN_BACK]]),
            )
            return True

        if state.state == FlowState.ADD_CONTACT_WAIT_IDENTIFIER:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            if incoming.content_type != ContentType.TEXT:
                await self._send_text(user.platform, user.chat_id, "لطفاً شناسه متنی ارسال کنید.")
                return True

            target_platform = _platform_from_data(state.data.get("target_platform"))
            users = await self.repository.find_registered_users_by_identifier(text, target_platform)
            if not users:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "❗️مخاطب پیدا نشد. اگر هنوز ثبت‌نام نکرده، از منوی اصلی «🆘 درخواست اطلاع‌رسانی» را بزنید.",
                    keyboard=main_menu(),
                )
                await self.repository.clear_user_state(user.id)
                return True

            if len(users) > 1:
                candidates = "\n".join([f"- {u.bridge_id} | {u.platform.value} | @{u.username or '-'}" for u in users[:8]])
                await self._send_text(user.platform, user.chat_id, f"چند کاربر پیدا شد. دقیق‌تر وارد کنید:\n{candidates}")
                return True

            target = users[0]
            if target.id == user.id:
                await self._send_text(user.platform, user.chat_id, "نمی‌توانید خودتان را به مخاطبین اضافه کنید.")
                return True

            await self.repository.set_user_state(
                user.id,
                FlowState.ADD_CONTACT_WAIT_ALIAS,
                {"target_user_id": target.id},
            )
            await self._send_text(
                user.platform,
                user.chat_id,
                "برای این مخاطب یک نام دلخواه بنویسید.",
                keyboard=reply_keyboard([[BTN_BACK]]),
            )
            return True

        if state.state == FlowState.ADD_CONTACT_WAIT_ALIAS:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            if incoming.content_type != ContentType.TEXT:
                await self._send_text(user.platform, user.chat_id, "نام مخاطب باید متنی باشد.")
                return True

            alias = text.strip()
            if len(alias) < 2 or len(alias) > 40:
                await self._send_text(user.platform, user.chat_id, "نام مخاطب باید بین 2 تا 40 کاراکتر باشد.")
                return True

            target_user_id = int(state.data.get("target_user_id", 0))
            target = await self.repository.get_user_by_id(target_user_id)
            if not target:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "مخاطب نامعتبر بود. دوباره تلاش کنید.")
                return True

            try:
                entry = await self.repository.add_contact(user.id, target_user_id, alias)
            except Exception as exc:
                await self._send_text(user.platform, user.chat_id, f"ذخیره مخاطب ناموفق بود: {exc}")
                return True

            await self.repository.clear_user_state(user.id)
            await self._send_main_menu(user, f"✅ مخاطب ذخیره شد: {entry.alias}")
            return True

        if state.state == FlowState.CONTACTS_WAIT_SELECTION:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            contact_id = self._extract_contact_id(text)
            if not contact_id:
                await self._send_text(user.platform, user.chat_id, "یک مخاطب را از دکمه‌ها انتخاب کنید.")
                return True

            contact = await self.repository.get_contact(user.id, contact_id)
            if not contact:
                await self._show_contacts(user, "مخاطب یافت نشد. لیست بروزرسانی شد.")
                return True

            await self.repository.set_user_state(
                user.id,
                FlowState.CONTACT_ACTION_WAIT,
                {"contact_id": contact.id},
            )
            target = await self.repository.get_user_by_id(contact.target_user_id)
            if not target:
                await self._send_text(user.platform, user.chat_id, "پروفایل مخاطب یافت نشد.")
                await self._show_contacts(user)
                return True

            await self._send_text(
                user.platform,
                user.chat_id,
                self._contact_summary(contact, target),
                keyboard=contact_actions_menu(),
            )
            return True

        if state.state == FlowState.CONTACT_ACTION_WAIT:
            if text == BTN_BACK:
                await self._show_contacts(user)
                return True

            contact_id = int(state.data.get("contact_id", 0))
            contact = await self.repository.get_contact(user.id, contact_id)
            if not contact:
                await self._show_contacts(user, "مخاطب دیگر موجود نیست.")
                return True

            target = await self.repository.get_user_by_id(contact.target_user_id)
            if not target:
                await self._show_contacts(user, "کاربر مقصد یافت نشد.")
                return True

            if text == BTN_CONNECT_CONTACT:
                await self.repository.set_active_session(user.id, target.id)
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    f"🔌 به مخاطب «{contact.alias}» وصل شدید. حالا پیام بفرستید.",
                    keyboard=contact_actions_menu(),
                )
                return True

            if text == BTN_BLOCK:
                await self.repository.add_block(user.id, target.id)
                await self._send_text(user.platform, user.chat_id, f"🚫 {contact.alias} بلاک شد.", keyboard=contact_actions_menu())
                return True

            if text == BTN_UNBLOCK:
                await self.repository.remove_block(user.id, target.id)
                await self._send_text(user.platform, user.chat_id, f"✅ {contact.alias} از بلاک خارج شد.", keyboard=contact_actions_menu())
                return True

            if text == BTN_PROFILE:
                await self._send_profile(user, contact, target)
                return True

            if text == BTN_DELETE_CONTACT:
                await self.repository.delete_contact(user.id, contact.id)
                await self._show_contacts(user, "🗑 مخاطب حذف شد.")
                return True

            await self._send_text(user.platform, user.chat_id, "یک گزینه از منو انتخاب کنید.", keyboard=contact_actions_menu())
            return True

        if state.state == FlowState.REQUEST_WAIT_PLATFORM:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            target_platform = self._parse_platform_choice(text)
            if text == BTN_PLATFORM_ANY:
                target_platform = None

            if text not in {BTN_PLATFORM_ANY, BTN_PLATFORM_TELEGRAM, BTN_PLATFORM_BALE}:
                await self._send_text(user.platform, user.chat_id, "پلتفرم مقصد را انتخاب کنید.", keyboard=platform_menu())
                return True

            await self.repository.set_user_state(
                user.id,
                FlowState.REQUEST_WAIT_IDENTIFIER,
                {"target_platform": target_platform.value if target_platform else ""},
            )
            await self._send_text(
                user.platform,
                user.chat_id,
                "شناسه مخاطب را وارد کنید (bridge_id / شماره / @username)",
                keyboard=reply_keyboard([[BTN_BACK]]),
            )
            return True

        if state.state == FlowState.REQUEST_WAIT_IDENTIFIER:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            if incoming.content_type != ContentType.TEXT:
                await self._send_text(user.platform, user.chat_id, "شناسه باید متنی باشد.")
                return True

            target_platform = _platform_from_data(state.data.get("target_platform"))
            matches = await self.repository.find_registered_users_by_identifier(text, target_platform)
            if matches:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(
                    user,
                    "✅ این مخاطب الان ثبت‌نام کرده است. از گزینه «🔗 اتصال به مخاطب» استفاده کنید.",
                )
                return True

            await self.repository.set_user_state(
                user.id,
                FlowState.REQUEST_WAIT_NOTE,
                {
                    "target_platform": target_platform.value if target_platform else _default_target_platform(user.platform).value,
                    "target_identifier": text,
                },
            )
            await self._send_text(
                user.platform,
                user.chat_id,
                "یک توضیح برای ادمین بنویسید (یا بدون توضیح).",
                keyboard=note_menu(),
            )
            return True

        if state.state == FlowState.REQUEST_WAIT_NOTE:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            target_platform = _platform_from_data(state.data.get("target_platform")) or Platform.TELEGRAM
            target_identifier = str(state.data.get("target_identifier") or "").strip()
            if not target_identifier:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "درخواست نامعتبر بود. دوباره تلاش کنید.")
                return True

            note = None
            if text and text != BTN_SKIP_NOTE:
                note = text

            request_id = await self.repository.create_admin_request(
                requester_user_id=user.id,
                target_platform=target_platform,
                target_identifier=target_identifier,
                note=note,
            )
            delivered = await self._notify_admins(request_id, user, target_platform, target_identifier, note)
            await self.repository.clear_user_state(user.id)

            if delivered > 0:
                await self._send_main_menu(user, "✅ درخواست شما برای ادمین ارسال شد.")
            else:
                await self._send_main_menu(
                    user,
                    "⚠️ درخواست ثبت شد ولی پیام به ادمین ارسال نشد. تنظیم ADMIN_IDS را بررسی کنید.",
                )
            return True

        return False

    async def _handle_registered_menu_or_relay(self, user: User, incoming: IncomingMessage) -> None:
        text = (incoming.text or "").strip()

        if text == BTN_HELP:
            await self._send_help(user)
            return

        if text == BTN_MY_ID:
            await self._send_text(user.platform, user.chat_id, f"🆔 شناسه شما: {user.bridge_id}", keyboard=main_menu())
            return

        if text == BTN_END_SESSION:
            await self.repository.clear_active_session(user.id)
            await self._send_main_menu(user, "⛔ اتصال فعال قطع شد.")
            return

        if text == BTN_CONNECT:
            await self.repository.set_user_state(user.id, FlowState.CONNECT_WAIT_PLATFORM, {})
            await self._send_text(
                user.platform,
                user.chat_id,
                "پلتفرم مقصد را انتخاب کنید.",
                keyboard=platform_menu(),
            )
            return

        if text == BTN_ADD_CONTACT:
            await self.repository.set_user_state(user.id, FlowState.ADD_CONTACT_WAIT_PLATFORM, {})
            await self._send_text(
                user.platform,
                user.chat_id,
                "پلتفرم مخاطب را انتخاب کنید.",
                keyboard=platform_menu(),
            )
            return

        if text == BTN_CONTACTS:
            await self._show_contacts(user)
            return

        if text == BTN_REQUEST_ADMIN:
            await self.repository.set_user_state(user.id, FlowState.REQUEST_WAIT_PLATFORM, {})
            await self._send_text(
                user.platform,
                user.chat_id,
                "پلتفرم مخاطبی که هنوز عضو نشده را انتخاب کنید.",
                keyboard=platform_menu(),
            )
            return

        if incoming.content_type in {ContentType.TEXT, ContentType.PHOTO, ContentType.VOICE}:
            if incoming.content_type == ContentType.TEXT and text in MENU_TEXTS:
                await self._send_main_menu(user, "یک گزینه معتبر انتخاب کنید.")
                return
            await self._relay_user_message(user, incoming)
            return

        await self._send_main_menu(user, "نوع پیام پشتیبانی نمی‌شود.")

    async def _show_contacts(self, user: User, preface: str | None = None) -> None:
        contacts = await self.repository.list_contacts(user.id)
        if not contacts:
            await self.repository.clear_user_state(user.id)
            msg = "👥 مخاطبی ذخیره نشده است. از گزینه «➕ افزودن مخاطب» استفاده کنید."
            if preface:
                msg = f"{preface}\n\n{msg}"
            await self._send_text(user.platform, user.chat_id, msg, keyboard=main_menu())
            return

        await self.repository.set_user_state(user.id, FlowState.CONTACTS_WAIT_SELECTION, {})
        rows: list[list[str]] = []
        current_row: list[str] = []
        for c in contacts[:24]:
            label = f"👤 {c.alias} [{c.id}]"
            current_row.append(label)
            if len(current_row) == 2:
                rows.append(current_row)
                current_row = []
        if current_row:
            rows.append(current_row)
        rows.append([BTN_BACK])

        lines = ["👥 لیست مخاطبین فندقی:"]
        for c in contacts[:24]:
            target = await self.repository.get_user_by_id(c.target_user_id)
            if target:
                lines.append(f"- {c.alias} | {target.bridge_id} | {target.platform.value}")

        msg = "\n".join(lines)
        if preface:
            msg = f"{preface}\n\n{msg}"

        await self._send_text(user.platform, user.chat_id, msg, keyboard=reply_keyboard(rows))

    async def _send_profile(self, requester: User, contact: ContactEntry, target: User) -> None:
        chat_info: dict | None = None
        photo_file_id: str | None = None

        try:
            chat_info = await self._client(target.platform).get_chat(target.chat_id)
        except Exception:
            chat_info = None

        try:
            photos = await self._client(target.platform).get_user_profile_photos(target.platform_user_id, limit=1)
            if photos.get("photos"):
                first_group = photos["photos"][0]
                if isinstance(first_group, list) and first_group:
                    photo_file_id = first_group[-1].get("file_id")
        except Exception:
            photo_file_id = None

        bio = "-"
        if isinstance(chat_info, dict):
            bio = chat_info.get("bio") or "-"

        profile_text = (
            f"👤 پروفایل مخاطب: {contact.alias}\n"
            f"نام: {target.display_name or '-'}\n"
            f"نام کاربری: @{target.username or '-'}\n"
            f"پلتفرم: {target.platform.value}\n"
            f"شناسه فندقی: {target.bridge_id}\n"
            f"شماره: {target.phone_number or '-'}\n"
            f"بیو: {bio}"
        )

        if photo_file_id:
            try:
                await self._deliver(
                    source_platform=target.platform,
                    dest_user=requester,
                    content_type=ContentType.PHOTO,
                    text=None,
                    source_file_id=photo_file_id,
                    caption=profile_text,
                )
                return
            except Exception:
                pass

        await self._send_text(requester.platform, requester.chat_id, profile_text, keyboard=contact_actions_menu())

    async def _notify_admins(
        self,
        request_id: int,
        requester: User,
        target_platform: Platform,
        target_identifier: str,
        note: str | None,
    ) -> int:
        message = (
            "📨 درخواست اطلاع‌رسانی جدید\n"
            f"شماره درخواست: #{request_id}\n"
            f"درخواست‌دهنده: {requester.display_name or '-'}\n"
            f"bridge_id: {requester.bridge_id}\n"
            f"پلتفرم درخواست‌دهنده: {requester.platform.value}\n"
            f"مقصد: {target_platform.value}\n"
            f"شناسه مقصد: {target_identifier}\n"
            f"توضیح: {note or '-'}"
        )

        delivered = 0
        for admin_platform, admin_chat_id in self._iter_admin_targets():
            try:
                await self._client(admin_platform).send_message(admin_chat_id, message)
                delivered += 1
            except Exception as exc:
                logger.warning("Admin notify failed for %s:%s => %s", admin_platform.value, admin_chat_id, exc)
        return delivered

    def _iter_admin_targets(self) -> list[tuple[Platform, str]]:
        targets: list[tuple[Platform, str]] = []
        for raw in self.settings.admin_ids:
            item = str(raw).strip()
            if not item:
                continue

            if ":" in item:
                prefix, value = item.split(":", 1)
                p = prefix.strip().lower()
                cid = value.strip()
                if p == "telegram" and cid:
                    targets.append((Platform.TELEGRAM, cid))
                elif p == "bale" and cid:
                    targets.append((Platform.BALE, cid))
                continue

            # No prefix: try both platforms.
            targets.append((Platform.TELEGRAM, item))
            targets.append((Platform.BALE, item))

        # de-duplicate while preserving order
        dedup: list[tuple[Platform, str]] = []
        seen = set()
        for t in targets:
            if t in seen:
                continue
            seen.add(t)
            dedup.append(t)
        return dedup

    async def _send_main_menu(self, user: User, preface: str | None = None) -> None:
        target = await self.repository.get_active_target(user.id)
        status = "🔌 اتصال فعال ندارید"
        if target:
            status = f"🔌 اتصال فعال: {target.display_name or '-'} ({target.bridge_id})"

        text = status
        if preface:
            text = f"{preface}\n\n{status}"

        await self._send_text(user.platform, user.chat_id, text, keyboard=main_menu())

    async def _send_help(self, user: User) -> None:
        text = (
            "📘 راهنما\n\n"
            "1) ابتدا ثبت‌نام را انجام دهید (قبول مقررات + شماره موبایل).\n"
            "2) برای اتصال سریع: «🔗 اتصال به مخاطب».\n"
            "3) شناسه مقصد می‌تواند bridge_id یا شماره یا @username باشد.\n"
            "4) برای ذخیره مخاطب: «➕ افزودن مخاطب».\n"
            "5) برای مدیریت: «👥 لیست مخاطبین».\n"
            "6) اگر مخاطب عضو نیست: «🆘 درخواست اطلاع‌رسانی».\n"
            "7) بعد از اتصال، متن/عکس/ویس ارسال کنید."
        )

        keyboard = main_menu() if user.is_registered else pre_login_menu()
        await self._send_text(user.platform, user.chat_id, text, keyboard=keyboard)

    async def _relay_user_message(self, source_user: User, incoming: IncomingMessage) -> None:
        target = await self.repository.get_active_target(source_user.id)
        if not target:
            await self._send_main_menu(source_user, "ابتدا یک مخاطب را برای اتصال انتخاب کنید.")
            return

        is_blocked = await self.repository.is_blocked(target.id, source_user.id)
        if is_blocked:
            await self._send_text(
                source_user.platform,
                source_user.chat_id,
                "🚫 این کاربر شما را بلاک کرده است.",
                keyboard=main_menu(),
            )
            return

        if self.settings.rate_limit_enabled:
            kind = "text" if incoming.content_type == ContentType.TEXT else "media"
            if not self.rate_limiter.allow(source_user.id, kind):
                await self._send_text(
                    source_user.platform,
                    source_user.chat_id,
                    "⏳ محدودیت نرخ پیام فعال است. کمی بعد دوباره تلاش کنید.",
                    keyboard=main_menu(),
                )
                return

        sender_header = self._sender_header(source_user)
        text_payload: str | None = None
        caption_payload: str | None = None

        if incoming.content_type == ContentType.TEXT:
            body = (incoming.text or "").strip()
            if not body:
                return
            if len(body) > self.settings.message_max_text_len:
                body = body[: self.settings.message_max_text_len]
            text_payload = f"{sender_header}\n\n{body}"
        else:
            base = sender_header
            if incoming.caption:
                base = f"{base}\n\n{incoming.caption}"
            caption_payload = base

        try:
            await self._deliver(
                source_platform=source_user.platform,
                dest_user=target,
                content_type=incoming.content_type,
                text=text_payload,
                source_file_id=incoming.source_file_id,
                caption=caption_payload,
            )
            self.metrics.delivered_total += 1
            await self.repository.log_message(
                source_user_id=source_user.id,
                dest_user_id=target.id,
                source_platform=source_user.platform,
                dest_platform=target.platform,
                content_type=incoming.content_type,
                status=DeliveryStatus.SENT,
                error=None,
            )
        except Exception as exc:
            self.metrics.failed_total += 1
            logger.warning("Immediate delivery failed, queueing message: %s", exc)
            await self._queue_outbox(
                source_user_id=source_user.id,
                dest_user_id=target.id,
                content_type=incoming.content_type,
                text=text_payload,
                source_file_id=incoming.source_file_id,
                source_file_platform=source_user.platform,
                caption=caption_payload,
            )
            await self.repository.log_message(
                source_user_id=source_user.id,
                dest_user_id=target.id,
                source_platform=source_user.platform,
                dest_platform=target.platform,
                content_type=incoming.content_type,
                status=DeliveryStatus.FAILED,
                error=str(exc),
            )
            await self._send_text(
                source_user.platform,
                source_user.chat_id,
                "⚠️ ارسال فعلاً انجام نشد و در صف retry قرار گرفت.",
                keyboard=main_menu(),
            )

    async def _deliver(
        self,
        *,
        source_platform: Platform,
        dest_user: User,
        content_type: ContentType,
        text: str | None,
        source_file_id: str | None,
        caption: str | None,
    ) -> None:
        dest_client = self._client(dest_user.platform)

        if content_type == ContentType.TEXT:
            if not text:
                raise ValueError("Empty text payload")
            await dest_client.send_message(dest_user.chat_id, text)
            return

        if not source_file_id:
            raise ValueError("Missing source file id")

        if source_platform == dest_user.platform:
            if content_type == ContentType.PHOTO:
                await dest_client.send_photo(dest_user.chat_id, photo_file_id=source_file_id, caption=caption)
            elif content_type == ContentType.VOICE:
                await dest_client.send_voice(dest_user.chat_id, voice_file_id=source_file_id, caption=caption)
            else:
                raise ValueError("Unsupported media content type")
            return

        source_client = self._client(source_platform)
        file_meta = await source_client.get_file(source_file_id)
        file_path = file_meta.get("file_path")
        if not file_path:
            raise ValueError("file_path missing from getFile")

        max_download = self.settings.media_max_download_mb * 1024 * 1024
        file_size = int(file_meta.get("file_size") or 0)
        if file_size and file_size > max_download:
            raise ValueError("File exceeds MEDIA_MAX_DOWNLOAD_MB")

        suffix = Path(file_path).suffix or (".jpg" if content_type == ContentType.PHOTO else ".ogg")
        temp_path = Path(self.settings.media_tmp_dir) / f"{source_platform.value}_{source_file_id}{suffix}"
        await source_client.download_file(file_path, temp_path)

        try:
            max_upload = self.settings.media_max_upload_mb * 1024 * 1024
            if temp_path.stat().st_size > max_upload:
                raise ValueError("File exceeds MEDIA_MAX_UPLOAD_MB")

            if content_type == ContentType.PHOTO:
                await dest_client.send_photo(dest_user.chat_id, photo_path=temp_path, caption=caption)
            elif content_type == ContentType.VOICE:
                await dest_client.send_voice(dest_user.chat_id, voice_path=temp_path, caption=caption)
            else:
                raise ValueError("Unsupported media content type")
        finally:
            if self.settings.media_delete_after_send and temp_path.exists():
                temp_path.unlink(missing_ok=True)

    async def _queue_outbox(
        self,
        *,
        source_user_id: int,
        dest_user_id: int,
        content_type: ContentType,
        text: str | None,
        source_file_id: str | None,
        source_file_platform: Platform,
        caption: str | None,
    ) -> None:
        now = utc_now()
        retry_in = timedelta(seconds=self.settings.queue_retry_base_sec)
        expires_at = now + timedelta(hours=self.settings.queue_retry_max_hours)
        await self.repository.enqueue_outbox(
            source_user_id=source_user_id,
            dest_user_id=dest_user_id,
            content_type=content_type,
            text=text,
            source_file_id=source_file_id,
            source_file_platform=source_file_platform,
            caption=caption,
            next_retry_at=(now + retry_in).isoformat(),
            expires_at=expires_at.isoformat(),
        )
        self.metrics.queued_total += 1

    async def _outbox_worker(self) -> None:
        while not self._stop_event.is_set():
            if not self.settings.queue_retry_enabled:
                await asyncio.sleep(self.settings.queue_worker_interval_sec)
                continue

            due = await self.repository.fetch_due_outbox(now_iso=utc_iso(), limit=100)
            if not due:
                await asyncio.sleep(self.settings.queue_worker_interval_sec)
                continue

            for item in due:
                await self._process_outbox_item(item)

            await asyncio.sleep(0)

    async def _process_outbox_item(self, item) -> None:
        now = utc_now()
        if parse_iso(item.expires_at) <= now:
            await self.repository.mark_outbox_expired(item.id, "Retry window expired")
            return

        source_user = await self.repository.get_user_by_id(item.source_user_id)
        dest_user = await self.repository.get_user_by_id(item.dest_user_id)
        if not source_user or not dest_user:
            await self.repository.mark_outbox_expired(item.id, "Source or destination user missing")
            return

        if await self.repository.is_blocked(dest_user.id, source_user.id):
            await self.repository.mark_outbox_expired(item.id, "Blocked by destination user")
            return

        source_platform = item.source_file_platform or source_user.platform

        try:
            await self._deliver(
                source_platform=source_platform,
                dest_user=dest_user,
                content_type=item.content_type,
                text=item.text,
                source_file_id=item.source_file_id,
                caption=item.caption,
            )
            await self.repository.mark_outbox_sent(item.id)
            self.metrics.delivered_total += 1
            await self.repository.log_message(
                source_user_id=source_user.id,
                dest_user_id=dest_user.id,
                source_platform=source_user.platform,
                dest_platform=dest_user.platform,
                content_type=item.content_type,
                status=DeliveryStatus.SENT,
                error=None,
            )
        except Exception as exc:
            attempts = item.attempts + 1
            delay = min(self.settings.queue_retry_base_sec * (2**max(attempts - 1, 0)), self.settings.queue_retry_max_sec)
            next_retry = now + timedelta(seconds=delay)

            if next_retry >= parse_iso(item.expires_at):
                await self.repository.mark_outbox_expired(item.id, str(exc))
                return

            await self.repository.mark_outbox_retry(
                item.id,
                attempts=attempts,
                next_retry_at=next_retry.isoformat(),
                error=str(exc),
            )

    async def _metrics_worker(self) -> None:
        while not self._stop_event.is_set():
            logger.info(
                "metrics incoming=%s delivered=%s queued=%s failed=%s",
                self.metrics.incoming_total,
                self.metrics.delivered_total,
                self.metrics.queued_total,
                self.metrics.failed_total,
            )
            await asyncio.sleep(60)

    @staticmethod
    def _extract_command(text: str | None) -> tuple[str | None, list[str]]:
        if not text:
            return None, []
        stripped = text.strip()
        if not stripped.startswith("/"):
            return None, []

        parts = stripped.split()
        cmd = parts[0][1:]
        if "@" in cmd:
            cmd = cmd.split("@", 1)[0]
        return cmd.lower(), parts[1:]

    async def _send_text(self, platform: Platform, chat_id: str, text: str, keyboard: dict | None = None) -> None:
        client = self._client(platform)
        await client.send_message(chat_id, text, reply_markup=keyboard)

    def _sender_header(self, source_user: User) -> str:
        platform = source_user.platform.value if self.settings.show_sender_platform else ""
        username = source_user.username or "-"
        display_name = source_user.display_name or "Unknown"

        if not self.settings.show_sender_username:
            username = "-"
        if not self.settings.show_sender_display_name:
            display_name = "کاربر"

        return self.settings.forward_caption_template.format(
            platform=platform,
            username=username,
            display_name=display_name,
            bridge_id=source_user.bridge_id,
        )

    @staticmethod
    def _parse_platform_choice(text: str) -> Platform | None:
        if text == BTN_PLATFORM_TELEGRAM:
            return Platform.TELEGRAM
        if text == BTN_PLATFORM_BALE:
            return Platform.BALE
        return None

    @staticmethod
    def _extract_contact_id(text: str) -> int | None:
        if not text:
            return None
        match = re.search(r"\[(\d+)\]", text)
        if not match:
            return None
        return int(match.group(1))

    def _contact_summary(self, contact: ContactEntry, target: User) -> str:
        return (
            f"👤 مخاطب: {contact.alias}\n"
            f"شناسه فندقی: {target.bridge_id}\n"
            f"پلتفرم: {target.platform.value}\n"
            f"یوزرنیم: @{target.username or '-'}\n"
            f"شماره: {target.phone_number or '-'}"
        )

    @staticmethod
    def _terms_text() -> str:
        return (
            "📜 شرایط استفاده و مقررات پل فندقی\n\n"
            "با انتخاب «✅ قبول شرایط»، شما تأیید می‌کنید که:\n"
            "1) اطلاعات واردشده متعلق به خود شماست و از شماره واقعی خودتان استفاده می‌کنید.\n"
            "2) مسئولیت کامل محتوای ارسالی (متن، تصویر، صوت و...) با خود شماست.\n"
            "3) استفاده از سرویس برای هرگونه محتوای مجرمانه، توهین‌آمیز، کلاهبرداری، مزاحمت، نشر اکاذیب یا نقض حریم خصوصی ممنوع است.\n"
            "4) رعایت قوانین جمهوری اسلامی ایران الزامی است و مسئولیت هرگونه تخلف یا پیامد حقوقی/قضایی کاملاً بر عهده کاربر متخلف خواهد بود.\n"
            "5) این سرویس فقط نقش واسط انتقال پیام را دارد و تعهدی نسبت به اختلافات بین کاربران ندارد.\n"
            "6) در صورت گزارش سوءاستفاده، دسترسی کاربر می‌تواند محدود یا مسدود شود.\n"
            "7) اطلاعات فنی لازم برای امنیت و پایداری سرویس ممکن است ثبت شود.\n\n"
            "در صورت عدم موافقت، گزینه «❌ عدم پذیرش» را انتخاب کنید."
        )


def _platform_from_data(value: object) -> Platform | None:
    if not value:
        return None
    if str(value) == Platform.TELEGRAM.value:
        return Platform.TELEGRAM
    if str(value) == Platform.BALE.value:
        return Platform.BALE
    return None


def _default_target_platform(source_platform: Platform) -> Platform:
    return Platform.BALE if source_platform == Platform.TELEGRAM else Platform.TELEGRAM
