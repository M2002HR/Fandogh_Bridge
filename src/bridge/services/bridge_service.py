from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import httpx

from bridge.config import Settings
from bridge.platforms.client import BotApiClient
from bridge.platforms.parser import parse_update
from bridge.rate_limit import InMemoryRateLimiter
from bridge.repository import Repository
from bridge.services.ui import (
    BTN_ACCEPT_TERMS,
    BTN_ADD_CONTACT,
    BTN_BACK,
    BTN_CONNECT,
    BTN_CONTACTS,
    BTN_DECLINE_TERMS,
    BTN_END_SESSION,
    BTN_ENTER_PHONE,
    BTN_HELP,
    BTN_MY_ID,
    BTN_PLATFORM_ANY,
    BTN_PLATFORM_BALE,
    BTN_PLATFORM_TELEGRAM,
    BTN_REGISTER,
    BTN_REQUEST_ADMIN,
    BTN_SHARE_PHONE,
    BTN_SKIP_NOTE,
    connected_menu,
    contact_profile_actions,
    incoming_reply_actions,
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

    REQUEST_WAIT_PLATFORM = "REQUEST_WAIT_PLATFORM"
    REQUEST_WAIT_PHONE = "REQUEST_WAIT_PHONE"
    REQUEST_WAIT_NOTE = "REQUEST_WAIT_NOTE"


MENU_TEXTS = {
    BTN_REGISTER,
    BTN_HELP,
    BTN_BACK,
    BTN_ACCEPT_TERMS,
    BTN_DECLINE_TERMS,
    BTN_SHARE_PHONE,
    BTN_ENTER_PHONE,
    BTN_CONNECT,
    BTN_ADD_CONTACT,
    BTN_CONTACTS,
    BTN_MY_ID,
    BTN_END_SESSION,
    BTN_REQUEST_ADMIN,
    BTN_PLATFORM_TELEGRAM,
    BTN_PLATFORM_BALE,
    BTN_PLATFORM_ANY,
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
        self._recent_interactions: dict[tuple[str, str], float] = {}

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
                    update_id = int(update["update_id"])
                    offset = update_id + 1
                    claimed = await self.repository.claim_update(platform, update_id)
                    if not claimed:
                        continue
                    incoming = parse_update(platform, update)
                    if incoming is None:
                        continue
                    self.metrics.incoming_total += 1
                    await self._process_incoming(incoming)
            except PlatformApiError as exc:
                message = str(exc)
                if "getUpdates HTTP 409" in message:
                    logger.warning(
                        "Poll conflict on %s: another bot instance is already consuming updates.",
                        platform.value,
                    )
                    await asyncio.sleep(5)
                    continue
                logger.exception("Poll error on %s: %s", platform.value, exc)
                await asyncio.sleep(2)
            except httpx.ReadTimeout:
                # Normal on unstable networks / long-poll edge timing.
                continue
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

        if self._is_duplicate_interaction(user, incoming):
            return

        if incoming.is_callback:
            await self._handle_callback(user, incoming)
            return

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
                keyboard = connected_menu() if await self.repository.get_active_target(user.id) else main_menu()
                await self._send_text(user.platform, user.chat_id, f"🆔 فندق‌آیدی شما: {user.bridge_id}", keyboard=keyboard)
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

        if user.is_registered and command == "contacts":
            if await self.repository.get_active_target(user.id):
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "در حال اتصال فعال هستید. برای مدیریت مخاطبین ابتدا «⛔ پایان اتصال» را بزنید.",
                    keyboard=connected_menu(),
                )
                return
            await self._show_contacts(user, page=0)
            return

        if user.is_registered and command and command.startswith("contacts_"):
            if await self.repository.get_active_target(user.id):
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "در حال اتصال فعال هستید. برای مدیریت مخاطبین ابتدا «⛔ پایان اتصال» را بزنید.",
                    keyboard=connected_menu(),
                )
                return
            page = self._parse_contacts_page_command(command)
            await self._show_contacts(user, page=page)
            return

        if user.is_registered and command and command.startswith("contact_"):
            if await self.repository.get_active_target(user.id):
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "در حال اتصال فعال هستید. برای مدیریت مخاطبین ابتدا «⛔ پایان اتصال» را بزنید.",
                    keyboard=connected_menu(),
                )
                return
            contact_id = self._parse_contact_command(command)
            if contact_id is None:
                await self._send_main_menu(user, "لینک مخاطب نامعتبر است.")
                return
            await self._open_contact_from_command(user, contact_id)
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
                    "📱 لطفاً شماره موبایل خودتان را وارد کنید.\n"
                    "برای اتصال پایدار، حتماً شماره متعلق به خودتان را ارسال کنید.",
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

            if text == BTN_ENTER_PHONE:
                await self.repository.set_user_state(user.id, FlowState.REG_WAIT_PHONE_MANUAL, {})
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "شماره موبایل خودتان را به فرمت 09xxxxxxxxx ارسال کنید.",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return

            phone = incoming.phone_number or normalize_phone(text)
            if not phone:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "شماره معتبر نیست. شماره موبایل خودتان را دقیق وارد کنید (09xxxxxxxxx).",
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
                f"✅ ثبت‌نام کامل شد.\n🆔 فندق‌آیدی شما: {fresh.bridge_id}\nاز منو ادامه دهید.",
            )
            return

        if state and state.state == FlowState.REG_WAIT_PHONE_MANUAL:
            if text == BTN_BACK:
                await self.repository.set_user_state(user.id, FlowState.REG_WAIT_PHONE, {})
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "📱 لطفاً شماره موبایل خودتان را وارد کنید.",
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
                f"✅ ثبت‌نام کامل شد.\n🆔 فندق‌آیدی شما: {fresh.bridge_id}\nاز منو ادامه دهید.",
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

            await self.repository.set_user_state(
                user.id,
                FlowState.CONNECT_WAIT_IDENTIFIER,
                {"target_platform": target_platform.value if target_platform else ""},
            )
            await self._send_text(
                user.platform,
                user.chat_id,
                "فندق‌آیدی/شماره/@username مقصد را بفرستید.",
                keyboard=reply_keyboard([[BTN_BACK], [BTN_REQUEST_ADMIN]]),
            )
            return True

        if state.state == FlowState.CONNECT_WAIT_IDENTIFIER:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            if text == BTN_REQUEST_ADMIN:
                target_platform = _platform_from_data(state.data.get("target_platform"))
                if not target_platform:
                    target_platform = _default_target_platform(user.platform)
                await self.repository.set_user_state(
                    user.id,
                    FlowState.REQUEST_WAIT_PHONE,
                    {"target_platform": target_platform.value},
                )
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    self._admin_request_intro_text(),
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return True

            if incoming.content_type != ContentType.TEXT:
                await self._send_text(user.platform, user.chat_id, "لطفاً شناسه متنی ارسال کنید.")
                return True

            identifier = text
            target_platform = _platform_from_data(state.data.get("target_platform"))
            users = await self.repository.find_registered_users_by_identifier(identifier, target_platform)

            if not users:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "❗️مخاطبی با این مشخصات پیدا نشد.\nبرای کمک در اطلاع‌رسانی، دکمه «🆘 درخواست اطلاع‌رسانی» را بزنید.",
                    keyboard=reply_keyboard([[BTN_REQUEST_ADMIN], [BTN_BACK]]),
                )
                return True

            if len(users) > 1:
                candidates = "\n".join([f"- {u.bridge_id} | {u.platform.value} | @{u.username or '-'}" for u in users[:8]])
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    f"چند کاربر پیدا شد. لطفاً دقیق‌تر وارد کنید:\n{candidates}",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return True

            target = users[0]
            if target.id == user.id:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "نمی‌توانید به خودتان وصل شوید.",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return True

            await self.repository.set_active_session(user.id, target.id)
            await self.repository.clear_user_state(user.id)
            await self._send_text(
                user.platform,
                user.chat_id,
                f"🔌 اتصال فعال شد: {target.display_name or '-'} ({target.bridge_id})\nحالا متن/عکس/ویس بفرستید.",
                keyboard=connected_menu(),
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
                "فندق‌آیدی/شماره/@username مخاطب را بفرستید.",
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
                FlowState.REQUEST_WAIT_PHONE,
                {"target_platform": target_platform.value if target_platform else _default_target_platform(user.platform).value},
            )
            await self._send_text(
                user.platform,
                user.chat_id,
                self._admin_request_intro_text(),
                keyboard=reply_keyboard([[BTN_BACK]]),
            )
            return True

        if state.state == FlowState.REQUEST_WAIT_PHONE:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            phone = normalize_phone(incoming.phone_number or text)
            if not phone:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "شماره مقصد معتبر نیست. فرمت صحیح: 09xxxxxxxxx",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return True

            target_platform = _platform_from_data(state.data.get("target_platform")) or _default_target_platform(user.platform)
            matches = await self.repository.find_registered_users_by_identifier(phone, target_platform)
            if matches:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(
                    user,
                    "✅ این شماره در ربات ثبت‌نام کرده است. از گزینه «🔗 اتصال به مخاطب» استفاده کنید.",
                )
                return True

            await self.repository.set_user_state(
                user.id,
                FlowState.REQUEST_WAIT_NOTE,
                {
                    "target_platform": target_platform.value,
                    "target_phone": phone,
                },
            )
            await self._send_text(
                user.platform,
                user.chat_id,
                "حالا یک توضیح کمک‌کننده درباره صاحب این شماره بنویسید (یا بدون توضیح).",
                keyboard=note_menu(),
            )
            return True

        if state.state == FlowState.REQUEST_WAIT_NOTE:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            target_platform = _platform_from_data(state.data.get("target_platform")) or _default_target_platform(user.platform)
            target_phone = str(state.data.get("target_phone") or "").strip()
            if not target_phone:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "درخواست نامعتبر بود. دوباره تلاش کنید.")
                return True

            note = None
            if text and text != BTN_SKIP_NOTE:
                note = text

            request_id = await self.repository.create_admin_request(
                requester_user_id=user.id,
                target_platform=target_platform,
                target_identifier=target_phone,
                note=note,
            )
            delivered = await self._notify_admins(request_id, user, target_platform, target_phone, note)
            await self.repository.clear_user_state(user.id)

            base = self._admin_request_done_text()
            if delivered > 0:
                await self._send_main_menu(user, f"✅ درخواست شما ثبت و برای ادمین ارسال شد.\n\n{base}")
            else:
                await self._send_main_menu(
                    user,
                    f"⚠️ درخواست ثبت شد ولی ارسال به ادمین ناموفق بود.\n\n{base}",
                )
            return True

        return False

    async def _handle_registered_menu_or_relay(self, user: User, incoming: IncomingMessage) -> None:
        text = (incoming.text or "").strip()
        active_target = await self.repository.get_active_target(user.id)

        if active_target:
            if text == BTN_END_SESSION:
                await self.repository.clear_active_session(user.id)
                await self._send_main_menu(user, "⛔ اتصال فعال قطع شد.")
                return

            if incoming.content_type in {ContentType.TEXT, ContentType.PHOTO, ContentType.VOICE}:
                if incoming.content_type == ContentType.TEXT and text in MENU_TEXTS:
                    await self._send_text(
                        user.platform,
                        user.chat_id,
                        "در حال اتصال فعال هستید. برای تغییر مخاطب ابتدا «⛔ پایان اتصال» را بزنید.",
                        keyboard=connected_menu(),
                    )
                    return
                await self._relay_user_message(user, incoming)
                return

            await self._send_text(
                user.platform,
                user.chat_id,
                "در اتصال فعال فقط متن/عکس/ویس ارسال کنید یا اتصال را پایان دهید.",
                keyboard=connected_menu(),
            )
            return

        if text == BTN_HELP:
            await self._send_help(user)
            return

        if text == BTN_MY_ID:
            keyboard = connected_menu() if active_target else main_menu()
            await self._send_text(user.platform, user.chat_id, f"🆔 فندق‌آیدی شما: {user.bridge_id}", keyboard=keyboard)
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
            await self._show_contacts(user, page=0)
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
            await self._send_main_menu(user, "ابتدا از گزینه «🔗 اتصال به مخاطب» یک اتصال فعال بسازید.")
            return

        await self._send_main_menu(user, "نوع پیام پشتیبانی نمی‌شود.")

    async def _handle_callback(self, user: User, incoming: IncomingMessage) -> None:
        await self._safe_answer_callback(incoming)
        data = (incoming.callback_data or "").strip()
        if not data:
            return

        if data.startswith("reg:terms:"):
            action = data.removeprefix("reg:terms:")
            if user.is_registered:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "ثبت‌نام شما قبلاً تکمیل شده است.")
                return
            if action == "accept":
                await self.repository.mark_terms_accepted(user.id)
                await self.repository.set_user_state(user.id, FlowState.REG_WAIT_PHONE, {})
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "📱 لطفاً شماره موبایل خودتان را وارد کنید.\n"
                    "برای اتصال پایدار، حتماً شماره متعلق به خودتان را ارسال کنید.",
                    keyboard=phone_menu(user.platform),
                )
                return

            if action in {"decline", "back"}:
                await self.repository.clear_user_state(user.id)
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "ثبت‌نام لغو شد. هر زمان آماده بودید دوباره اقدام کنید.",
                    keyboard=pre_login_menu(),
                )
                return

        if data == "ct:menu":
            await self._send_main_menu(user, "بازگشت به منوی اصلی.")
            return

        if data.startswith("ct:page:"):
            try:
                page = int(data.split(":", 2)[2])
            except Exception:
                page = 0
            await self._show_contacts(user, page=page)
            return

        if data.startswith("ct:open:"):
            contact_id, page = _parse_contact_callback(data)
            if contact_id is None:
                return
            contact = await self.repository.get_contact(user.id, contact_id)
            if not contact:
                await self._show_contacts(user, "مخاطب یافت نشد.", page=page)
                return
            target = await self.repository.get_user_by_id(contact.target_user_id)
            if not target:
                await self._show_contacts(user, "کاربر مقصد یافت نشد.", page=page)
                return
            await self._send_profile(user, contact, target, page=page)
            return

        if data.startswith("ct:connect:"):
            contact_id, page = _parse_contact_callback(data)
            if contact_id is None:
                return
            contact = await self.repository.get_contact(user.id, contact_id)
            if not contact:
                await self._show_contacts(user, "مخاطب یافت نشد.", page=page)
                return
            target = await self.repository.get_user_by_id(contact.target_user_id)
            if not target:
                await self._show_contacts(user, "کاربر مقصد یافت نشد.", page=page)
                return
            await self.repository.set_active_session(user.id, target.id)
            await self._send_text(
                user.platform,
                user.chat_id,
                f"🔌 به «{contact.alias}» متصل شدید. حالا پیام بفرستید.",
                keyboard=connected_menu(),
            )
            return

        if data.startswith("ct:block:"):
            contact_id, page = _parse_contact_callback(data)
            if contact_id is None:
                return
            contact = await self.repository.get_contact(user.id, contact_id)
            if not contact:
                await self._show_contacts(user, "مخاطب یافت نشد.", page=page)
                return
            target = await self.repository.get_user_by_id(contact.target_user_id)
            if not target:
                await self._show_contacts(user, "کاربر مقصد یافت نشد.", page=page)
                return
            await self.repository.add_block(user.id, target.id)
            await self._send_profile(user, contact, target, page=page, preface="🚫 مخاطب بلاک شد.")
            return

        if data.startswith("ct:unblock:"):
            contact_id, page = _parse_contact_callback(data)
            if contact_id is None:
                return
            contact = await self.repository.get_contact(user.id, contact_id)
            if not contact:
                await self._show_contacts(user, "مخاطب یافت نشد.", page=page)
                return
            target = await self.repository.get_user_by_id(contact.target_user_id)
            if not target:
                await self._show_contacts(user, "کاربر مقصد یافت نشد.", page=page)
                return
            await self.repository.remove_block(user.id, target.id)
            await self._send_profile(user, contact, target, page=page, preface="✅ مخاطب از بلاک خارج شد.")
            return

        if data.startswith("ct:delete:"):
            contact_id, page = _parse_contact_callback(data)
            if contact_id is None:
                return
            await self.repository.delete_contact(user.id, contact_id)
            await self._show_contacts(user, "🗑 مخاطب حذف شد.", page=page)
            return

        if data.startswith("in:connect:") or data.startswith("in:reply:"):
            parts = data.split(":")
            if len(parts) != 3:
                return
            try:
                source_user_id = int(parts[2])
            except ValueError:
                return
            source_user = await self.repository.get_user_by_id(source_user_id)
            if not source_user or not source_user.is_registered:
                await self._send_text(user.platform, user.chat_id, "فرستنده پیام دیگر در دسترس نیست.")
                return

            await self.repository.set_active_session(user.id, source_user.id)
            if parts[1] == "connect":
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    f"🔌 اتصال با «{source_user.display_name or '-'}» فعال شد. حالا پیام بفرستید.",
                    keyboard=connected_menu(),
                )
            else:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    f"💬 آماده پاسخ به «{source_user.display_name or '-'}» هستید.",
                    keyboard=connected_menu(),
                )
            return

    async def _safe_answer_callback(self, incoming: IncomingMessage) -> None:
        if not incoming.callback_query_id:
            return
        try:
            await self._client(incoming.platform).answer_callback_query(incoming.callback_query_id)
        except Exception:
            pass

    async def _show_contacts(self, user: User, preface: str | None = None, page: int = 0) -> None:
        contacts = await self.repository.list_contacts(user.id)
        if not contacts:
            msg = "👥 مخاطبی ذخیره نشده است. از گزینه «➕ افزودن مخاطب» استفاده کنید."
            if preface:
                msg = f"{preface}\n\n{msg}"
            await self._send_text(user.platform, user.chat_id, msg, keyboard=main_menu())
            return

        page_size = 5
        max_page = max((len(contacts) - 1) // page_size, 0)
        page = max(0, min(page, max_page))

        start = page * page_size
        end = start + page_size
        page_contacts = contacts[start:end]

        lines = ["👥 لیست مخاطبین فندقی (برای مشاهده پروفایل، روی لینک هر مخاطب بزنید):"]
        for idx, c in enumerate(page_contacts, start=1):
            target = await self.repository.get_user_by_id(c.target_user_id)
            if not target:
                lines.append(f"{idx}. {c.alias} | کاربر حذف‌شده")
                continue
            lines.append(
                f"{idx}. {c.alias} | {target.bridge_id} | {target.platform.value}\n"
                f"   🔗 /contact_{c.id}"
            )

        lines.append("")
        lines.append(f"صفحه {page + 1} از {max_page + 1}")
        if page > 0:
            lines.append(f"◀️ صفحه قبل: /contacts_{page}")
        if page < max_page:
            lines.append(f"▶️ صفحه بعد: /contacts_{page + 2}")

        msg = "\n".join(lines)
        if preface:
            msg = f"{preface}\n\n{msg}"

        await self._send_text(
            user.platform,
            user.chat_id,
            msg,
            keyboard=main_menu(),
        )

    async def _send_profile(
        self,
        requester: User,
        contact: ContactEntry,
        target: User,
        page: int = 0,
        preface: str | None = None,
    ) -> None:
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

        if photo_file_id:
            try:
                await self._deliver(
                    source_platform=target.platform,
                    dest_user=requester,
                    content_type=ContentType.PHOTO,
                    text=None,
                    source_file_id=photo_file_id,
                    caption="📷 تصویر پروفایل",
                )
            except Exception:
                pass

        blocked = await self.repository.is_blocked(requester.id, target.id)
        profile_text = (
            f"👤 پروفایل مخاطب: {contact.alias}\n"
            f"نام: {target.display_name or '-'}\n"
            f"نام کاربری: @{target.username or '-'}\n"
            f"پلتفرم: {target.platform.value}\n"
            f"فندق‌آیدی: {target.bridge_id}\n"
            f"شماره: {target.phone_number or '-'}\n"
            f"بیو: {bio}"
        )
        if preface:
            profile_text = f"{preface}\n\n{profile_text}"

        await self._send_text(
            requester.platform,
            requester.chat_id,
            profile_text,
            keyboard=contact_profile_actions(contact.id, page=page, blocked=blocked),
        )

    async def _notify_admins(
        self,
        request_id: int,
        requester: User,
        target_platform: Platform,
        target_phone: str,
        note: str | None,
    ) -> int:
        message = (
            "📨 درخواست اطلاع‌رسانی جدید\n"
            f"شماره درخواست: #{request_id}\n"
            f"درخواست‌دهنده: {requester.display_name or '-'}\n"
            f"فندق‌آیدی: {requester.bridge_id}\n"
            f"پلتفرم درخواست‌دهنده: {requester.platform.value}\n"
            f"پلتفرم مقصد: {target_platform.value}\n"
            f"شماره مقصد: {target_phone}\n"
            f"توضیح کمک‌کننده: {note or '-'}"
        )

        delivered = 0
        for admin_platform, admin_chat_id in self._iter_admin_targets():
            try:
                await self._client(admin_platform).send_message(admin_chat_id, message)
                delivered += 1
            except Exception as exc:
                logger.warning("Admin notify failed for %s:%s => %s", admin_platform.value, admin_chat_id, exc)

        channel_id = self._normalize_telegram_channel_target(self.settings.telegram_admin_channel_id)
        if channel_id:
            try:
                await self.telegram_client.send_message(channel_id, message)
                delivered += 1
            except Exception as exc:
                logger.warning("Admin notify failed for telegram channel %s => %s", channel_id, exc)
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
            targets.append((Platform.TELEGRAM, item))
            targets.append((Platform.BALE, item))

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
        keyboard = main_menu()
        if target:
            status = f"🔌 اتصال فعال: {target.display_name or '-'} ({target.bridge_id})"
            keyboard = connected_menu()

        text = status if not preface else f"{preface}\n\n{status}"
        await self._send_text(user.platform, user.chat_id, text, keyboard=keyboard)

    async def _send_help(self, user: User) -> None:
        text = (
            "📘 راهنما\n\n"
            "1) ابتدا ثبت‌نام را انجام دهید (قبول مقررات + شماره موبایل خودتان).\n"
            "2) برای اتصال سریع: «🔗 اتصال به مخاطب».\n"
            "3) شناسه مقصد می‌تواند فندق‌آیدی یا شماره یا @username باشد.\n"
            "4) برای ذخیره مخاطب: «➕ افزودن مخاطب».\n"
            "5) برای مدیریت مخاطبین: «👥 لیست مخاطبین» (صفحه‌بندی + لینک /contact_ID).\n"
            "6) اگر مخاطب عضو نیست: «🆘 درخواست اطلاع‌رسانی» و شماره مقصد را بدهید.\n"
            "7) وقتی اتصال فعال است، فقط پیام‌ها را ارسال کنید یا اتصال را پایان دهید."
        )
        keyboard = connected_menu() if await self.repository.get_active_target(user.id) else main_menu()
        if not user.is_registered:
            keyboard = pre_login_menu()
        await self._send_text(user.platform, user.chat_id, text, keyboard=keyboard)

    @staticmethod
    def _admin_request_intro_text() -> str:
        return (
            "🆘 فرآیند درخواست اطلاع‌رسانی\n\n"
            "در این بخش، شماره تلفن مخاطب موردنظر شما را دریافت می‌کنیم و سپس یک توضیح کوتاه و کمک‌کننده از شما می‌گیریم.\n"
            "تیم پشتیبانی در صورت امکان تلاش می‌کند به آن فرد اطلاع دهد که در ربات فندقِ پیام‌رسان مقصد ثبت‌نام کند تا امکان ارتباط شما برقرار شود.\n"
            "شماره و توضیح شما فقط برای همین فرآیند استفاده می‌شود.\n\n"
            "لطفاً شماره مقصد را با فرمت 09xxxxxxxxx ارسال کنید."
        )

    @staticmethod
    def _admin_request_done_text() -> str:
        return (
            "شماره تلفن و توضیح شما دریافت شد و برای تیم رسیدگی ارسال گردید.\n"
            "در صورت امکان، به مخاطب اطلاع داده می‌شود که در ربات فندق ثبت‌نام کند تا ارتباط شما برقرار شود.\n"
            "از اعتماد شما سپاسگزاریم."
        )

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
                keyboard=connected_menu(),
            )
            return

        if self.settings.rate_limit_enabled:
            kind = "text" if incoming.content_type == ContentType.TEXT else "media"
            if not self.rate_limiter.allow(source_user.id, kind):
                await self._send_text(
                    source_user.platform,
                    source_user.chat_id,
                    "⏳ محدودیت نرخ پیام فعال است. کمی بعد دوباره تلاش کنید.",
                    keyboard=connected_menu(),
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
                reply_source_user_id=source_user.id,
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
                keyboard=connected_menu(),
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
        reply_source_user_id: int | None = None,
    ) -> None:
        dest_client = self._client(dest_user.platform)
        reply_markup = await self._incoming_reply_markup(dest_user.id, reply_source_user_id)

        if content_type == ContentType.TEXT:
            if not text:
                raise ValueError("Empty text payload")
            await dest_client.send_message(dest_user.chat_id, text, reply_markup=reply_markup)
            return

        if not source_file_id:
            raise ValueError("Missing source file id")

        if source_platform == dest_user.platform:
            if content_type == ContentType.PHOTO:
                await dest_client.send_photo(
                    dest_user.chat_id,
                    photo_file_id=source_file_id,
                    caption=caption,
                    reply_markup=reply_markup,
                )
            elif content_type == ContentType.VOICE:
                await dest_client.send_voice(
                    dest_user.chat_id,
                    voice_file_id=source_file_id,
                    caption=caption,
                    reply_markup=reply_markup,
                )
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
                await dest_client.send_photo(
                    dest_user.chat_id,
                    photo_path=temp_path,
                    caption=caption,
                    reply_markup=reply_markup,
                )
            elif content_type == ContentType.VOICE:
                await dest_client.send_voice(
                    dest_user.chat_id,
                    voice_path=temp_path,
                    caption=caption,
                    reply_markup=reply_markup,
                )
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
                reply_source_user_id=source_user.id,
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

    async def _incoming_reply_markup(self, dest_user_id: int, reply_source_user_id: int | None) -> dict | None:
        if reply_source_user_id is None:
            return None
        active = await self.repository.get_active_target(dest_user_id)
        connected = bool(active and active.id == reply_source_user_id)
        return incoming_reply_actions(reply_source_user_id, connected=connected)

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
    def _parse_contacts_page_command(command: str) -> int:
        # /contacts_1 => page 0
        _, _, raw = command.partition("_")
        try:
            page_num = int(raw)
        except ValueError:
            page_num = 1
        return max(0, page_num - 1)

    @staticmethod
    def _parse_contact_command(command: str) -> int | None:
        # /contact_12 => contact id 12
        _, _, raw = command.partition("_")
        try:
            value = int(raw)
        except ValueError:
            return None
        return value if value > 0 else None

    async def _open_contact_from_command(self, user: User, contact_id: int) -> None:
        contact = await self.repository.get_contact(user.id, contact_id)
        if not contact:
            await self._show_contacts(user, preface="مخاطب یافت نشد.", page=0)
            return
        target = await self.repository.get_user_by_id(contact.target_user_id)
        if not target:
            await self._show_contacts(user, preface="کاربر مقصد یافت نشد.", page=0)
            return
        await self._send_profile(user, contact, target, page=0)

    def _is_duplicate_interaction(self, user: User, incoming: IncomingMessage) -> bool:
        loop = asyncio.get_running_loop()
        now = loop.time()

        if len(self._recent_interactions) > 4000:
            cutoff = now - 8.0
            self._recent_interactions = {k: t for k, t in self._recent_interactions.items() if t >= cutoff}

        key: tuple[str, str] | None = None
        ttl = 1.2

        if incoming.is_callback:
            if incoming.callback_query_id:
                key = ("cb", f"{incoming.platform.value}:{incoming.callback_query_id}")
            elif incoming.callback_data:
                key = ("cb", f"{incoming.platform.value}:{user.id}:{incoming.callback_data}")
            ttl = 1.8
        elif incoming.platform == Platform.BALE and incoming.content_type == ContentType.TEXT:
            text = (incoming.text or "").strip()
            if text in MENU_TEXTS:
                key = ("btn", f"{user.id}:{text}")
                ttl = 1.5

        if key is None:
            return False

        previous = self._recent_interactions.get(key)
        self._recent_interactions[key] = now
        if previous is None:
            return False
        return (now - previous) <= ttl

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

    @staticmethod
    def _normalize_telegram_channel_target(value: str) -> str:
        target = (value or "").strip()
        if not target:
            return ""
        # Telegram channel ids are usually -100xxxxxxxxxx.
        # Accept raw 100xxxxxxxxxx and normalize automatically.
        if target.isdigit() and target.startswith("100"):
            return f"-{target}"
        if target.startswith("https://t.me/"):
            slug = target.removeprefix("https://t.me/").strip("/")
            if slug and not slug.startswith("+"):
                return f"@{slug}"
        return target


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


def _parse_contact_callback(data: str) -> tuple[int | None, int]:
    # formats: ct:open:<id>:<page>, ct:block:<id>:<page>, ...
    parts = data.split(":")
    if len(parts) < 4:
        return None, 0
    try:
        contact_id = int(parts[2])
    except ValueError:
        return None, 0
    try:
        page = int(parts[3])
    except ValueError:
        page = 0
    return contact_id, max(page, 0)
