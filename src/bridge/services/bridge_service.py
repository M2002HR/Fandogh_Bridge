from __future__ import annotations

import asyncio
import html
import logging
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from bridge.config import Settings
from bridge.crypto_pay import CryptoPayClient, CryptoPayError
from bridge.platforms.client import BotApiClient
from bridge.platforms.parser import parse_update
from bridge.rate_limit import InMemoryRateLimiter
from bridge.repository import Repository
from bridge.sales import SalesCatalog, SalesPackage
from bridge.services.ui import (
    BTN_ACCEPT_TERMS,
    BTN_ADD_CONTACT,
    BTN_BACK,
    BTN_BALANCE,
    BTN_BUY_PACKAGE,
    BTN_CONNECT,
    BTN_CONTACTS,
    BTN_DECLINE_TERMS,
    BTN_END_SESSION,
    BTN_ENTER_PHONE,
    BTN_HELP,
    BTN_MY_ID,
    BTN_PAYMENT_HISTORY,
    BTN_PLATFORM_ANY,
    BTN_PLATFORM_BALE,
    BTN_PLATFORM_TELEGRAM,
    BTN_REGISTER,
    BTN_REQUEST_ADMIN,
    BTN_SHARE_PHONE,
    BTN_SKIP_NOTE,
    BTN_SUPPORT,
    apply_telegram_button_styles,
    admin_payment_actions,
    connected_menu,
    contact_profile_actions,
    incoming_reply_actions,
    main_menu,
    note_menu,
    package_actions_keyboard,
    package_beneficiary_candidates_keyboard,
    package_recipient_keyboard,
    packages_keyboard,
    payment_history_detail_keyboard,
    payment_history_keyboard,
    phone_menu,
    platform_menu,
    pre_login_menu,
    reply_keyboard,
    terms_menu,
)
from bridge.types import (
    ContactEntry,
    ContentType,
    CreditWallet,
    DeliveryStatus,
    IncomingMessage,
    PaymentOrder,
    Platform,
    PlatformApiError,
    User,
    UserState,
)
from bridge.utils import extract_contact_identifiers, normalize_phone, parse_iso, utc_iso, utc_now

logger = logging.getLogger(__name__)

TELEGRAM_STARS_PER_USDT = 100.0 / 1.5  # user rule: every 100 stars = 1.5 USDT


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
    REQUEST_WAIT_IDENTIFIER = "REQUEST_WAIT_IDENTIFIER"
    REQUEST_WAIT_NOTE = "REQUEST_WAIT_NOTE"

    PAYMENT_WAIT_BENEFICIARY_IDENTIFIER = "PAYMENT_WAIT_BENEFICIARY_IDENTIFIER"
    PAYMENT_MANUAL_WAIT_RECEIPT = "PAYMENT_MANUAL_WAIT_RECEIPT"


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
    BTN_BALANCE,
    BTN_BUY_PACKAGE,
    BTN_PAYMENT_HISTORY,
    BTN_MY_ID,
    BTN_END_SESSION,
    BTN_REQUEST_ADMIN,
    BTN_SUPPORT,
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
        sales_catalog: SalesCatalog,
        repository: Repository,
        telegram_client: BotApiClient,
        bale_client: BotApiClient,
        rate_limiter: InMemoryRateLimiter,
        crypto_pay_client: CryptoPayClient | None = None,
    ) -> None:
        self.settings = settings
        self.sales_catalog = sales_catalog
        self.repository = repository
        self.telegram_client = telegram_client
        self.bale_client = bale_client
        self.rate_limiter = rate_limiter
        self.crypto_pay_client = crypto_pay_client
        self.metrics = Metrics()
        self._stop_event = asyncio.Event()
        self._recent_interactions: dict[tuple[str, str], float] = {}
        self._usdt_rate_toman = max(int(self.settings.usdt_fixed_toman_rate), 0)
        self._usdt_rate_lock = asyncio.Lock()
        self._ton_per_usdt: float | None = None
        self._ton_rate_last_fetch: datetime | None = None
        self._ton_rate_lock = asyncio.Lock()
        try:
            self._rate_tz = ZoneInfo(self.sales_catalog.usdt_rate_timezone)
        except Exception:
            self._rate_tz = ZoneInfo("Asia/Tehran")

    async def run(self) -> None:
        await self._ensure_daily_usdt_rate(force=False)
        await self._configure_telegram_presentation()
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
        if self.crypto_pay_client:
            tasks.append(asyncio.create_task(self._telegram_ton_pay_worker()))
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
            if self.crypto_pay_client:
                await self.crypto_pay_client.aclose()

    async def _configure_telegram_presentation(self) -> None:
        if self.settings.telegram_set_commands_on_start:
            try:
                await self.telegram_client.set_my_commands(
                    [
                        {"command": "start", "description": "شروع و نمایش منوی اصلی"},
                        {"command": "help", "description": "راهنمای استفاده"},
                        {"command": "id", "description": "نمایش فندق‌آیدی من"},
                        {"command": "contacts", "description": "نمایش مخاطبین ذخیره‌شده"},
                        {"command": "balance", "description": "مشاهده اعتبار باقی‌مانده"},
                        {"command": "buy", "description": "نمایش بسته‌ها و خرید"},
                        {"command": "history", "description": "تاریخچه پرداخت و خرید"},
                        {"command": "end", "description": "پایان اتصال فعال"},
                    ]
                )
            except Exception as exc:
                logger.warning("Telegram setMyCommands failed: %s", exc)
        if self.settings.telegram_set_menu_button_on_start:
            try:
                await self.telegram_client.set_chat_menu_button(menu_button={"type": "commands"})
            except Exception as exc:
                logger.warning("Telegram setChatMenuButton failed: %s", exc)

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
            except (httpx.ReadTimeout, httpx.ConnectTimeout):
                # Normal on unstable networks / long-poll edge timing.
                await asyncio.sleep(0.3)
                continue
            except httpx.ConnectError as exc:
                logger.warning("Poll connection error on %s: %s", platform.value, exc)
                await asyncio.sleep(2)
                continue
            except Exception as exc:
                logger.exception("Unexpected poll error on %s: %s", platform.value, exc)
                await asyncio.sleep(2)

    async def _process_incoming(self, incoming: IncomingMessage) -> None:
        if incoming.is_callback and incoming.chat_type != "private":
            await self._handle_admin_callback(incoming)
            return

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

        if incoming.content_type == ContentType.PRE_CHECKOUT:
            await self._handle_pre_checkout(user, incoming)
            return

        if incoming.content_type == ContentType.SUCCESSFUL_PAYMENT:
            await self._handle_successful_payment(user, incoming)
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

        if user.is_registered and command and command.startswith("connect_user_"):
            if await self.repository.get_active_target(user.id):
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "در حال اتصال فعال هستید. برای تغییر مخاطب ابتدا «⛔ پایان اتصال» را بزنید.",
                    keyboard=connected_menu(),
                )
                return
            target_user_id = self._parse_connect_user_command(command)
            if target_user_id is None:
                await self._send_main_menu(user, "لینک اتصال نامعتبر است.")
                return
            await self._connect_to_user_from_command(user, target_user_id)
            return

        if user.is_registered and command == "balance":
            await self._send_balance(user)
            return

        if user.is_registered and command in {"buy", "packages"}:
            await self._show_packages(user)
            return

        if user.is_registered and command in {"history", "payments"}:
            await self._show_payment_history(user, page=0)
            return

        if user.is_registered and command == "support":
            await self._send_text(user.platform, user.chat_id, self._support_text(), keyboard=main_menu())
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
            await self._grant_starter_credits_if_needed(fresh)
            await self._notify_requesters_target_joined(fresh)
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
            await self._grant_starter_credits_if_needed(fresh)
            await self._notify_requesters_target_joined(fresh)
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
                    FlowState.REQUEST_WAIT_IDENTIFIER,
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
                candidates = self._format_connect_candidates(users[:8])
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    f"برای اتصال، یکی از گزینه‌های زیر را انتخاب کنید:\n{candidates}",
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

        if state.state == FlowState.PAYMENT_WAIT_BENEFICIARY_IDENTIFIER:
            package_id = str(state.data.get("package_id") or "").strip()
            method_id = str(state.data.get("method_id") or "").strip()
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._show_package_detail(user, package_id)
                return True

            if incoming.content_type != ContentType.TEXT:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "لطفاً شناسه متنی بفرستید: فندق‌آیدی یا شماره یا @username",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return True

            candidates = await self.repository.find_registered_users_by_identifier(text, None)
            candidates = [item for item in candidates if item.is_registered and item.id != user.id]
            if not candidates:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "کاربر مقصد پیدا نشد. شناسه دقیق‌تری بفرستید.",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return True

            if len(candidates) > 1:
                rows = []
                for item in candidates[:8]:
                    title = f"{item.display_name or '-'} | {item.bridge_id} | {item.platform.value}"
                    rows.append((item.id, title))
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "برای خرید بسته، گیرنده را انتخاب کنید:",
                    keyboard=package_beneficiary_candidates_keyboard(package_id, method_id, rows),
                )
                return True

            await self.repository.clear_user_state(user.id)
            await self._start_package_payment(user, package_id, method_id, beneficiary_user=candidates[0])
            return True

        if state.state == FlowState.PAYMENT_MANUAL_WAIT_RECEIPT:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "فرآیند پرداخت لغو شد.")
                return True

            handled = await self._handle_manual_payment_receipt(user, incoming, state)
            return handled

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
                {"target_platform": target_platform.value if target_platform else _default_target_platform(user.platform).value},
            )
            await self._send_text(
                user.platform,
                user.chat_id,
                self._admin_request_intro_text(),
                keyboard=reply_keyboard([[BTN_BACK]]),
            )
            return True

        if state.state == FlowState.REQUEST_WAIT_IDENTIFIER:
            if text == BTN_BACK:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "بازگشت به منوی اصلی.")
                return True

            phone, username = extract_contact_identifiers(text, incoming.phone_number)
            if not phone and not username:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "حداقل یکی از این‌ها را بفرستید: شماره موبایل یا آیدی تلگرام.\nنمونه: `0912xxxxxxx` یا `@username` یا هر دو در یک پیام.",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return True

            target_platform = _platform_from_data(state.data.get("target_platform")) or _default_target_platform(user.platform)
            matches = await self._find_request_matches(phone, username, target_platform)
            if matches:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(
                    user,
                    "✅ این مشخصات در ربات ثبت‌نام شده است. از گزینه «🔗 اتصال به مخاطب» استفاده کنید.",
                )
                return True

            await self.repository.set_user_state(
                user.id,
                FlowState.REQUEST_WAIT_NOTE,
                {
                    "target_platform": target_platform.value,
                    "target_phone": phone,
                    "target_username": username,
                },
            )
            await self._send_text(
                user.platform,
                user.chat_id,
                "حالا یک توضیح کمک‌کننده درباره صاحب این شماره/آیدی بنویسید (یا بدون توضیح).",
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
            target_username = str(state.data.get("target_username") or "").strip()
            if not target_phone and not target_username:
                await self.repository.clear_user_state(user.id)
                await self._send_main_menu(user, "درخواست نامعتبر بود. دوباره تلاش کنید.")
                return True

            note = None
            if text and text != BTN_SKIP_NOTE:
                note = text

            request_id = await self.repository.create_admin_request(
                requester_user_id=user.id,
                target_platform=target_platform,
                target_identifier=self._format_admin_request_target(target_phone, target_username),
                target_phone=target_phone or None,
                target_username=target_username or None,
                note=note,
            )
            delivered = await self._notify_admins(request_id, user, target_platform, target_phone, target_username, note)
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
                self._unsupported_message_reason(incoming),
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

        if text == BTN_BALANCE:
            await self._send_balance(user)
            return

        if text == BTN_BUY_PACKAGE:
            await self._show_packages(user)
            return

        if text == BTN_PAYMENT_HISTORY:
            await self._show_payment_history(user, page=0)
            return

        if text == BTN_SUPPORT:
            await self._send_text(user.platform, user.chat_id, self._support_text(), keyboard=main_menu())
            return

        if incoming.content_type in {ContentType.TEXT, ContentType.PHOTO, ContentType.VOICE}:
            if incoming.content_type == ContentType.TEXT and text in MENU_TEXTS:
                await self._send_main_menu(user, "یک گزینه معتبر انتخاب کنید.")
                return
            await self._send_main_menu(user, "ابتدا از گزینه «🔗 اتصال به مخاطب» یک اتصال فعال بسازید.")
            return

        await self._send_main_menu(user, self._unsupported_message_reason(incoming))

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

        if data == "pkg:menu":
            await self._send_main_menu(user, "بازگشت به منوی اصلی.")
            return

        if data == "pkg:list":
            await self._show_packages(user)
            return

        if data.startswith("pkg:open:"):
            package_id = data.removeprefix("pkg:open:")
            await self._show_package_detail(user, package_id)
            return

        if data.startswith("pkg:pay:"):
            parts = data.split(":", 3)
            if len(parts) != 4:
                await self._send_main_menu(user, "درخواست پرداخت نامعتبر است.")
                return
            _, _, method_id, package_id = parts
            await self._ask_package_recipient(user, package_id, method_id)
            return

        if data.startswith("pkg:who:"):
            parts = data.split(":", 4)
            if len(parts) != 5:
                await self._send_main_menu(user, "درخواست پرداخت نامعتبر است.")
                return
            _, _, who, method_id, package_id = parts
            if who == "self":
                await self.repository.clear_user_state(user.id)
                await self._start_package_payment(user, package_id, method_id, beneficiary_user=user)
                return
            if who == "other":
                await self.repository.set_user_state(
                    user.id,
                    FlowState.PAYMENT_WAIT_BENEFICIARY_IDENTIFIER,
                    {"package_id": package_id, "method_id": method_id},
                )
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    "شناسه گیرنده را بفرستید: فندق‌آیدی یا شماره یا @username",
                    keyboard=reply_keyboard([[BTN_BACK]]),
                )
                return
            await self._send_main_menu(user, "گزینه گیرنده نامعتبر است.")
            return

        if data.startswith("pkg:benef:"):
            parts = data.split(":", 4)
            if len(parts) != 5:
                await self._send_main_menu(user, "درخواست انتخاب گیرنده نامعتبر است.")
                return
            _, _, method_id, package_id, target_user_id_raw = parts
            try:
                target_user_id = int(target_user_id_raw)
            except ValueError:
                await self._send_main_menu(user, "شناسه گیرنده نامعتبر است.")
                return
            target = await self.repository.get_user_by_id(target_user_id)
            if not target or not target.is_registered:
                await self._send_main_menu(user, "گیرنده انتخاب‌شده در دسترس نیست.")
                return
            if target.id == user.id:
                await self._start_package_payment(user, package_id, method_id, beneficiary_user=user)
                return
            await self.repository.clear_user_state(user.id)
            await self._start_package_payment(user, package_id, method_id, beneficiary_user=target)
            return

        if data == "payh:menu":
            await self._send_main_menu(user, "بازگشت به منوی اصلی.")
            return

        if data.startswith("payh:page:"):
            try:
                page = int(data.split(":", 2)[2])
            except Exception:
                page = 0
            await self._show_payment_history(user, page=page)
            return

        if data.startswith("payh:open:"):
            parts = data.split(":", 3)
            if len(parts) != 4:
                await self._show_payment_history(user, page=0, preface="ورودی نامعتبر بود.")
                return
            try:
                order_id = int(parts[2])
                page = int(parts[3])
            except ValueError:
                await self._show_payment_history(user, page=0, preface="ورودی نامعتبر بود.")
                return
            await self._show_payment_order_detail(user, order_id=order_id, page=page)
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

        if data.startswith("in:seen:"):
            parts = data.split(":")
            if len(parts) != 4:
                return
            try:
                source_user_id = int(parts[2])
                source_message_id = int(parts[3])
            except ValueError:
                return
            source_user = await self.repository.get_user_by_id(source_user_id)
            if not source_user or not source_user.is_registered:
                return
            claimed = await self.repository.claim_message_read_receipt(
                reader_user_id=user.id,
                source_user_id=source_user_id,
                source_message_id=source_message_id,
            )
            if not claimed:
                return
            try:
                await self._send_text(
                    source_user.platform,
                    source_user.chat_id,
                    "👁️ این پیام مشاهده شد.",
                    reply_to_message_id=source_message_id if source_message_id > 0 else None,
                )
            except Exception as exc:
                logger.warning("Failed to send read receipt to source user %s: %s", source_user_id, exc)
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

        lines = ["👥 لیست مخاطبین فندقی (برای اتصال یا مشاهده پروفایل، فرمان اتصال هر مخاطب را بزنید):"]
        for idx, c in enumerate(page_contacts, start=1):
            target = await self.repository.get_user_by_id(c.target_user_id)
            if not target:
                lines.append(f"{idx}. {c.alias} | کاربر حذف‌شده")
                continue
            lines.append(
                f"{idx}. {c.alias} | {target.bridge_id} | {target.platform.value}\n"
                f"   🔗 مشاهده پروفایل: /contact_{c.id}\n"
                f"   ⚡ اتصال: /connect_user_{target.id}"
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
        target_username: str,
        note: str | None,
    ) -> int:
        target_username_text = f"@{target_username}" if target_username else "-"
        message = (
            "📨 درخواست اطلاع‌رسانی جدید\n"
            f"شماره درخواست: #{request_id}\n"
            f"درخواست‌دهنده: {requester.display_name or '-'}\n"
            f"فندق‌آیدی: {requester.bridge_id}\n"
            f"پلتفرم درخواست‌دهنده: {requester.platform.value}\n"
            f"پلتفرم مقصد: {target_platform.value}\n"
            f"شماره مقصد: {target_phone or '-'}\n"
            f"آیدی تلگرام مقصد: {target_username_text}\n"
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
        keyboard = main_menu()
        text = preface or "منوی اصلی"
        if target:
            keyboard = connected_menu()
            status = f"🔌 اتصال فعال: {target.display_name or '-'} ({target.bridge_id})"
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
            "6) برای مشاهده اعتبار: «💳 اعتبار من».\n"
            "7) برای خرید بسته: «🛒 خرید بسته».\n"
            "8) برای مشاهده وضعیت همه خریدها: «🧾 تاریخچه پرداخت‌ها».\n"
            "9) برای پیگیری خرید یا سوال مالی: «💬 ارتباط با پشتیبانی».\n"
            "10) اگر مخاطب عضو نیست: «🆘 درخواست اطلاع‌رسانی» و شماره یا آیدی تلگرام مقصد را بدهید.\n"
            "11) وقتی اتصال فعال است، فقط متن، عکس یا ویس بفرستید یا اتصال را پایان دهید."
        )
        keyboard = connected_menu() if await self.repository.get_active_target(user.id) else main_menu()
        if not user.is_registered:
            keyboard = pre_login_menu()
        await self._send_text(user.platform, user.chat_id, text, keyboard=keyboard)

    @staticmethod
    def _admin_request_intro_text() -> str:
        return (
            "🆘 فرآیند درخواست اطلاع‌رسانی\n\n"
            "در این بخش، شماره تلفن مخاطب و/یا آیدی تلگرام او را دریافت می‌کنیم و سپس یک توضیح کوتاه و کمک‌کننده از شما می‌گیریم.\n"
            "تیم پشتیبانی در صورت امکان تلاش می‌کند به آن فرد اطلاع دهد که در ربات فندقِ پیام‌رسان مقصد ثبت‌نام کند تا امکان ارتباط شما برقرار شود.\n"
            "اگر مخاطب ثبت‌نام کند، به شما هم خبر می‌دهیم.\n"
            "اطلاعاتی که می‌فرستید فقط برای همین فرآیند استفاده می‌شود.\n\n"
            "لطفاً شماره موبایل یا آیدی تلگرام را بفرستید.\n"
            "نمونه: 09xxxxxxxxx یا @username یا هر دو در یک پیام."
        )

    @staticmethod
    def _admin_request_done_text() -> str:
        return (
            "اطلاعات مخاطب و توضیح شما دریافت شد و برای تیم رسیدگی ارسال گردید.\n"
            "در صورت امکان، به مخاطب اطلاع داده می‌شود که در ربات فندق ثبت‌نام کند تا ارتباط شما برقرار شود.\n"
            "اگر ثبت‌نام انجام شود، برای شما هم پیام اطلاع‌رسانی ارسال می‌کنیم.\n"
            "از اعتماد شما سپاسگزاریم."
        )

    async def _notify_requesters_target_joined(self, user: User) -> None:
        if not user.is_registered:
            return
        matches = await self.repository.find_open_admin_requests(
            target_platform=user.platform,
            target_phone=user.phone_number,
            target_username=user.username,
        )
        if not matches:
            other_platform = Platform.BALE if user.platform == Platform.TELEGRAM else Platform.TELEGRAM
            matches = await self.repository.find_open_admin_requests(
                target_platform=other_platform,
                target_phone=user.phone_number,
                target_username=user.username,
            )
        if not matches:
            return

        username = f"@{user.username}" if user.username else "-"
        text = (
            "✅ مخاطبی که برای او درخواست اطلاع‌رسانی ثبت کرده بودید، در ربات فندق عضو شد.\n\n"
            f"نام: {user.display_name or '-'}\n"
            f"پلتفرم: {user.platform.value}\n"
            f"نام کاربری: {username}\n"
            f"فندق‌آیدی: {user.bridge_id}\n"
            "اکنون می‌توانید از منوی «🔗 اتصال به مخاطب» یا لیست مخاطبین برای ارتباط استفاده کنید."
        )

        for request_id, requester_user_id in matches:
            requester = await self.repository.get_user_by_id(requester_user_id)
            if requester:
                try:
                    await self._send_text(requester.platform, requester.chat_id, text, keyboard=main_menu())
                except Exception as exc:
                    logger.warning("Failed to notify requester %s for matched admin request: %s", requester_user_id, exc)
            await self.repository.mark_admin_request_matched(request_id, user.id)

    async def _grant_starter_credits_if_needed(self, user: User) -> None:
        identity_key = self._wallet_identity(user)
        starter = self.sales_catalog.rules.starter_credits
        if not identity_key:
            return
        if starter.text_units <= 0 and starter.voice_minutes <= 0 and starter.photo_count <= 0:
            return
        if await self.repository.has_credit_entry(identity_key, "STARTER"):
            return
        await self.repository.apply_credit_delta(
            identity_key=identity_key,
            user_id=user.id,
            entry_type="STARTER",
            text_units_delta=starter.text_units,
            voice_minutes_delta=starter.voice_minutes,
            photo_count_delta=starter.photo_count,
            package_id=None,
            payment_order_id=None,
            note="Starter credits on first registration",
        )

    def _wallet_identity(self, user: User) -> str | None:
        if not user.is_registered:
            return None
        return f"{user.platform.value}:{user.platform_user_id}"

    @staticmethod
    def _normalize_fa_digits(value: str) -> str:
        return value.translate(
            str.maketrans(
                "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
                "01234567890123456789",
            )
        )

    @staticmethod
    def _extract_latest_channel_message_text(page_html: str) -> str | None:
        matches = re.findall(
            r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
            page_html,
            flags=re.S,
        )
        if not matches:
            return None
        raw = matches[-1]
        raw = raw.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
        text = re.sub(r"<[^>]+>", " ", raw)
        text = html.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    @classmethod
    def _extract_usdt_rate_from_text(cls, text: str) -> int | None:
        normalized = cls._normalize_fa_digits(text)
        normalized = normalized.replace("٬", ",")
        prioritized_lines: list[str] = []
        fallback_lines: list[str] = []
        for line in re.split(r"[\n\r]+", normalized):
            clean = line.strip()
            if not clean:
                continue
            if any(k in clean.lower() for k in ("تتر", "usdt", "usd", "دلار")):
                prioritized_lines.append(clean)
            else:
                fallback_lines.append(clean)

        def extract_candidates(line: str) -> list[int]:
            values: list[int] = []
            for m in re.finditer(r"(?<!\d)(\d{2,3}(?:[,\s]\d{3})+|\d{5,7}|\d{2,3})(?!\d)", line):
                raw = re.sub(r"[,\s]", "", m.group(1))
                if not raw:
                    continue
                v = int(raw)
                context = line[max(0, m.start() - 12) : m.end() + 12]
                if v < 1000 and any(k in context for k in ("هزار", "k", "K")):
                    v *= 1000
                if 10000 <= v <= 1000000:
                    values.append(v)
            return values

        for bucket in (prioritized_lines, fallback_lines):
            for line in bucket:
                cands = extract_candidates(line)
                if cands:
                    return cands[0]
        return None

    async def _fetch_usdt_rate_from_channel(self) -> tuple[int, str]:
        channel = (getattr(self.sales_catalog, "usdt_rate_channel", "") or "").strip().lstrip("@")
        if not channel:
            raise ValueError("USDT channel is not configured")
        url = f"https://t.me/s/{channel}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                },
            )
        response.raise_for_status()
        text = self._extract_latest_channel_message_text(response.text or "")
        if not text:
            raise ValueError("channel latest post text not found")
        rate = self._extract_usdt_rate_from_text(text)
        if not rate:
            raise ValueError("usdt rate not detected in latest channel post")
        return rate, text

    def _local_now(self) -> datetime:
        return datetime.now(self._rate_tz)

    async def _daily_usdt_rate_worker(self) -> None:
        # Disabled by design: conversion rate is fixed from settings.
        while not self._stop_event.is_set():
            await asyncio.sleep(3600)

    async def _ensure_daily_usdt_rate(self, force: bool) -> None:
        _ = force
        self._usdt_rate_toman = max(int(self.settings.usdt_fixed_toman_rate), 0)

    @staticmethod
    def _extract_ton_rate_price_from_payload(payload: object, symbol: str) -> float:
        symbol_upper = symbol.upper()
        if isinstance(payload, dict):
            row = payload
            if "symbol" in row and str(row.get("symbol", "")).upper() not in {symbol_upper, ""}:
                raise ValueError(f"unexpected symbol in TON rate payload: {row.get('symbol')}")
            price_raw = row.get("price")
            if price_raw is None:
                raise ValueError("TON rate payload missing 'price'")
            return float(price_raw)
        if isinstance(payload, list):
            for row in payload:
                if not isinstance(row, dict):
                    continue
                if str(row.get("symbol", "")).upper() == symbol_upper and row.get("price") is not None:
                    return float(row["price"])
            raise ValueError(f"TON rate payload did not include symbol {symbol_upper}")
        raise ValueError("unexpected TON rate payload format")

    async def _fetch_ton_per_usdt_from_api(self) -> float:
        url = self.settings.ton_rate_api_url.strip()
        symbol = self.settings.ton_rate_api_symbol.strip().upper()
        if not url:
            raise ValueError("TON rate API URL is empty")
        if not symbol:
            raise ValueError("TON rate API symbol is empty")
        timeout = max(3, int(self.settings.ton_rate_api_timeout_sec))
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, params={"symbol": symbol})
        response.raise_for_status()
        payload = response.json()
        price_usdt_per_ton = self._extract_ton_rate_price_from_payload(payload, symbol)
        if price_usdt_per_ton <= 0:
            raise ValueError("TON rate must be positive")
        return 1.0 / price_usdt_per_ton

    def _ton_rate_is_fresh(self) -> bool:
        if self._ton_per_usdt is None or self._ton_rate_last_fetch is None:
            return False
        max_age = timedelta(seconds=max(30, int(self.settings.ton_rate_api_cache_sec)))
        return (utc_now() - self._ton_rate_last_fetch) <= max_age

    async def _ensure_ton_rate(self, force: bool) -> None:
        if not self.settings.ton_rate_api_enabled:
            return
        async with self._ton_rate_lock:
            if not force and self._ton_rate_is_fresh():
                return
            try:
                self._ton_per_usdt = await self._fetch_ton_per_usdt_from_api()
                self._ton_rate_last_fetch = utc_now()
            except Exception as exc:
                if self._ton_per_usdt is not None:
                    logger.warning("TON rate refresh failed; using cached value: %s", exc)
                    return
                logger.warning("TON rate refresh failed: %s", exc)

    async def _telegram_ton_pay_worker(self) -> None:
        while not self._stop_event.is_set():
            if not self.crypto_pay_client or not self.settings.telegram_ton_pay_enabled:
                await asyncio.sleep(5)
                continue
            try:
                orders = await self.repository.fetch_payment_orders_for_method(
                    payment_method="telegram_ton_wallet",
                    statuses=("INVOICE_SENT",),
                    limit=100,
                )
                for order in orders:
                    await self._process_ton_wallet_order(order)
            except Exception as exc:
                logger.warning("telegram_ton_pay worker error: %s", exc)
            await asyncio.sleep(max(5, self.settings.telegram_ton_pay_poll_interval_sec))

    async def _process_ton_wallet_order(self, order: PaymentOrder) -> None:
        if not self.crypto_pay_client:
            return
        invoice_id_raw = str(order.account_id or "").strip()
        if not invoice_id_raw.isdigit():
            return
        invoice_id = int(invoice_id_raw)
        try:
            items = await self.crypto_pay_client.get_invoices([invoice_id])
        except CryptoPayError as exc:
            logger.warning("Crypto Pay getInvoices failed for order=%s: %s", order.id, exc)
            return
        if not items:
            return
        item = items[0]
        status = str(item.get("status") or "").strip().lower()
        if status in {"paid", "completed"}:
            charge = str(item.get("hash") or item.get("invoice_id") or invoice_id)
            approved = await self.repository.mark_external_payment_approved(
                order_id=order.id,
                external_charge_id=charge,
                provider_charge_id="TON",
                approval_note="auto-approved by Crypto Pay status=paid",
            )
            if not approved:
                return
            await self._credit_package_if_needed(approved)
            await self._notify_ton_order_success(approved)
            return
        if status in {"expired", "cancelled", "failed", "invalid"}:
            rejected = await self.repository.mark_external_payment_rejected(
                order_id=order.id,
                approval_note=f"crypto pay status={status}",
            )
            if not rejected:
                return
            await self._notify_wallet_credited(
                rejected.requester_user_id,
                f"❌ پرداخت تون بسته {rejected.package_id} ناموفق/منقضی شد ({status}).",
            )

    async def _notify_ton_order_success(self, order: PaymentOrder) -> None:
        if order.beneficiary_user_id == order.requester_user_id:
            await self._notify_wallet_credited(
                order.requester_user_id,
                f"✅ پرداخت تون بسته {order.package_id} تایید شد و اعتبار شما شارژ شد.",
            )
            return
        requester = await self.repository.get_user_by_id(order.requester_user_id)
        beneficiary = await self.repository.get_user_by_id(order.beneficiary_user_id)
        if beneficiary:
            await self._notify_wallet_credited(
                beneficiary.id,
                f"✅ پرداخت تون برای بسته {order.package_id} انجام شد و اعتبار شما شارژ شد.",
            )
        if requester:
            target_name = beneficiary.display_name if beneficiary else str(order.beneficiary_user_id)
            await self._notify_wallet_credited(
                requester.id,
                f"✅ پرداخت تون موفق بود و بسته {order.package_id} برای «{target_name}» شارژ شد.",
            )

    def _manual_price_toman(self, package: SalesPackage) -> int | None:
        rate = self._usdt_rate_toman
        if rate <= 0:
            return None
        return int(round(package.price_usd * rate))

    def _wallet_price_rial(self, package: SalesPackage) -> int | None:
        toman_value = self._manual_price_toman(package)
        if toman_value is not None and toman_value > 0:
            return int(toman_value * 10)
        return None

    @staticmethod
    def _stars_price(package: SalesPackage) -> int | None:
        if package.price_usd <= 0:
            return None
        stars = math.ceil(package.price_usd * TELEGRAM_STARS_PER_USDT)
        return stars if stars > 0 else None

    @staticmethod
    def _format_toman(value: int | None) -> str:
        if value is None:
            return "-"
        return f"{value:,.0f} تومان"

    @staticmethod
    def _format_usdt(value: float) -> str:
        shown = value
        if value >= 0.02:
            shown = value - 0.01
        whole = int(shown)
        frac = int(round((shown - whole) * 100))
        if frac == 100:
            whole += 1
            frac = 0
        return f"{whole}.{frac:02d} تتر"

    def _price_reference_text(self, package: SalesPackage) -> str:
        toman_value = self._manual_price_toman(package)
        stars_price = self._stars_price(package)
        lines = [f"قیمت مرجع: {self._format_usdt(package.price_usd)}"]
        if toman_value is not None:
            lines.append(f"معادل کارت‌به‌کارت: {self._format_toman(toman_value)}")
        if stars_price is not None:
            lines.append(f"قیمت در تلگرام: {stars_price} ⭐")
        return "\n".join(lines)

    def _support_text(self) -> str:
        contact = self.sales_catalog.support_contact_id or "-"
        return (
            "💬 ارتباط با پشتیبانی\n\n"
            f"شناسه پشتیبانی: {contact}\n"
            f"{self.sales_catalog.support_message}"
        )

    async def _send_balance(self, user: User, preface: str | None = None) -> None:
        identity_key = self._wallet_identity(user)
        if not identity_key:
            await self._send_main_menu(user, "برای مشاهده اعتبار، ثبت‌نام کامل لازم است.")
            return

        wallet = await self.repository.get_wallet(identity_key)
        text_rule = (
            "هر پیام متنی = ۱ واحد"
            if self.sales_catalog.rules.text_is_unlimited_per_message
            else f"هر {self.sales_catalog.rules.text_segment_chars} کاراکتر = ۱ واحد پیام"
        )
        text = (
            "💳 اعتبار باقی‌مانده شما\n\n"
            f"پیام متنی: {wallet.text_units_remaining} واحد\n"
            f"ویس: {wallet.voice_minutes_remaining} دقیقه\n"
            f"عکس: {wallet.photo_count_remaining} عدد\n\n"
            f"{text_rule}\n"
            f"هر {self.sales_catalog.rules.voice_credit_seconds} ثانیه ویس = ۱ واحد\n"
            f"حداکثر حجم هر عکس: {self.sales_catalog.rules.photo_max_file_mb}MB"
        )
        if preface:
            text = f"{preface}\n\n{text}"
        await self._send_text(user.platform, user.chat_id, text, keyboard=main_menu())

    async def _show_payment_history(self, user: User, page: int = 0, preface: str | None = None) -> None:
        page = max(page, 0)
        page_size = 8
        total = await self.repository.count_payment_orders_for_user(user.id)
        if total == 0:
            text = "🧾 تاریخچه پرداخت‌ها\n\nهنوز پرداختی ثبت نشده است."
            if preface:
                text = f"{preface}\n\n{text}"
            await self._send_text(user.platform, user.chat_id, text, keyboard=main_menu())
            return

        max_page = max((total - 1) // page_size, 0)
        if page > max_page:
            page = max_page
        offset = page * page_size
        orders = await self.repository.list_payment_orders_for_user(user.id, limit=page_size, offset=offset)

        rows: list[tuple[int, str]] = []
        for order in orders:
            package = self.sales_catalog.package_by_id(order.package_id)
            package_title = package.title if package else order.package_id
            role = self._order_role_label(order, user.id)
            status = self._payment_status_label(order.status)
            short_title = package_title[:20]
            rows.append((order.id, f"#{order.id} | {short_title} | {status} | {role}"))

        text = (
            f"🧾 تاریخچه پرداخت‌ها\n"
            f"نمایش {offset + 1}-{offset + len(rows)} از {total}\n"
            f"صفحه {page + 1} از {max_page + 1}\n\n"
            "برای مشاهده جزئیات، یکی از سفارش‌ها را انتخاب کنید:"
        )
        if preface:
            text = f"{preface}\n\n{text}"

        await self._send_text(
            user.platform,
            user.chat_id,
            text,
            keyboard=payment_history_keyboard(rows, page=page, has_prev=page > 0, has_next=page < max_page),
        )

    async def _show_payment_order_detail(self, user: User, order_id: int, page: int) -> None:
        order = await self.repository.get_payment_order(order_id)
        if not order:
            await self._show_payment_history(user, page=page, preface="سفارش مورد نظر پیدا نشد.")
            return
        if user.id not in {order.requester_user_id, order.beneficiary_user_id}:
            await self._show_payment_history(user, page=page, preface="به این سفارش دسترسی ندارید.")
            return

        package = self.sales_catalog.package_by_id(order.package_id)
        package_title = package.title if package else order.package_id
        method_title = self._payment_method_title(order.payment_method)
        status_title = self._payment_status_label(order.status)
        role = self._order_role_label(order, user.id)
        stars_text = f"{order.amount_stars} ⭐" if order.amount_stars else "-"
        details = (
            f"🧾 جزئیات سفارش #{order.id}\n\n"
            f"بسته: {package_title}\n"
            f"وضعیت: {status_title}\n"
            f"روش پرداخت: {method_title}\n"
            f"نقش شما: {role}\n"
            f"قیمت مرجع: {self._format_usdt(order.amount_usd)}\n"
            f"قیمت استار (در صورت وجود): {stars_text}\n"
            f"زمان ایجاد: {self._format_order_dt(order.created_at)}\n"
            f"آخرین تغییر وضعیت: {self._format_order_dt(order.updated_at)}"
        )
        if order.approval_note:
            details += f"\nیادداشت بررسی: {order.approval_note}"
        await self._send_text(
            user.platform,
            user.chat_id,
            details,
            keyboard=payment_history_detail_keyboard(page=max(page, 0)),
        )

    def _payment_status_label(self, status: str) -> str:
        mapping = {
            "PENDING_MANUAL": "🕓 در انتظار رسید",
            "PENDING_REVIEW": "🕓 در انتظار تایید",
            "PENDING_STARS": "🕓 در انتظار پرداخت",
            "INVOICE_SENT": "🧾 فاکتور ارسال شد",
            "APPROVED": "✅ تایید شد",
            "REJECTED": "❌ رد شد",
            "FAILED": "⚠️ ناموفق",
            "EXPIRED": "⌛ منقضی",
        }
        return mapping.get(status, status)

    def _payment_method_title(self, method_id: str) -> str:
        method = self.sales_catalog.payment_method(method_id)
        return method.title if method else method_id

    @staticmethod
    def _order_role_label(order: PaymentOrder, current_user_id: int) -> str:
        if order.requester_user_id == order.beneficiary_user_id == current_user_id:
            return "خرید برای خود"
        if order.requester_user_id == current_user_id and order.beneficiary_user_id != current_user_id:
            return "پرداخت برای دیگران"
        if order.beneficiary_user_id == current_user_id and order.requester_user_id != current_user_id:
            return "دریافت هدیه"
        return "مشترک"

    def _format_order_dt(self, iso_text: str) -> str:
        try:
            dt = parse_iso(iso_text)
            local_dt = dt.astimezone(self._rate_tz)
            return local_dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            return iso_text

    async def _show_packages(self, user: User) -> None:
        await self._ensure_daily_usdt_rate(force=False)
        price_labels: dict[str, str] = {}
        for package in self.sales_catalog.packages:
            toman = self._manual_price_toman(package)
            if toman is not None:
                price_labels[package.id] = f"{self._format_usdt(package.price_usd)} | {self._format_toman(toman)}"
            else:
                price_labels[package.id] = self._format_usdt(package.price_usd)
        intro = (
            "🛒 بسته‌های فعال فندق\n\n"
            "یکی از بسته‌ها را برای مشاهده جزئیات و پرداخت انتخاب کنید."
        )
        await self._send_text(
            user.platform,
            user.chat_id,
            intro,
            keyboard=packages_keyboard(self.sales_catalog.packages, price_labels=price_labels),
        )

    async def _show_package_detail(self, user: User, package_id: str) -> None:
        await self._ensure_daily_usdt_rate(force=False)
        package = self.sales_catalog.package_by_id(package_id)
        if not package:
            await self._send_main_menu(user, "بسته انتخاب‌شده یافت نشد.")
            return

        actions = self._package_payment_actions(user, package)
        methods_text = "، ".join(label for label, _ in actions) if actions else "در حال حاضر روشی فعال نیست"
        text = (
            f"📦 {package.title}\n"
            f"{package.description}\n\n"
            f"{self._price_reference_text(package)}\n"
            f"پیام متنی: {package.credits.text_units} واحد\n\n"
            f"ویس: {package.credits.voice_minutes} دقیقه\n"
            f"عکس: {package.credits.photo_count} عدد\n"
            f"روش‌های پرداخت: {methods_text}"
        )

        if actions:
            await self._send_text(
                user.platform,
                user.chat_id,
                text,
                keyboard=package_actions_keyboard(package.id, actions),
            )
            return

        await self._send_text(user.platform, user.chat_id, text, keyboard=main_menu())

    def _package_payment_actions(self, user: User, package: SalesPackage) -> list[tuple[str, str]]:
        actions: list[tuple[str, str]] = []
        admin_channel_ready = bool(self._normalize_telegram_channel_target(self.settings.telegram_admin_channel_id))
        for method_id in package.payment_methods:
            method = self.sales_catalog.payment_method(method_id)
            if not method or not method.enabled:
                continue
            if method_id == "telegram_stars" and user.platform != Platform.TELEGRAM:
                continue
            if method_id == "telegram_ton_wallet" and user.platform != Platform.TELEGRAM:
                continue
            if method_id == "telegram_ton_wallet" and not (self.settings.telegram_ton_pay_enabled and self.crypto_pay_client):
                continue
            if method_id == "bale_wallet" and user.platform != Platform.BALE:
                continue
            if method_id == "bale_wallet" and (self._wallet_price_rial(package) is None):
                continue
            if method_id in {"manual_bank_transfer", "manual_usdt_transfer"} and not admin_channel_ready:
                continue
            actions.append((method.title, method.id))
        return actions

    async def _ask_package_recipient(self, user: User, package_id: str, method_id: str) -> None:
        package = self.sales_catalog.package_by_id(package_id)
        method = self.sales_catalog.payment_method(method_id)
        if not package or not method or not method.enabled:
            await self._send_main_menu(user, "روش پرداخت یا بسته نامعتبر است.")
            return

        if not user.is_registered:
            await self._send_main_menu(user, "برای خرید بسته، ثبت‌نام کامل لازم است.")
            return

        await self.repository.clear_user_state(user.id)
        await self._send_text(
            user.platform,
            user.chat_id,
            f"🎯 بسته «{package.title}» با روش «{method.title}»\nگیرنده شارژ را انتخاب کنید.",
            keyboard=package_recipient_keyboard(package.id, method.id),
        )

    async def _start_package_payment(
        self,
        user: User,
        package_id: str,
        method_id: str,
        beneficiary_user: User | None = None,
    ) -> None:
        package = self.sales_catalog.package_by_id(package_id)
        method = self.sales_catalog.payment_method(method_id)
        if not package or not method or not method.enabled:
            await self._send_main_menu(user, "روش پرداخت یا بسته نامعتبر است.")
            return

        beneficiary = beneficiary_user or user
        if not beneficiary.is_registered:
            await self._send_main_menu(user, "گیرنده شارژ معتبر نیست.")
            return

        identity_key = self._wallet_identity(beneficiary)
        if not identity_key:
            await self._send_main_menu(user, "هویت کیف‌پول گیرنده نامعتبر است.")
            return

        if method_id == "telegram_stars":
            await self._start_stars_payment(user, beneficiary, package, method)
            return
        if method_id == "telegram_ton_wallet":
            await self._start_telegram_ton_wallet_payment(user, beneficiary, package, method)
            return
        if method_id == "bale_wallet":
            await self._start_bale_wallet_payment(user, beneficiary, package, method)
            return
        if method_id == "manual_bank_transfer":
            await self._start_manual_bank_payment(user, beneficiary, package, method)
            return
        if method_id == "manual_usdt_transfer":
            await self._start_manual_usdt_payment(user, beneficiary, package, method)
            return

        await self._send_main_menu(user, "این روش پرداخت هنوز پیاده‌سازی نشده است.")

    async def _start_stars_payment(self, user: User, beneficiary: User, package: SalesPackage, method) -> None:
        if user.platform != Platform.TELEGRAM:
            await self._send_main_menu(user, "پرداخت با استار فقط در تلگرام فعال است.")
            return
        stars_price = self._stars_price(package)
        if stars_price is None:
            await self._send_main_menu(user, "قیمت استار برای این بسته تعریف نشده است.")
            return

        identity_key = self._wallet_identity(beneficiary)
        if not identity_key:
            await self._send_main_menu(user, "هویت کیف‌پول کاربر نامعتبر است.")
            return

        payload = f"stars:{package.id}:{user.id}:{uuid4().hex[:12]}"
        await self.repository.create_payment_order(
            requester_user_id=user.id,
            beneficiary_user_id=beneficiary.id,
            identity_key=identity_key,
            package_id=package.id,
            payment_method="telegram_stars",
            status="INVOICE_SENT",
            amount_usd=package.price_usd,
            amount_stars=stars_price,
            invoice_payload=payload,
            account_id=None,
            receipt_file_id=None,
            receipt_file_platform=None,
            receipt_caption=None,
        )

        try:
            await self.telegram_client.send_invoice(
                chat_id=user.chat_id,
                title=package.title,
                description=package.description or f"خرید {package.title}",
                payload=payload,
                currency="XTR",
                prices=[{"label": package.title, "amount": stars_price}],
                provider_token=None,
            )
        except Exception as exc:
            logger.exception("Failed to send Telegram Stars invoice: %s", exc)
            await self._send_main_menu(
                user,
                "ارسال فاکتور استار ناموفق بود. کمی بعد دوباره تلاش کنید.\n"
                f"{self.sales_catalog.pay_support_text}",
            )
            return

        await self._send_text(
            user.platform,
            user.chat_id,
            "فاکتور استار ارسال شد. پس از پرداخت، تراکنش برای بررسی ادمین به کانال فندق ارسال می‌شود.",
            keyboard=main_menu(),
        )

    async def _start_telegram_ton_wallet_payment(self, user: User, beneficiary: User, package: SalesPackage, method) -> None:
        if user.platform != Platform.TELEGRAM:
            await self._send_main_menu(user, "پرداخت تون خودکار فقط در تلگرام فعال است.")
            return
        if not self.settings.telegram_ton_pay_enabled or not self.crypto_pay_client:
            await self._send_main_menu(
                user,
                "پرداخت تون خودکار فعال نیست. `TELEGRAM_TON_PAY_ENABLED=true` و `TELEGRAM_TON_PAY_API_TOKEN` را تنظیم کنید.",
            )
            return
        if package.price_usd <= 0:
            await self._send_main_menu(user, "قیمت بسته برای پرداخت تون نامعتبر است.")
            return

        identity_key = self._wallet_identity(beneficiary)
        if not identity_key:
            await self._send_main_menu(user, "هویت کیف‌پول گیرنده نامعتبر است.")
            return

        payload = f"tonpay:{package.id}:{user.id}:{uuid4().hex[:12]}"
        description = (package.description or f"خرید {package.title}")[:180]
        try:
            invoice = await self.crypto_pay_client.create_invoice(
                amount_usd=package.price_usd,
                payload=payload,
                description=description,
                paid_btn_url=None,
            )
        except CryptoPayError as exc:
            await self._send_main_menu(
                user,
                f"ایجاد فاکتور تون ناموفق بود: {exc}\n{self.sales_catalog.pay_support_text}",
            )
            return

        invoice_id = str(invoice.get("invoice_id") or invoice.get("id") or "").strip()
        invoice_url = str(invoice.get("bot_invoice_url") or invoice.get("pay_url") or "").strip()
        if not invoice_id or not invoice_url:
            await self._send_main_menu(
                user,
                "پاسخ درگاه تون ناقص بود. لطفاً دوباره تلاش کنید یا با پشتیبانی تماس بگیرید.",
            )
            return

        await self.repository.create_payment_order(
            requester_user_id=user.id,
            beneficiary_user_id=beneficiary.id,
            identity_key=identity_key,
            package_id=package.id,
            payment_method="telegram_ton_wallet",
            status="INVOICE_SENT",
            amount_usd=package.price_usd,
            amount_stars=None,
            invoice_payload=payload,
            account_id=invoice_id,
            receipt_file_id=None,
            receipt_file_platform=None,
            receipt_caption=None,
        )

        keyboard = {
            "inline_keyboard": [
                [{"text": "💎 پرداخت تون", "url": invoice_url}],
                [{"text": "🔙 بازگشت", "callback_data": "pkg:menu"}],
            ]
        }
        beneficiary_line = ""
        if beneficiary.id != user.id:
            beneficiary_line = f"\nگیرنده شارژ: {beneficiary.display_name or '-'} ({beneficiary.bridge_id})"
        await self._send_text(
            user.platform,
            user.chat_id,
            "💎 لینک پرداخت تون آماده شد.\n"
            "پس از پرداخت موفق، اعتبار به‌صورت خودکار شارژ می‌شود."
            f"{beneficiary_line}",
            keyboard=keyboard,
        )

    async def _start_bale_wallet_payment(self, user: User, beneficiary: User, package: SalesPackage, method) -> None:
        if user.platform != Platform.BALE:
            await self._send_main_menu(user, "پرداخت کیف پول بله فقط داخل بله فعال است.")
            return
        await self._ensure_daily_usdt_rate(force=False)
        wallet_price_rial = self._wallet_price_rial(package)
        if wallet_price_rial is None or wallet_price_rial <= 0:
            await self._send_main_menu(user, "قیمت کیف پول بله برای این بسته تعریف نشده است.")
            return
        provider_token = (self.settings.bale_wallet_provider_token or method.provider_token or "").strip()
        if not provider_token:
            await self._send_main_menu(
                user,
                "توکن کیف پول بله تنظیم نشده است. `BALE_WALLET_PROVIDER_TOKEN` را در `.env` یا `provider_token` را در `sales_catalog.json` تنظیم کنید.",
            )
            return

        identity_key = self._wallet_identity(beneficiary)
        if not identity_key:
            await self._send_main_menu(user, "هویت کیف‌پول گیرنده نامعتبر است.")
            return

        payload = f"balewallet:{package.id}:{user.id}:{uuid4().hex[:12]}"
        await self.repository.create_payment_order(
            requester_user_id=user.id,
            beneficiary_user_id=beneficiary.id,
            identity_key=identity_key,
            package_id=package.id,
            payment_method="bale_wallet",
            status="INVOICE_SENT",
            amount_usd=package.price_usd,
            amount_stars=None,
            invoice_payload=payload,
            account_id=None,
            receipt_file_id=None,
            receipt_file_platform=None,
            receipt_caption=None,
        )

        description = package.description or f"خرید {package.title}"
        try:
            await self.bale_client.send_invoice(
                chat_id=user.chat_id,
                title=package.title[:32],
                description=description[:255],
                payload=payload,
                currency="IRR",
                provider_token=provider_token,
                prices=[{"label": package.title[:32], "amount": int(wallet_price_rial)}],
            )
        except Exception as exc:
            logger.exception("Failed to send Bale wallet invoice: %s", exc)
            if "PAYMENT_PROVIDER_INVALID" in str(exc):
                fail_text = (
                    "ارسال فاکتور کیف پول بله ناموفق بود: توکن درگاه کیف پول بله نامعتبر است.\n"
                    "توکن را از BotFather بله برای همین بازو دوباره دریافت و در `BALE_WALLET_PROVIDER_TOKEN` تنظیم کنید.\n"
                    f"{self.sales_catalog.pay_support_text}"
                )
            else:
                fail_text = (
                    "ارسال فاکتور کیف پول بله ناموفق بود. کمی بعد دوباره تلاش کنید.\n"
                    f"{self.sales_catalog.pay_support_text}"
                )
            await self._send_main_menu(
                user,
                fail_text,
            )
            return

        await self._send_text(
            user.platform,
            user.chat_id,
            "فاکتور کیف پول بله ارسال شد. پس از پرداخت، تراکنش برای بررسی ادمین به کانال فندق ارسال می‌شود.",
            keyboard=main_menu(),
        )

    async def _start_manual_bank_payment(self, user: User, beneficiary: User, package: SalesPackage, method) -> None:
        await self._ensure_daily_usdt_rate(force=False)
        if not self._normalize_telegram_channel_target(self.settings.telegram_admin_channel_id):
            await self._send_main_menu(user, "کانال بررسی پرداخت تنظیم نشده است. فعلاً پرداخت دستی در دسترس نیست.")
            return
        account = self.sales_catalog.bank_accounts[0] if self.sales_catalog.bank_accounts else None
        if account is None:
            await self._send_main_menu(user, "حساب بانکی برای پرداخت دستی تعریف نشده است.")
            return

        await self.repository.set_user_state(
            user.id,
            FlowState.PAYMENT_MANUAL_WAIT_RECEIPT,
            {
                "package_id": package.id,
                "payment_method": method.id,
                "account_id": account.id,
                "beneficiary_user_id": beneficiary.id,
            },
        )
        beneficiary_line = ""
        if beneficiary.id != user.id:
            beneficiary_line = f"گیرنده شارژ: {beneficiary.display_name or '-'} ({beneficiary.bridge_id})\n"
        support_id = self.sales_catalog.support_contact_id or "-"
        sheba_line = account.sheba or "-"
        text = (
            f"🏦 پرداخت کارت‌به‌کارت برای {package.title}\n\n"
            f"{self._price_reference_text(package)}\n"
            f"{beneficiary_line}"
            "\n"
            f"دارنده حساب: {account.holder_name}\n"
            f"شماره کارت:\n{account.card_number}\n"
            f"شبا:\n{sheba_line}\n"
            f"آیدی پشتیبانی: {support_id}\n\n"
            "پس از واریز، تصویر رسید را همین‌جا ارسال کنید تا برای بررسی به ادمین فرستاده شود."
        )
        await self._send_text(
            user.platform,
            user.chat_id,
            text,
            keyboard=reply_keyboard([[BTN_BACK]]),
        )

    async def _start_manual_usdt_payment(self, user: User, beneficiary: User, package: SalesPackage, method) -> None:
        await self._ensure_daily_usdt_rate(force=False)
        if not self._normalize_telegram_channel_target(self.settings.telegram_admin_channel_id):
            await self._send_main_menu(user, "کانال بررسی پرداخت تنظیم نشده است. فعلاً پرداخت تتر در دسترس نیست.")
            return
        wallet = self.sales_catalog.usdt_wallets[0] if self.sales_catalog.usdt_wallets else None
        if wallet is None:
            await self._send_main_menu(user, "کیف پول تتر برای پرداخت دستی تعریف نشده است.")
            return

        await self.repository.set_user_state(
            user.id,
            FlowState.PAYMENT_MANUAL_WAIT_RECEIPT,
            {
                "package_id": package.id,
                "payment_method": method.id,
                "account_id": wallet.id,
                "beneficiary_user_id": beneficiary.id,
            },
        )
        beneficiary_line = ""
        if beneficiary.id != user.id:
            beneficiary_line = f"گیرنده شارژ: {beneficiary.display_name or '-'} ({beneficiary.bridge_id})\n"
        support_id = self.sales_catalog.support_contact_id or "-"
        text = (
            f"💵 پرداخت تتر برای {package.title}\n\n"
            f"{self._price_reference_text(package)}\n"
            f"{beneficiary_line}"
            "\n"
            f"شبکه: {wallet.network}\n"
            f"آدرس کیف پول:\n{wallet.wallet_address}\n"
            f"\n\n{wallet.memo or '-'}\n"
            f"آیدی پشتیبانی: {support_id}\n\n"
            "پس از انتقال، اسکرین‌شات یا رسید تراکنش را ارسال کنید تا پس از بررسی ادمین، اعتبار شما شارژ شود.\n"
            f"{method.note}"
        )
        await self._send_text(
            user.platform,
            user.chat_id,
            text,
            keyboard=reply_keyboard([[BTN_BACK]]),
        )

    async def _handle_manual_payment_receipt(self, user: User, incoming: IncomingMessage, state: UserState) -> bool:
        if incoming.content_type != ContentType.PHOTO:
            await self._send_text(
                user.platform,
                user.chat_id,
                "برای ثبت پرداخت، لطفاً تصویر رسید یا اسکرین‌شات تراکنش را ارسال کنید.",
                keyboard=reply_keyboard([[BTN_BACK]]),
            )
            return True

        package_id = str(state.data.get("package_id") or "").strip()
        payment_method = str(state.data.get("payment_method") or "").strip()
        account_id = str(state.data.get("account_id") or "").strip()
        beneficiary_user_id_raw = int(state.data.get("beneficiary_user_id") or user.id)
        package = self.sales_catalog.package_by_id(package_id)
        if not package or not payment_method:
            await self.repository.clear_user_state(user.id)
            await self._send_main_menu(user, "اطلاعات پرداخت نامعتبر بود. دوباره تلاش کنید.")
            return True

        beneficiary = await self.repository.get_user_by_id(beneficiary_user_id_raw)
        if not beneficiary or not beneficiary.is_registered:
            beneficiary = user
        identity_key = self._wallet_identity(beneficiary)
        if not identity_key:
            await self.repository.clear_user_state(user.id)
            await self._send_main_menu(user, "هویت کاربر برای شارژ اعتبار نامعتبر است.")
            return True

        order = await self.repository.create_payment_order(
            requester_user_id=user.id,
            beneficiary_user_id=beneficiary.id,
            identity_key=identity_key,
            package_id=package.id,
            payment_method=payment_method,
            status="PENDING_MANUAL",
            amount_usd=package.price_usd,
            amount_stars=self._stars_price(package),
            invoice_payload=None,
            account_id=account_id or None,
            receipt_file_id=incoming.source_file_id,
            receipt_file_platform=incoming.platform,
            receipt_caption=incoming.caption,
        )

        try:
            admin_message_id = await self._send_payment_receipt_to_admin_channel(order, user, package)
        except Exception as exc:
            logger.exception("Failed to send payment receipt to admin channel: %s", exc)
            admin_message_id = None
        if admin_message_id is not None:
            channel_id = self._normalize_telegram_channel_target(self.settings.telegram_admin_channel_id)
            if channel_id:
                await self.repository.set_payment_order_admin_message(order.id, channel_id, admin_message_id)
            await self.repository.clear_user_state(user.id)
            await self._send_main_menu(
                user,
                "✅ رسید شما ثبت شد و برای بررسی ادمین ارسال گردید.\nپس از تایید، اعتبار بسته به‌صورت خودکار شارژ می‌شود.",
            )
            return True

        await self.repository.clear_user_state(user.id)
        await self._send_main_menu(
            user,
            "⚠️ رسید شما ثبت شد، اما ارسال به کانال بررسی ناموفق بود.\nلطفاً بعداً دوباره تلاش کنید یا با پشتیبانی فروش تماس بگیرید.",
        )
        await self._send_text(
            user.platform,
            user.chat_id,
            self.sales_catalog.pay_support_text,
            keyboard=main_menu(),
        )
        return True

    async def _send_payment_receipt_to_admin_channel(self, order: PaymentOrder, user: User, package: SalesPackage) -> int | None:
        await self._ensure_daily_usdt_rate(force=False)
        channel_id = self._normalize_telegram_channel_target(self.settings.telegram_admin_channel_id)
        if not channel_id:
            logger.warning("Telegram admin channel is not configured for payment review.")
            return None

        beneficiary = await self.repository.get_user_by_id(order.beneficiary_user_id)
        beneficiary_line = ""
        if beneficiary and beneficiary.id != user.id:
            beneficiary_line = (
                f"گیرنده شارژ: {beneficiary.display_name or '-'}\n"
                f"فندق‌آیدی گیرنده: {beneficiary.bridge_id}\n"
                f"پلتفرم گیرنده: {beneficiary.platform.value}\n"
            )

        title = "📥 رسید پرداخت جدید" if order.payment_method in {"manual_bank_transfer", "manual_usdt_transfer"} else "💳 پرداخت موفق جدید (نیازمند تایید)"
        caption = (
            f"{title}\n"
            f"سفارش: #{order.id}\n"
            f"کاربر: {user.display_name or '-'}\n"
            f"پلتفرم: {user.platform.value}\n"
            f"فندق‌آیدی: {user.bridge_id}\n"
            f"شماره: {user.phone_number or '-'}\n"
            f"{beneficiary_line}"
            f"بسته: {package.title}\n"
            f"روش پرداخت: {order.payment_method}\n"
            f"شناسه تراکنش: {order.telegram_charge_id or '-'}\n"
            f"شناسه ارائه‌دهنده: {order.provider_charge_id or '-'}\n"
            f"{self._price_reference_text(package)}\n"
            f"توضیح رسید: {order.receipt_caption or '-'}"
        )
        admin_markup = self._prepare_reply_markup(Platform.TELEGRAM, admin_payment_actions(order.id))

        if not order.receipt_file_id or not order.receipt_file_platform:
            result = await self.telegram_client.send_message(channel_id, caption, reply_markup=admin_markup)
            return int(result.get("message_id", 0))

        if order.receipt_file_platform == Platform.TELEGRAM:
            result = await self.telegram_client.send_photo(
                channel_id,
                photo_file_id=order.receipt_file_id,
                caption=caption,
                reply_markup=admin_markup,
            )
            return int(result.get("message_id", 0))

        source_client = self._client(order.receipt_file_platform)
        file_meta = await source_client.get_file(order.receipt_file_id)
        file_path = file_meta.get("file_path")
        if not file_path:
            raise ValueError("Receipt file_path missing from getFile")

        suffix = Path(file_path).suffix or ".jpg"
        temp_path = Path(self.settings.media_tmp_dir) / f"receipt_{order.id}{suffix}"
        await source_client.download_file(file_path, temp_path)
        try:
            result = await self.telegram_client.send_photo(
                channel_id,
                photo_path=temp_path,
                caption=caption,
                reply_markup=admin_markup,
            )
            return int(result.get("message_id", 0))
        finally:
            temp_path.unlink(missing_ok=True)

    async def _handle_pre_checkout(self, user: User, incoming: IncomingMessage) -> None:
        if not incoming.pre_checkout_query_id:
            return
        client = self._client(incoming.platform)
        payload = incoming.payment_payload or ""
        order = await self.repository.get_payment_order_by_invoice_payload(payload)
        if not order or order.requester_user_id != user.id:
            await client.answer_pre_checkout_query(
                incoming.pre_checkout_query_id,
                ok=False,
                error_message="سفارش پرداخت معتبر نیست.",
            )
            return
        package = self.sales_catalog.package_by_id(order.package_id)
        if not package:
            await client.answer_pre_checkout_query(
                incoming.pre_checkout_query_id,
                ok=False,
                error_message="بسته انتخاب‌شده دیگر در دسترس نیست.",
            )
            return

        if incoming.platform == Platform.TELEGRAM:
            stars_price = self._stars_price(package)
            if order.payment_method != "telegram_stars" or stars_price is None:
                await client.answer_pre_checkout_query(
                    incoming.pre_checkout_query_id,
                    ok=False,
                    error_message="این سفارش برای پرداخت تلگرام معتبر نیست.",
                )
                return
            if (incoming.payment_currency or "").upper() != "XTR":
                await client.answer_pre_checkout_query(
                    incoming.pre_checkout_query_id,
                    ok=False,
                    error_message="واحد پرداخت نامعتبر است.",
                )
                return
            if int(incoming.payment_total_amount or 0) != int(stars_price):
                await client.answer_pre_checkout_query(
                    incoming.pre_checkout_query_id,
                    ok=False,
                    error_message="مبلغ سفارش تغییر کرده است.",
                )
                return
        elif incoming.platform == Platform.BALE:
            bale_wallet_price_rial = self._wallet_price_rial(package)
            if order.payment_method != "bale_wallet" or bale_wallet_price_rial is None:
                await client.answer_pre_checkout_query(
                    incoming.pre_checkout_query_id,
                    ok=False,
                    error_message="این سفارش برای کیف پول بله معتبر نیست.",
                )
                return
            currency = (incoming.payment_currency or "").upper()
            if currency and currency not in {"IRR", "RIAL"}:
                await client.answer_pre_checkout_query(
                    incoming.pre_checkout_query_id,
                    ok=False,
                    error_message="واحد پرداخت کیف پول بله باید ریال باشد.",
                )
                return
            if int(incoming.payment_total_amount or 0) != int(bale_wallet_price_rial):
                await client.answer_pre_checkout_query(
                    incoming.pre_checkout_query_id,
                    ok=False,
                    error_message="مبلغ سفارش با صورتحساب یکسان نیست.",
                )
                return

        await client.answer_pre_checkout_query(incoming.pre_checkout_query_id, ok=True)

    async def _handle_successful_payment(self, user: User, incoming: IncomingMessage) -> None:
        payload = incoming.payment_payload or ""
        if not payload:
            return
        expected_methods: tuple[str, ...]
        if incoming.platform == Platform.TELEGRAM:
            expected_methods = ("telegram_stars",)
        elif incoming.platform == Platform.BALE:
            expected_methods = ("bale_wallet",)
        else:
            return

        order = await self.repository.mark_invoice_payment_received(
            invoice_payload=payload,
            charge_id=incoming.telegram_payment_charge_id or "",
            provider_charge_id=incoming.provider_payment_charge_id,
            expected_methods=expected_methods,
        )
        if not order:
            return

        package = self.sales_catalog.package_by_id(order.package_id)
        payer = await self.repository.get_user_by_id(order.requester_user_id)
        if not package or not payer:
            return

        admin_message_id: int | None = None
        try:
            admin_message_id = await self._send_payment_receipt_to_admin_channel(order, payer, package)
        except Exception as exc:
            logger.exception("Failed to send paid invoice to admin channel: %s", exc)
            admin_message_id = None

        if admin_message_id is not None:
            channel_id = self._normalize_telegram_channel_target(self.settings.telegram_admin_channel_id)
            if channel_id:
                await self.repository.set_payment_order_admin_message(order.id, channel_id, admin_message_id)
            await self._notify_wallet_credited(
                order.requester_user_id,
                "✅ پرداخت شما ثبت شد و برای بررسی ادمین ارسال گردید.\nپس از تایید، اعتبار بسته شارژ می‌شود.",
            )
            return

        await self._notify_wallet_credited(
            order.requester_user_id,
            "⚠️ پرداخت شما ثبت شد، اما ارسال برای بررسی ادمین ناموفق بود.\nلطفاً با پشتیبانی در ارتباط باشید.",
        )

    async def _credit_package_if_needed(self, order: PaymentOrder) -> None:
        if await self.repository.has_payment_credit_entry(order.id):
            return
        package = self.sales_catalog.package_by_id(order.package_id)
        if not package:
            logger.warning("Package %s no longer exists for order %s", order.package_id, order.id)
            return
        await self.repository.apply_credit_delta(
            identity_key=order.identity_key,
            user_id=order.beneficiary_user_id,
            entry_type="PURCHASE_CREDIT",
            text_units_delta=package.credits.text_units,
            voice_minutes_delta=package.credits.voice_minutes,
            photo_count_delta=package.credits.photo_count,
            package_id=package.id,
            payment_order_id=order.id,
            note=f"Package {package.id} credited",
        )

    async def _notify_wallet_credited(self, user_id: int, message: str) -> None:
        item = await self.repository.get_user_by_id(user_id)
        if not item:
            return
        try:
            await self._send_text(item.platform, item.chat_id, message, keyboard=main_menu())
        except Exception as exc:
            logger.warning("Failed to notify wallet update for user %s: %s", item.id, exc)

    @staticmethod
    def _unsupported_message_reason(incoming: IncomingMessage) -> str:
        if incoming.content_type == ContentType.CONTACT:
            return "ارسال ناموفق بود: مخاطب/شماره تماس در اتصال فعال پشتیبانی نمی‌شود. فقط متن، عکس و ویس مجاز است."
        if incoming.content_type == ContentType.UNSUPPORTED and incoming.unsupported_kind:
            labels = {
                "document": "فایل",
                "video": "ویدیو",
                "audio": "فایل صوتی",
                "sticker": "استیکر",
                "animation": "انیمیشن",
                "video_note": "ویدیو نوت",
                "location": "موقعیت مکانی",
                "venue": "مکان",
            }
            kind = labels.get(incoming.unsupported_kind, "این نوع پیام")
            return f"ارسال ناموفق بود: {kind} پشتیبانی نمی‌شود. فقط متن، عکس و ویس مجاز است."
        return "ارسال ناموفق بود: فقط متن، عکس و ویس پشتیبانی می‌شود."

    @staticmethod
    def _delivery_error_reason(exc: Exception) -> str:
        text = str(exc)
        if "MEDIA_MAX_DOWNLOAD_MB" in text:
            return "حجم فایل از سقف دانلود مجاز بیشتر است."
        if "MEDIA_MAX_UPLOAD_MB" in text:
            return "حجم فایل از سقف آپلود مجاز بیشتر است."
        if "message is too long" in text.lower():
            return "متن پیام برای پیام‌رسان مقصد بیش از حد طولانی است."
        if "caption is too long" in text.lower():
            return "متن توضیح رسانه برای پیام‌رسان مقصد بیش از حد طولانی است."
        if "Missing source file id" in text:
            return "شناسه فایل مبدا در دسترس نیست."
        if "file_path missing" in text:
            return "فایل مبدا در دسترس نیست."
        if "bot was blocked" in text.lower():
            return "ربات در پیام‌رسان مقصد توسط کاربر مسدود شده است."
        if "chat not found" in text.lower():
            return "گفت‌وگوی مقصد در دسترس نیست."
        if "unauthorized" in text.lower():
            return "احراز هویت ربات نامعتبر است."
        if isinstance(exc, PlatformApiError):
            return "ارسال به پیام‌رسان مقصد ناموفق بود."
        if isinstance(exc, ValueError):
            return text
        return "خطای موقت در ارسال رخ داد."

    @staticmethod
    def _is_retryable_delivery_error(exc: Exception) -> bool:
        text = str(exc).lower()
        if isinstance(exc, ValueError):
            return False
        if "message is too long" in text or "caption is too long" in text:
            return False
        if "chat not found" in text or "unauthorized" in text:
            return False
        return True

    async def _reserve_usage_credits(self, user: User, incoming: IncomingMessage) -> CreditWallet | None:
        if not self.sales_catalog.rules.enforce_credits:
            identity_key = self._wallet_identity(user)
            return await self.repository.get_wallet(identity_key) if identity_key else None

        identity_key = self._wallet_identity(user)
        if not identity_key:
            await self._send_main_menu(user, "برای استفاده از سرویس، ثبت‌نام کامل لازم است.")
            return None

        text_units = 0
        voice_minutes = 0
        photo_count = 0

        if incoming.content_type == ContentType.TEXT:
            text_units = self.sales_catalog.rules.text_units_for_length(len((incoming.text or "").strip()))
        elif incoming.content_type == ContentType.VOICE:
            duration = incoming.voice_duration_sec or self.sales_catalog.rules.voice_credit_seconds
            voice_minutes = self.sales_catalog.rules.voice_units_for_seconds(duration)
        elif incoming.content_type == ContentType.PHOTO:
            photo_size = incoming.source_file_size or 0
            if photo_size and photo_size > self.sales_catalog.rules.photo_max_file_bytes:
                await self._send_text(
                    user.platform,
                    user.chat_id,
                    f"حجم عکس بیشتر از سقف مجاز است. حداکثر حجم هر عکس {self.sales_catalog.rules.photo_max_file_mb}MB است.",
                    keyboard=connected_menu(),
                )
                return None
            photo_count = self.sales_catalog.rules.photo_credit_unit

        wallet = await self.repository.consume_credits(
            identity_key=identity_key,
            user_id=user.id,
            text_units=text_units,
            voice_minutes=voice_minutes,
            photo_count=photo_count,
            note=f"usage:{incoming.content_type.value}",
        )
        if wallet is not None:
            return wallet

        current = await self.repository.get_wallet(identity_key)
        await self._send_text(
            user.platform,
            user.chat_id,
            "اعتبار شما برای این ارسال کافی نیست.\n"
            f"باقی‌مانده: پیام {current.text_units_remaining} | ویس {current.voice_minutes_remaining} | عکس {current.photo_count_remaining}\n"
            "از گزینه «🛒 خرید بسته» برای شارژ حساب استفاده کنید.",
            keyboard=main_menu(),
        )
        return None

    async def _handle_admin_callback(self, incoming: IncomingMessage) -> None:
        await self._safe_answer_callback(incoming)
        data = (incoming.callback_data or "").strip()
        if not data.startswith("pay:"):
            return
        parts = data.split(":")
        if len(parts) != 3:
            return
        action = parts[1]
        try:
            order_id = int(parts[2])
        except ValueError:
            return

        order = await self.repository.get_payment_order(order_id)
        if not order:
            return

        if action == "approve":
            updated = await self.repository.mark_manual_payment_approved(
                order_id=order_id,
                approved_by_platform=incoming.platform,
                approved_by_user_id=incoming.user_id,
                approval_note="approved from channel",
            )
            if not updated or updated.status != "APPROVED":
                return
            await self._credit_package_if_needed(updated)
            if updated.beneficiary_user_id == updated.requester_user_id:
                await self._notify_wallet_credited(
                    updated.requester_user_id,
                    f"✅ پرداخت شما برای بسته {updated.package_id} تایید شد و اعتبار حساب شارژ شد.",
                )
            else:
                beneficiary = await self.repository.get_user_by_id(updated.beneficiary_user_id)
                if beneficiary:
                    await self._notify_wallet_credited(
                        beneficiary.id,
                        f"✅ یک پرداخت برای بسته {updated.package_id} تایید شد و اعتبار شما شارژ شد.",
                    )
                target_name = beneficiary.display_name if beneficiary else str(updated.beneficiary_user_id)
                await self._notify_wallet_credited(
                    updated.requester_user_id,
                    f"✅ پرداخت شما تایید شد و بسته {updated.package_id} برای «{target_name}» شارژ گردید.",
                )
            return

        if action == "reject":
            updated = await self.repository.mark_manual_payment_rejected(
                order_id=order_id,
                approved_by_platform=incoming.platform,
                approved_by_user_id=incoming.user_id,
                approval_note="rejected from channel",
            )
            if not updated or updated.status != "REJECTED":
                return
            await self._notify_wallet_credited(
                updated.requester_user_id,
                f"❌ پرداخت بسته {updated.package_id} رد شد. در صورت نیاز، پرداخت را دوباره ثبت کنید.",
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

        usage_wallet = await self._reserve_usage_credits(source_user, incoming)
        if usage_wallet is None:
            return

        sender_header = self._sender_header(source_user)
        text_payload: str | None = None
        caption_payload: str | None = None

        if incoming.content_type == ContentType.TEXT:
            body = (incoming.text or "").strip()
            if not body:
                return
            if self.settings.message_max_text_len > 0 and len(body) > self.settings.message_max_text_len:
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
                reply_source_message_id=incoming.message_id,
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
            await self._send_text(
                source_user.platform,
                source_user.chat_id,
                "✅ ارسال با موفقیت انجام شد.",
                keyboard=connected_menu(),
            )
        except Exception as exc:
            self.metrics.failed_total += 1
            reason = self._delivery_error_reason(exc)
            await self.repository.log_message(
                source_user_id=source_user.id,
                dest_user_id=target.id,
                source_platform=source_user.platform,
                dest_platform=target.platform,
                content_type=incoming.content_type,
                status=DeliveryStatus.FAILED,
                error=str(exc),
            )
            if self._is_retryable_delivery_error(exc):
                logger.warning("Immediate delivery failed, queueing message: %s", exc)
                try:
                    await self._queue_outbox(
                        source_user_id=source_user.id,
                        dest_user_id=target.id,
                        content_type=incoming.content_type,
                        text=text_payload,
                        source_file_id=incoming.source_file_id,
                        source_file_platform=source_user.platform,
                        caption=caption_payload,
                    )
                    await self._send_text(
                        source_user.platform,
                        source_user.chat_id,
                        f"⚠️ ارسال موفق نبود.\nعلت: {reason}\nپیام در صف تلاش مجدد قرار گرفت.",
                        keyboard=connected_menu(),
                    )
                    return
                except Exception as queue_exc:
                    logger.warning("Queueing failed after delivery error: %s", queue_exc)
                    reason = f"{reason} (ثبت در صف هم ناموفق بود)"

            await self._send_text(
                source_user.platform,
                source_user.chat_id,
                f"❌ ارسال انجام نشد.\nعلت: {reason}",
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
        reply_source_message_id: int | None = None,
    ) -> None:
        dest_client = self._client(dest_user.platform)
        reply_markup = await self._incoming_reply_markup(dest_user.id, reply_source_user_id, reply_source_message_id)
        reply_markup = self._prepare_reply_markup(dest_user.platform, reply_markup)

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
                reply_source_message_id=None,
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

    async def _send_text(
        self,
        platform: Platform,
        chat_id: str,
        text: str,
        keyboard: dict | None = None,
        reply_to_message_id: int | None = None,
    ) -> None:
        client = self._client(platform)
        reply_markup = self._prepare_reply_markup(platform, keyboard)
        await client.send_message(chat_id, text, reply_markup=reply_markup, reply_to_message_id=reply_to_message_id)

    def _prepare_reply_markup(self, platform: Platform, keyboard: dict | None) -> dict | None:
        if not keyboard:
            return keyboard
        if platform == Platform.TELEGRAM and self.settings.telegram_enable_button_styles:
            return apply_telegram_button_styles(keyboard, mode=self.settings.telegram_button_style_mode)
        return keyboard

    async def _incoming_reply_markup(
        self,
        dest_user_id: int,
        reply_source_user_id: int | None,
        reply_source_message_id: int | None,
    ) -> dict | None:
        if reply_source_user_id is None:
            return None
        active = await self.repository.get_active_target(dest_user_id)
        connected = bool(active and active.id == reply_source_user_id)
        return incoming_reply_actions(
            reply_source_user_id,
            connected=connected,
            source_message_id=reply_source_message_id,
        )

    async def _find_request_matches(
        self,
        phone: str | None,
        username: str | None,
        target_platform: Platform,
    ) -> list[User]:
        results: dict[int, User] = {}
        if phone:
            for user in await self.repository.find_registered_users_by_identifier(phone, target_platform):
                results[user.id] = user
        if username:
            identifier = username if username.startswith("@") else f"@{username}"
            for user in await self.repository.find_registered_users_by_identifier(identifier, target_platform):
                results[user.id] = user
        return list(results.values())

    @staticmethod
    def _format_admin_request_target(phone: str, username: str) -> str:
        parts: list[str] = []
        if phone:
            parts.append(f"phone={phone}")
        if username:
            parts.append(f"username=@{username}")
        return " | ".join(parts)

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

    @staticmethod
    def _parse_connect_user_command(command: str) -> int | None:
        prefix = "connect_user_"
        if not command.startswith(prefix):
            return None
        raw = command.removeprefix(prefix)
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

    async def _connect_to_user_from_command(self, user: User, target_user_id: int) -> None:
        target = await self.repository.get_user_by_id(target_user_id)
        if not target or not target.is_registered:
            await self._send_main_menu(user, "کاربر انتخاب‌شده در دسترس نیست.")
            return
        if target.id == user.id:
            await self._send_main_menu(user, "نمی‌توانید به خودتان وصل شوید.")
            return

        await self.repository.set_active_session(user.id, target.id)
        await self.repository.clear_user_state(user.id)
        await self._send_text(
            user.platform,
            user.chat_id,
            f"🔌 اتصال فعال شد: {target.display_name or '-'} ({target.bridge_id})\nحالا متن/عکس/ویس بفرستید.",
            keyboard=connected_menu(),
        )

    @staticmethod
    def _format_connect_candidates(users: list[User]) -> str:
        lines: list[str] = []
        for user in users:
            username = f"@{user.username}" if user.username else "@-"
            lines.append(
                f"- {user.bridge_id} | {user.platform.value} | {username} | /connect_user_{user.id}"
            )
        return "\n".join(lines)

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
