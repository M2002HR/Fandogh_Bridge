from __future__ import annotations

from copy import deepcopy

from bridge.sales import SalesPackage
from bridge.types import ContactEntry, Platform

BTN_REGISTER = "📝 ثبت‌نام"
BTN_HELP = "📘 راهنما"
BTN_BACK = "🔙 بازگشت"

BTN_ACCEPT_TERMS = "✅ قبول شرایط"
BTN_DECLINE_TERMS = "❌ عدم پذیرش"
BTN_SHARE_PHONE = "📱 ارسال شماره من"
BTN_ENTER_PHONE = "📱 ورود شماره خودم"

BTN_CONNECT = "🔗 اتصال به مخاطب"
BTN_ADD_CONTACT = "➕ افزودن مخاطب"
BTN_CONTACTS = "👥 لیست مخاطبین"
BTN_MY_ID = "🆔 فندق‌آیدی من"
BTN_END_SESSION = "⛔ پایان اتصال"
BTN_REQUEST_ADMIN = "🆘 درخواست اطلاع‌رسانی"
BTN_BALANCE = "💳 اعتبار من"
BTN_BUY_PACKAGE = "🛒 خرید بسته"
BTN_PAYMENT_HISTORY = "🧾 تاریخچه پرداخت‌ها"
BTN_SUPPORT = "💬 ارتباط با پشتیبانی"

BTN_PLATFORM_TELEGRAM = "🟦 تلگرام"
BTN_PLATFORM_BALE = "🟧 بله"
BTN_PLATFORM_ANY = "🌐 فرقی ندارد"

BTN_SKIP_NOTE = "⏭ بدون توضیح"


def reply_keyboard(rows: list[list[str]], resize: bool = True) -> dict:
    keyboard = [[{"text": text} for text in row] for row in rows]
    return {
        "keyboard": keyboard,
        "resize_keyboard": resize,
        "one_time_keyboard": False,
    }


def inline_keyboard(rows: list[list[dict]]) -> dict:
    return {"inline_keyboard": rows}


def pre_login_menu() -> dict:
    return reply_keyboard([[BTN_REGISTER, BTN_HELP]])


def terms_menu() -> dict:
    return inline_keyboard(
        [
            [
                {"text": BTN_ACCEPT_TERMS, "callback_data": "reg:terms:accept"},
                {"text": BTN_DECLINE_TERMS, "callback_data": "reg:terms:decline"},
            ],
            [{"text": BTN_BACK, "callback_data": "reg:terms:back"}],
        ]
    )


def phone_menu(platform: Platform) -> dict:
    if platform == Platform.BALE:
        return reply_keyboard(
            [
                [BTN_ENTER_PHONE],
                [BTN_BACK],
            ]
        )

    # Telegram keeps contact-share option.
    keyboard = [
        [{"text": BTN_SHARE_PHONE, "request_contact": True}],
        [{"text": BTN_ENTER_PHONE}, {"text": BTN_BACK}],
    ]
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


def main_menu() -> dict:
    return reply_keyboard(
        [
            [BTN_CONNECT, BTN_ADD_CONTACT],
            [BTN_CONTACTS, BTN_REQUEST_ADMIN],
            [BTN_BALANCE, BTN_BUY_PACKAGE],
            [BTN_PAYMENT_HISTORY, BTN_SUPPORT],
            [BTN_MY_ID, BTN_HELP],
        ]
    )


def connected_menu() -> dict:
    return reply_keyboard([[BTN_END_SESSION]])


def platform_menu() -> dict:
    return reply_keyboard(
        [
            [BTN_PLATFORM_TELEGRAM, BTN_PLATFORM_BALE],
            [BTN_PLATFORM_ANY],
            [BTN_BACK],
        ]
    )


def note_menu() -> dict:
    return reply_keyboard([[BTN_SKIP_NOTE], [BTN_BACK]])


def contacts_page_keyboard(contacts: list[ContactEntry], page: int, page_size: int = 5) -> dict:
    total = len(contacts)
    start = page * page_size
    end = start + page_size
    slice_ = contacts[start:end]

    rows: list[list[dict]] = []
    for c in slice_:
        rows.append(
            [
                {
                    "text": f"👤 {c.alias}",
                    "callback_data": f"ct:open:{c.id}:{page}",
                }
            ]
        )

    nav_row: list[dict] = []
    if page > 0:
        nav_row.append({"text": "◀️ قبلی", "callback_data": f"ct:page:{page-1}"})

    max_page = (total - 1) // page_size if total else 0
    if page < max_page:
        nav_row.append({"text": "بعدی ▶️", "callback_data": f"ct:page:{page+1}"})

    if nav_row:
        rows.append(nav_row)

    rows.append([{"text": "🔙 بازگشت به منو", "callback_data": "ct:menu"}])
    return inline_keyboard(rows)


def contact_profile_actions(contact_id: int, page: int, blocked: bool) -> dict:
    rows: list[list[dict]] = [
        [
            {"text": "🔌 اتصال", "callback_data": f"ct:connect:{contact_id}:{page}"},
            {"text": "🗑 حذف", "callback_data": f"ct:delete:{contact_id}:{page}"},
        ],
    ]
    if blocked:
        rows.append([{"text": "✅ آنبلاک", "callback_data": f"ct:unblock:{contact_id}:{page}"}])
    else:
        rows.append([{"text": "🚫 بلاک", "callback_data": f"ct:block:{contact_id}:{page}"}])

    rows.append([{"text": "🔙 بازگشت به لیست", "callback_data": f"ct:page:{page}"}])
    return inline_keyboard(rows)


def incoming_reply_actions(source_user_id: int, connected: bool, source_message_id: int | None = None) -> dict:
    rows: list[list[dict]] = []
    seen_data = f"in:seen:{source_user_id}:{source_message_id or 0}"
    if connected:
        rows.append(
            [
                {"text": "💬 پاسخ", "callback_data": f"in:reply:{source_user_id}"},
                {"text": "👁️ مشاهده شد", "callback_data": seen_data},
            ]
        )
    else:
        rows.append(
            [
                {"text": "🔌 اتصال", "callback_data": f"in:connect:{source_user_id}"},
                {"text": "💬 پاسخ", "callback_data": f"in:reply:{source_user_id}"},
            ]
        )
        rows.append([{"text": "👁️ مشاهده شد", "callback_data": seen_data}])
    return inline_keyboard(rows)


def packages_keyboard(packages: list[SalesPackage], price_labels: dict[str, str] | None = None) -> dict:
    rows = [
        [
            {
                "text": f"{item.title} | {(price_labels or {}).get(item.id, f'${item.price_usd:.2f}')}",
                "callback_data": f"pkg:open:{item.id}",
            }
        ]
        for item in packages
    ]
    rows.append([{"text": "🔙 بازگشت", "callback_data": "pkg:menu"}])
    return inline_keyboard(rows)


def package_actions_keyboard(package_id: str, actions: list[tuple[str, str]]) -> dict:
    rows = [[{"text": label, "callback_data": f"pkg:pay:{method_id}:{package_id}"}] for label, method_id in actions]
    rows.append([{"text": "🔙 بازگشت به بسته‌ها", "callback_data": "pkg:list"}])
    return inline_keyboard(rows)


def package_recipient_keyboard(package_id: str, method_id: str) -> dict:
    return inline_keyboard(
        [
            [{"text": "🙋 خرید برای خودم", "callback_data": f"pkg:who:self:{method_id}:{package_id}"}],
            [{"text": "🎁 خرید برای دیگران", "callback_data": f"pkg:who:other:{method_id}:{package_id}"}],
            [{"text": "🔙 بازگشت", "callback_data": f"pkg:open:{package_id}"}],
        ]
    )


def package_beneficiary_candidates_keyboard(
    package_id: str,
    method_id: str,
    candidates: list[tuple[int, str]],
) -> dict:
    rows: list[list[dict]] = []
    for user_id, title in candidates:
        rows.append(
            [
                {
                    "text": f"🎯 {title}",
                    "callback_data": f"pkg:benef:{method_id}:{package_id}:{user_id}",
                }
            ]
        )
    rows.append([{"text": "🔙 بازگشت", "callback_data": f"pkg:open:{package_id}"}])
    return inline_keyboard(rows)


def admin_payment_actions(order_id: int) -> dict:
    return inline_keyboard(
        [
            [
                {"text": "✅ تایید", "callback_data": f"pay:approve:{order_id}"},
                {"text": "❌ رد", "callback_data": f"pay:reject:{order_id}"},
            ]
        ]
    )


def payment_history_keyboard(items: list[tuple[int, str]], page: int, has_prev: bool, has_next: bool) -> dict:
    rows: list[list[dict]] = []
    for order_id, label in items:
        rows.append([{"text": label, "callback_data": f"payh:open:{order_id}:{page}"}])

    nav: list[dict] = []
    if has_prev:
        nav.append({"text": "◀️ قبلی", "callback_data": f"payh:page:{page-1}"})
    if has_next:
        nav.append({"text": "بعدی ▶️", "callback_data": f"payh:page:{page+1}"})
    if nav:
        rows.append(nav)

    rows.append([{"text": "🔙 بازگشت به منو", "callback_data": "payh:menu"}])
    return inline_keyboard(rows)


def payment_history_detail_keyboard(page: int) -> dict:
    return inline_keyboard(
        [
            [{"text": "🔙 بازگشت به تاریخچه", "callback_data": f"payh:page:{page}"}],
            [{"text": "🏠 منوی اصلی", "callback_data": "payh:menu"}],
        ]
    )


def apply_telegram_button_styles(markup: dict | None, mode: str = "none") -> dict | None:
    if not markup:
        return markup
    if mode != "auto":
        # Keep structure untouched; styles can be enabled later by switching mode to "auto".
        return deepcopy(markup)
    styled = deepcopy(markup)
    for key in ("keyboard", "inline_keyboard"):
        rows = styled.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, list):
                continue
            for button in row:
                if isinstance(button, dict):
                    _apply_single_button_style(button)
    return styled


def _apply_single_button_style(button: dict) -> None:
    if "style" in button:
        return
    text = str(button.get("text", "")).strip()
    if not text:
        return
    style = _style_for_text(text)
    if style:
        button["style"] = style


def _style_for_text(text: str) -> str | None:
    danger_prefixes = ("❌", "⛔", "🚫", "🗑")
    success_prefixes = ("✅", "💬", "👁️", "🎁", "🙋")
    primary_prefixes = (
        "📝",
        "📘",
        "🔙",
        "🔗",
        "➕",
        "👥",
        "🆔",
        "🆘",
        "💳",
        "🛒",
        "🧾",
        "💬",
        "🟦",
        "🟧",
        "🌐",
        "🎯",
        "⚡",
        "📦",
        "🏠",
    )
    if text.startswith(danger_prefixes):
        return "danger"
    if text.startswith(success_prefixes):
        return "success"
    if text.startswith(primary_prefixes):
        return "primary"
    return None
