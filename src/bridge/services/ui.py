from __future__ import annotations

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


def incoming_reply_actions(source_user_id: int, connected: bool) -> dict:
    rows: list[list[dict]] = []
    if connected:
        rows.append([{"text": "💬 پاسخ", "callback_data": f"in:reply:{source_user_id}"}])
    else:
        rows.append(
            [
                {"text": "🔌 اتصال", "callback_data": f"in:connect:{source_user_id}"},
                {"text": "💬 پاسخ", "callback_data": f"in:reply:{source_user_id}"},
            ]
        )
    return inline_keyboard(rows)
