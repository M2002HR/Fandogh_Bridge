from __future__ import annotations

from bridge.types import Platform

BTN_REGISTER = "📝 ثبت‌نام"
BTN_HELP = "📘 راهنما"
BTN_BACK = "🔙 بازگشت"

BTN_ACCEPT_TERMS = "✅ قبول شرایط"
BTN_DECLINE_TERMS = "❌ عدم پذیرش"
BTN_SHARE_PHONE = "📱 ارسال شماره من"
BTN_MANUAL_PHONE = "✍️ ورود دستی شماره"

BTN_CONNECT = "🔗 اتصال به مخاطب"
BTN_ADD_CONTACT = "➕ افزودن مخاطب"
BTN_CONTACTS = "👥 لیست مخاطبین"
BTN_MY_ID = "🆔 شناسه من"
BTN_END_SESSION = "⛔ پایان اتصال"
BTN_REQUEST_ADMIN = "🆘 درخواست اطلاع‌رسانی"

BTN_PLATFORM_TELEGRAM = "🟦 تلگرام"
BTN_PLATFORM_BALE = "🟧 بله"
BTN_PLATFORM_ANY = "🌐 فرقی ندارد"

BTN_BLOCK = "🚫 بلاک"
BTN_UNBLOCK = "✅ آنبلاک"
BTN_PROFILE = "👤 پروفایل"
BTN_DELETE_CONTACT = "🗑 حذف مخاطب"
BTN_CONNECT_CONTACT = "🔌 اتصال"

BTN_SKIP_NOTE = "⏭ بدون توضیح"


def reply_keyboard(rows: list[list[str]], resize: bool = True) -> dict:
    keyboard = [[{"text": text} for text in row] for row in rows]
    return {
        "keyboard": keyboard,
        "resize_keyboard": resize,
        "one_time_keyboard": False,
    }


def pre_login_menu() -> dict:
    return reply_keyboard(
        [
            [BTN_REGISTER, BTN_HELP],
        ]
    )


def terms_menu() -> dict:
    return reply_keyboard(
        [
            [BTN_ACCEPT_TERMS, BTN_DECLINE_TERMS],
            [BTN_BACK],
        ]
    )


def phone_menu(platform: Platform) -> dict:
    contact_btn: dict[str, str | bool] = {"text": BTN_SHARE_PHONE}
    if platform == Platform.TELEGRAM:
        contact_btn["request_contact"] = True
    keyboard = [
        [contact_btn],
        [{"text": BTN_MANUAL_PHONE}, {"text": BTN_BACK}],
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
            [BTN_MY_ID, BTN_END_SESSION],
            [BTN_HELP],
        ]
    )


def platform_menu() -> dict:
    return reply_keyboard(
        [
            [BTN_PLATFORM_TELEGRAM, BTN_PLATFORM_BALE],
            [BTN_PLATFORM_ANY],
            [BTN_BACK],
        ]
    )


def contact_actions_menu() -> dict:
    return reply_keyboard(
        [
            [BTN_CONNECT_CONTACT, BTN_PROFILE],
            [BTN_BLOCK, BTN_UNBLOCK],
            [BTN_DELETE_CONTACT],
            [BTN_BACK],
        ]
    )


def note_menu() -> dict:
    return reply_keyboard(
        [
            [BTN_SKIP_NOTE],
            [BTN_BACK],
        ]
    )
