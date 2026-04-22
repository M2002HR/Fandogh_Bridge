from __future__ import annotations

from bridge.types import ContentType, IncomingMessage, Platform


def parse_update(platform: Platform, update: dict) -> IncomingMessage | None:
    message = update.get("message")
    if not message:
        return None

    chat = message.get("chat") or {}
    if chat.get("type") != "private":
        return None

    sender = message.get("from") or {}
    if not sender:
        return None

    chat_id = str(chat.get("id"))
    user_id = str(sender.get("id"))

    first_name = (sender.get("first_name") or "").strip()
    last_name = (sender.get("last_name") or "").strip()
    display_name = f"{first_name} {last_name}".strip() or sender.get("username") or "unknown"

    base = dict(
        platform=platform,
        update_id=int(update["update_id"]),
        chat_id=chat_id,
        user_id=user_id,
        username=sender.get("username"),
        display_name=display_name,
        message_id=int(message.get("message_id", 0)),
        raw=update,
    )

    if isinstance(message.get("contact"), dict):
        phone = message["contact"].get("phone_number")
        if phone:
            return IncomingMessage(content_type=ContentType.CONTACT, phone_number=phone, **base)

    if "text" in message and isinstance(message.get("text"), str):
        return IncomingMessage(content_type=ContentType.TEXT, text=message["text"], **base)

    if isinstance(message.get("photo"), list) and message["photo"]:
        largest = message["photo"][-1]
        file_id = largest.get("file_id")
        if not file_id:
            return None
        return IncomingMessage(
            content_type=ContentType.PHOTO,
            caption=message.get("caption"),
            source_file_id=file_id,
            **base,
        )

    if isinstance(message.get("voice"), dict):
        voice = message["voice"]
        file_id = voice.get("file_id")
        if not file_id:
            return None
        return IncomingMessage(
            content_type=ContentType.VOICE,
            caption=message.get("caption"),
            source_file_id=file_id,
            **base,
        )

    return None
