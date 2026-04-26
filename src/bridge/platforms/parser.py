from __future__ import annotations

from bridge.types import ContentType, IncomingMessage, Platform


def parse_update(platform: Platform, update: dict) -> IncomingMessage | None:
    pre_checkout = update.get("pre_checkout_query")
    if isinstance(pre_checkout, dict):
        from_user = pre_checkout.get("from") or {}
        user_id_raw = from_user.get("id")
        if user_id_raw is None:
            return None
        first_name = (from_user.get("first_name") or "").strip()
        last_name = (from_user.get("last_name") or "").strip()
        display_name = f"{first_name} {last_name}".strip() or from_user.get("username") or "unknown"
        return IncomingMessage(
            platform=platform,
            update_id=int(update["update_id"]),
            chat_id=str(user_id_raw),
            user_id=str(user_id_raw),
            username=from_user.get("username"),
            display_name=display_name,
            message_id=0,
            content_type=ContentType.PRE_CHECKOUT,
            payment_payload=str(pre_checkout.get("invoice_payload") or ""),
            payment_currency=str(pre_checkout.get("currency") or ""),
            payment_total_amount=int(pre_checkout.get("total_amount") or 0),
            pre_checkout_query_id=str(pre_checkout.get("id") or ""),
            raw=update,
            chat_type="private",
        )

    callback = update.get("callback_query")
    if isinstance(callback, dict):
        from_user = callback.get("from") or {}
        message = callback.get("message") or {}
        chat = message.get("chat") or {}

        chat_type = chat.get("type")
        callback_data = str(callback.get("data") or "")
        if chat_type not in {None, "private"} and not (
            platform == Platform.TELEGRAM and chat_type == "channel" and callback_data.startswith("pay:")
        ):
            return None
        if not from_user:
            return None

        chat_id_raw = chat.get("id")
        user_id_raw = from_user.get("id")
        if chat_id_raw is None or user_id_raw is None:
            return None
        chat_id = str(chat_id_raw)
        user_id = str(user_id_raw)
        first_name = (from_user.get("first_name") or "").strip()
        last_name = (from_user.get("last_name") or "").strip()
        display_name = f"{first_name} {last_name}".strip() or from_user.get("username") or "unknown"

        return IncomingMessage(
            platform=platform,
            update_id=int(update["update_id"]),
            chat_id=chat_id,
            user_id=user_id,
            username=from_user.get("username"),
            display_name=display_name,
            message_id=int(message.get("message_id", 0)),
            content_type=ContentType.TEXT,
            text=None,
            is_callback=True,
            callback_data=callback_data,
            callback_query_id=str(callback.get("id") or ""),
            raw=update,
            chat_type=chat_type or "private",
        )

    message = update.get("message")
    if not message:
        return None

    chat = message.get("chat") or {}
    chat_type = chat.get("type")
    if chat_type not in {None, "private"}:
        return None

    sender = message.get("from") or {}
    if not sender:
        return None

    chat_id_raw = chat.get("id")
    user_id_raw = sender.get("id")
    if chat_id_raw is None or user_id_raw is None:
        return None
    chat_id = str(chat_id_raw)
    user_id = str(user_id_raw)

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
        chat_type=chat_type or "private",
    )

    successful_payment = message.get("successful_payment")
    if isinstance(successful_payment, dict):
        return IncomingMessage(
            content_type=ContentType.SUCCESSFUL_PAYMENT,
            payment_payload=str(successful_payment.get("invoice_payload") or ""),
            payment_currency=str(successful_payment.get("currency") or ""),
            payment_total_amount=int(successful_payment.get("total_amount") or 0),
            telegram_payment_charge_id=successful_payment.get("telegram_payment_charge_id"),
            provider_payment_charge_id=successful_payment.get("provider_payment_charge_id"),
            **base,
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
            source_file_size=int(largest.get("file_size") or 0) or None,
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
            source_file_size=int(voice.get("file_size") or 0) or None,
            voice_duration_sec=int(voice.get("duration") or 0) or None,
            **base,
        )

    for key in ("document", "video", "audio", "sticker", "animation", "video_note", "location", "venue"):
        if key in message:
            return IncomingMessage(
                content_type=ContentType.UNSUPPORTED,
                unsupported_kind=key,
                **base,
            )

    return None
