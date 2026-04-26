from bridge.platforms.parser import parse_update
from bridge.types import ContentType, Platform


def test_parse_contact_message() -> None:
    update = {
        "update_id": 10,
        "message": {
            "message_id": 77,
            "chat": {"id": 1001, "type": "private"},
            "from": {"id": 2002, "username": "foo", "first_name": "Ali"},
            "contact": {"phone_number": "+989121234567"},
        },
    }

    msg = parse_update(Platform.TELEGRAM, update)
    assert msg is not None
    assert msg.content_type == ContentType.CONTACT
    assert msg.phone_number == "+989121234567"


def test_parse_callback_query() -> None:
    update = {
        "update_id": 12,
        "callback_query": {
            "id": "cb1",
            "from": {"id": 333, "username": "u1", "first_name": "Nima"},
            "data": "ct:page:1",
            "message": {
                "message_id": 9,
                "chat": {"id": 333, "type": "private"},
            },
        },
    }

    msg = parse_update(Platform.TELEGRAM, update)
    assert msg is not None
    assert msg.is_callback is True
    assert msg.callback_data == "ct:page:1"
    assert msg.callback_query_id == "cb1"


def test_parse_callback_query_without_chat_type() -> None:
    update = {
        "update_id": 13,
        "callback_query": {
            "id": "cb2",
            "from": {"id": 444, "username": "u2", "first_name": "Sara"},
            "data": "ct:menu",
            "message": {
                "message_id": 10,
                "chat": {"id": 444},
            },
        },
    }

    msg = parse_update(Platform.BALE, update)
    assert msg is not None
    assert msg.is_callback is True
    assert msg.callback_data == "ct:menu"


def test_parse_ignores_non_private() -> None:
    update = {
        "update_id": 11,
        "message": {
            "message_id": 88,
            "chat": {"id": -100, "type": "group"},
            "from": {"id": 2002},
            "text": "hi",
        },
    }

    msg = parse_update(Platform.TELEGRAM, update)
    assert msg is None


def test_parse_pre_checkout_query() -> None:
    update = {
        "update_id": 20,
        "pre_checkout_query": {
            "id": "pcq-1",
            "from": {"id": 777, "username": "buyer", "first_name": "Reza"},
            "currency": "XTR",
            "total_amount": 250,
            "invoice_payload": "stars:starter-100:1:abc123",
        },
    }

    msg = parse_update(Platform.TELEGRAM, update)
    assert msg is not None
    assert msg.content_type == ContentType.PRE_CHECKOUT
    assert msg.pre_checkout_query_id == "pcq-1"
    assert msg.payment_currency == "XTR"
    assert msg.payment_total_amount == 250


def test_parse_successful_payment_message() -> None:
    update = {
        "update_id": 21,
        "message": {
            "message_id": 91,
            "chat": {"id": 1001, "type": "private"},
            "from": {"id": 2002, "username": "buyer", "first_name": "Ali"},
            "successful_payment": {
                "currency": "XTR",
                "total_amount": 250,
                "invoice_payload": "stars:starter-100:1:abc123",
                "telegram_payment_charge_id": "tg-charge",
                "provider_payment_charge_id": "provider-charge",
            },
        },
    }

    msg = parse_update(Platform.TELEGRAM, update)
    assert msg is not None
    assert msg.content_type == ContentType.SUCCESSFUL_PAYMENT
    assert msg.payment_payload == "stars:starter-100:1:abc123"
    assert msg.telegram_payment_charge_id == "tg-charge"
    assert msg.provider_payment_charge_id == "provider-charge"


def test_parse_channel_payment_callback_for_admin_review() -> None:
    update = {
        "update_id": 22,
        "callback_query": {
            "id": "cb-pay",
            "from": {"id": 999, "username": "admin", "first_name": "Admin"},
            "data": "pay:approve:12",
            "message": {
                "message_id": 45,
                "chat": {"id": -1003790742908, "type": "channel"},
            },
        },
    }

    msg = parse_update(Platform.TELEGRAM, update)
    assert msg is not None
    assert msg.is_callback is True
    assert msg.chat_type == "channel"
    assert msg.callback_data == "pay:approve:12"


def test_parse_unsupported_document_message() -> None:
    update = {
        "update_id": 23,
        "message": {
            "message_id": 46,
            "chat": {"id": 1001, "type": "private"},
            "from": {"id": 2002, "username": "buyer", "first_name": "Ali"},
            "document": {"file_id": "doc-1"},
        },
    }

    msg = parse_update(Platform.TELEGRAM, update)
    assert msg is not None
    assert msg.content_type == ContentType.UNSUPPORTED
    assert msg.unsupported_kind == "document"
