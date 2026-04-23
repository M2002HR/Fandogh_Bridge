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
