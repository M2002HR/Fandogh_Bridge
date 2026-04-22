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
