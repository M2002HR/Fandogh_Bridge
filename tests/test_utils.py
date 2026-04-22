from bridge.utils import generate_bridge_id, normalize_phone, normalize_username


def test_generate_bridge_id_format_and_uniqueness() -> None:
    generated = {generate_bridge_id("FDG", 10) for _ in range(300)}
    assert len(generated) == 300

    for bridge_id in generated:
        assert bridge_id.startswith("FDG")
        assert len(bridge_id) == 10
        assert bridge_id.isalnum()


def test_normalize_phone_variants() -> None:
    assert normalize_phone("+989121234567") == "09121234567"
    assert normalize_phone("989121234567") == "09121234567"
    assert normalize_phone("9121234567") == "09121234567"
    assert normalize_phone("0912-123-4567") == "09121234567"
    assert normalize_phone("123") is None


def test_normalize_username() -> None:
    assert normalize_username("@UserName") == "username"
    assert normalize_username("  foo_bar ") == "foo_bar"
    assert normalize_username(" ") is None
