from bridge.utils import extract_contact_identifiers, generate_bridge_id, normalize_phone, normalize_username


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


def test_extract_contact_identifiers_variants() -> None:
    assert extract_contact_identifiers("@UserName") == (None, "username")
    assert extract_contact_identifiers("0912-123-4567") == ("09121234567", None)
    assert extract_contact_identifiers("phone: 09121234567 user: @Foo_bar") == ("09121234567", "foo_bar")
    assert extract_contact_identifiers(None, "+989121234567") == ("09121234567", None)
