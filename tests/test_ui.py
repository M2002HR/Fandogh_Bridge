from bridge.services.ui import (
    BTN_ENTER_PHONE,
    BTN_PAYMENT_HISTORY,
    BTN_SHARE_PHONE,
    apply_telegram_button_styles,
    incoming_reply_actions,
    main_menu,
    phone_menu,
    terms_menu,
)
from bridge.types import Platform


def test_bale_phone_menu_has_no_share_button() -> None:
    menu = phone_menu(Platform.BALE)
    texts = [btn.get("text") for row in menu["keyboard"] for btn in row]
    assert BTN_SHARE_PHONE not in texts
    assert BTN_ENTER_PHONE in texts


def test_telegram_phone_menu_keeps_contact_share() -> None:
    menu = phone_menu(Platform.TELEGRAM)
    first = menu["keyboard"][0][0]
    assert first.get("text") == BTN_SHARE_PHONE
    assert first.get("request_contact") is True


def test_terms_menu_uses_inline_buttons() -> None:
    menu = terms_menu()
    assert "inline_keyboard" in menu
    first = menu["inline_keyboard"][0][0]
    assert first["callback_data"] == "reg:terms:accept"


def test_incoming_reply_actions_vary_by_connection() -> None:
    disconnected = incoming_reply_actions(99, connected=False)
    connected = incoming_reply_actions(99, connected=True)

    assert len(disconnected["inline_keyboard"][0]) == 2
    assert disconnected["inline_keyboard"][0][0]["callback_data"] == "in:connect:99"
    assert disconnected["inline_keyboard"][1][0]["callback_data"] == "in:seen:99:0"
    assert connected["inline_keyboard"][0][0]["callback_data"] == "in:reply:99"
    assert connected["inline_keyboard"][0][1]["callback_data"] == "in:seen:99:0"


def test_main_menu_contains_payment_history_button() -> None:
    menu = main_menu()
    texts = [btn.get("text") for row in menu["keyboard"] for btn in row]
    assert BTN_PAYMENT_HISTORY in texts


def test_apply_telegram_button_styles_default_none_has_no_style() -> None:
    styled = apply_telegram_button_styles(terms_menu())
    assert styled is not None
    accept = styled["inline_keyboard"][0][0]
    decline = styled["inline_keyboard"][0][1]
    assert "style" not in accept
    assert "style" not in decline


def test_apply_telegram_button_styles_auto_adds_style() -> None:
    styled = apply_telegram_button_styles(terms_menu(), mode="auto")
    assert styled is not None
    accept = styled["inline_keyboard"][0][0]
    decline = styled["inline_keyboard"][0][1]
    assert accept["style"] == "success"
    assert decline["style"] == "danger"

