from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class Platform(str, Enum):
    TELEGRAM = "telegram"
    BALE = "bale"


class ContentType(str, Enum):
    TEXT = "TEXT"
    PHOTO = "PHOTO"
    VOICE = "VOICE"
    CONTACT = "CONTACT"
    UNSUPPORTED = "UNSUPPORTED"
    PRE_CHECKOUT = "PRE_CHECKOUT"
    SUCCESSFUL_PAYMENT = "SUCCESSFUL_PAYMENT"


class DeliveryStatus(str, Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    EXPIRED = "EXPIRED"


@dataclass(slots=True)
class IncomingMessage:
    platform: Platform
    update_id: int
    chat_id: str
    user_id: str
    username: str | None
    display_name: str | None
    message_id: int
    content_type: ContentType
    text: str | None = None
    caption: str | None = None
    source_file_id: str | None = None
    phone_number: str | None = None
    is_callback: bool = False
    callback_data: str | None = None
    callback_query_id: str | None = None
    raw: dict[str, Any] | None = None
    chat_type: str | None = "private"
    source_file_size: int | None = None
    voice_duration_sec: int | None = None
    payment_payload: str | None = None
    payment_currency: str | None = None
    payment_total_amount: int | None = None
    telegram_payment_charge_id: str | None = None
    provider_payment_charge_id: str | None = None
    pre_checkout_query_id: str | None = None
    unsupported_kind: str | None = None


@dataclass(slots=True)
class User:
    id: int
    platform: Platform
    platform_user_id: str
    chat_id: str
    bridge_id: str
    username: str | None
    display_name: str | None
    phone_number: str | None
    is_registered: bool
    terms_accepted_at: str | None
    registration_completed_at: str | None


@dataclass(slots=True)
class ContactEntry:
    id: int
    owner_user_id: int
    target_user_id: int
    alias: str
    created_at: str
    updated_at: str


@dataclass(slots=True)
class UserState:
    state: str
    data: dict[str, Any]


@dataclass(slots=True)
class OutboxItem:
    id: int
    source_user_id: int
    dest_user_id: int
    content_type: ContentType
    text: str | None
    source_file_id: str | None
    source_file_platform: Platform | None
    caption: str | None
    attempts: int
    next_retry_at: str
    expires_at: str


@dataclass(slots=True)
class CreditWallet:
    identity_key: str
    text_units_remaining: int
    voice_minutes_remaining: int
    photo_count_remaining: int
    updated_at: str


@dataclass(slots=True)
class UsdtRateRecord:
    date_local: str
    rate_toman: int
    source: str | None
    raw_text: str | None
    fetched_at: str


@dataclass(slots=True)
class PaymentOrder:
    id: int
    requester_user_id: int
    beneficiary_user_id: int
    identity_key: str
    package_id: str
    payment_method: str
    status: str
    amount_usd: float
    amount_stars: int | None
    invoice_payload: str | None
    telegram_charge_id: str | None
    provider_charge_id: str | None
    receipt_file_id: str | None
    receipt_file_platform: Platform | None
    receipt_caption: str | None
    account_id: str | None
    admin_channel_chat_id: str | None
    admin_channel_message_id: int | None
    approved_by_platform: Platform | None
    approved_by_user_id: str | None
    approval_note: str | None
    created_at: str
    updated_at: str


class BridgeError(Exception):
    pass


class PlatformApiError(BridgeError):
    pass


class UnsupportedContentError(BridgeError):
    pass
