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


class BridgeError(Exception):
    pass


class PlatformApiError(BridgeError):
    pass


class UnsupportedContentError(BridgeError):
    pass
