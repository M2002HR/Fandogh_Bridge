from __future__ import annotations

import re
import secrets
import string
from datetime import UTC, datetime

ALPHABET = string.ascii_uppercase + string.digits
PHONE_RE = re.compile(r"^09\d{9}$")
TELEGRAM_USERNAME_RE = re.compile(r"@([A-Za-z0-9_]{5,32})")
PHONE_CANDIDATE_RE = re.compile(r"(?:\+98|98|0)?9(?:[\s\-\(\)]*\d){9}")


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def utc_iso() -> str:
    return utc_now().isoformat()


def generate_bridge_id(prefix: str, length: int) -> str:
    if length < 4:
        raise ValueError("BRIDGE_ID_LENGTH must be at least 4")
    suffix_len = length - len(prefix)
    if suffix_len < 3:
        raise ValueError("BRIDGE_ID_LENGTH must be larger than BRIDGE_ID_PREFIX")
    suffix = "".join(secrets.choice(ALPHABET) for _ in range(suffix_len))
    return f"{prefix.upper()}{suffix}"


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def normalize_username(value: str | None) -> str | None:
    if not value:
        return None
    out = value.strip()
    if out.startswith("@"):
        out = out[1:]
    out = out.strip().lower()
    return out or None


def normalize_phone(value: str | None) -> str | None:
    if not value:
        return None

    out = value.strip()
    out = out.replace(" ", "").replace("-", "")
    out = out.replace("(", "").replace(")", "")

    if out.startswith("+98"):
        out = "0" + out[3:]
    elif out.startswith("98"):
        out = "0" + out[2:]
    elif out.startswith("9") and len(out) == 10:
        out = "0" + out

    if not PHONE_RE.match(out):
        return None
    return out


def looks_like_bridge_id(value: str, prefix: str) -> bool:
    v = value.strip().upper()
    return v.startswith(prefix.upper()) and v.isalnum()


def extract_contact_identifiers(text: str | None, shared_phone: str | None = None) -> tuple[str | None, str | None]:
    phone = normalize_phone(shared_phone)
    username: str | None = None
    raw = (text or "").strip()

    if raw:
        if not phone:
            for match in PHONE_CANDIDATE_RE.finditer(raw):
                candidate = normalize_phone(match.group(0))
                if candidate:
                    phone = candidate
                    break

        username_match = TELEGRAM_USERNAME_RE.search(raw)
        if username_match:
            username = normalize_username(username_match.group(1))
        elif phone is None and " " not in raw and "\n" not in raw and any(ch.isalpha() or ch == "_" for ch in raw):
            username = normalize_username(raw)

    return phone, username
