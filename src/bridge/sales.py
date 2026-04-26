from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class PackageCredits:
    text_units: int
    voice_minutes: int
    photo_count: int


@dataclass(slots=True)
class CreditRules:
    text_segment_chars: int
    voice_credit_seconds: int
    photo_max_file_mb: int
    photo_credit_unit: int
    starter_credits: PackageCredits
    enforce_credits: bool

    @property
    def photo_max_file_bytes(self) -> int:
        return self.photo_max_file_mb * 1024 * 1024

    def text_units_for_length(self, length: int) -> int:
        if length <= 0:
            return 0
        if self.text_segment_chars <= 0:
            return 1
        return math.ceil(length / self.text_segment_chars)

    @property
    def text_is_unlimited_per_message(self) -> bool:
        return self.text_segment_chars <= 0

    def voice_units_for_seconds(self, duration_sec: int) -> int:
        if duration_sec <= 0:
            return 0
        return math.ceil(duration_sec / self.voice_credit_seconds)


@dataclass(slots=True)
class BankAccount:
    id: str
    title: str
    holder_name: str
    card_number: str
    account_number: str
    sheba: str


@dataclass(slots=True)
class UsdtWallet:
    id: str
    title: str
    network: str
    wallet_address: str
    memo: str


@dataclass(slots=True)
class PaymentMethod:
    id: str
    enabled: bool
    title: str
    note: str
    provider_token: str


@dataclass(slots=True)
class SalesPackage:
    id: str
    title: str
    description: str
    price_usd: float
    credits: PackageCredits
    payment_methods: list[str]


@dataclass(slots=True)
class SalesCatalog:
    version: int
    rules: CreditRules
    packages: list[SalesPackage]
    payment_methods: dict[str, PaymentMethod]
    bank_accounts: list[BankAccount]
    usdt_wallets: list[UsdtWallet]
    usdt_to_toman_rate: int
    usdt_rate_channel: str
    usdt_rate_cache_sec: int
    usdt_rate_refresh_hour_local: int
    usdt_rate_timezone: str
    support_contact_id: str
    support_message: str
    pay_support_text: str

    def package_by_id(self, package_id: str) -> SalesPackage | None:
        for item in self.packages:
            if item.id == package_id:
                return item
        return None

    def payment_method(self, method_id: str) -> PaymentMethod | None:
        return self.payment_methods.get(method_id)

    def bank_account(self, account_id: str) -> BankAccount | None:
        for item in self.bank_accounts:
            if item.id == account_id:
                return item
        return None

    def usdt_wallet(self, wallet_id: str) -> UsdtWallet | None:
        for item in self.usdt_wallets:
            if item.id == wallet_id:
                return item
        return None


def load_sales_catalog(path: str) -> SalesCatalog:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))

    starter = raw.get("starter_credits") or {}
    rules = CreditRules(
        text_segment_chars=int(raw["credit_rules"]["text_segment_chars"]),
        voice_credit_seconds=int(raw["credit_rules"]["voice_credit_seconds"]),
        photo_max_file_mb=int(raw["credit_rules"]["photo_max_file_mb"]),
        photo_credit_unit=int(raw["credit_rules"].get("photo_credit_unit", 1)),
        starter_credits=PackageCredits(
            text_units=int(starter.get("text_units", 0)),
            voice_minutes=int(starter.get("voice_minutes", 0)),
            photo_count=int(starter.get("photo_count", 0)),
        ),
        enforce_credits=bool(raw.get("enforce_credits", True)),
    )

    packages = [
        SalesPackage(
            id=str(item["id"]),
            title=str(item["title"]),
            description=str(item.get("description", "")).strip(),
            price_usd=float(item["price_usd"]),
            credits=PackageCredits(
                text_units=int(item["credits"]["text_units"]),
                voice_minutes=int(item["credits"]["voice_minutes"]),
                photo_count=int(item["credits"]["photo_count"]),
            ),
            payment_methods=[str(method_id) for method_id in item.get("payment_methods", [])],
        )
        for item in raw["packages"]
    ]

    payment_methods = {
        str(item["id"]): PaymentMethod(
            id=str(item["id"]),
            enabled=bool(item.get("enabled", True)),
            title=str(item["title"]),
            note=str(item.get("note", "")).strip(),
            provider_token=str(item.get("provider_token", "")).strip(),
        )
        for item in raw.get("payment_methods", [])
    }

    bank_accounts = [
        BankAccount(
            id=str(item["id"]),
            title=str(item["title"]),
            holder_name=str(item["holder_name"]),
            card_number=str(item["card_number"]),
            account_number=str(item.get("account_number", "")),
            sheba=str(item.get("sheba", "")),
        )
        for item in raw.get("bank_accounts", [])
    ]

    # Backward-compatible: support old `ton_wallets` key if `usdt_wallets` is absent.
    raw_wallets = raw.get("usdt_wallets")
    if raw_wallets is None:
        raw_wallets = raw.get("ton_wallets", [])

    usdt_wallets = [
        UsdtWallet(
            id=str(item["id"]),
            title=str(item["title"]),
            network=str(item.get("network", "TRC20")),
            wallet_address=str(item["wallet_address"]),
            memo=str(item.get("memo", "")),
        )
        for item in raw_wallets
    ]

    return SalesCatalog(
        version=int(raw.get("version", 1)),
        rules=rules,
        packages=packages,
        payment_methods=payment_methods,
        bank_accounts=bank_accounts,
        usdt_wallets=usdt_wallets,
        usdt_to_toman_rate=int(raw.get("usdt_to_toman_rate", 0)),
        usdt_rate_channel=str(raw.get("usdt_rate_channel", "")).strip(),
        usdt_rate_cache_sec=int(raw.get("usdt_rate_cache_sec", 180)),
        usdt_rate_refresh_hour_local=int(raw.get("usdt_rate_refresh_hour_local", 11)),
        usdt_rate_timezone=str(raw.get("usdt_rate_timezone", "Asia/Tehran")).strip() or "Asia/Tehran",
        support_contact_id=str(raw.get("support_contact_id", "")).strip(),
        support_message=str(
            raw.get(
                "support_message",
                "برای پیگیری خرید، خطای پرداخت یا سوالات مالی می‌توانید با پشتیبانی فندق در ارتباط باشید.",
            )
        ).strip(),
        pay_support_text=str(raw.get("pay_support_text", "برای پیگیری پرداخت با پشتیبانی فروش در ارتباط باشید.")).strip(),
    )
