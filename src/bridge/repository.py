from __future__ import annotations

import json
from collections.abc import Sequence
from contextlib import asynccontextmanager

import aiosqlite

from bridge.types import (
    CreditWallet,
    ContactEntry,
    ContentType,
    DeliveryStatus,
    OutboxItem,
    PaymentOrder,
    Platform,
    User,
    UserState,
    UsdtRateRecord,
)
from bridge.utils import generate_bridge_id, normalize_phone, normalize_username, utc_iso


class Repository:
    def __init__(self, db_path: str, bridge_id_prefix: str, bridge_id_length: int) -> None:
        self.db_path = db_path
        self.bridge_id_prefix = bridge_id_prefix
        self.bridge_id_length = bridge_id_length

    @asynccontextmanager
    async def _connect(self):
        conn = await aiosqlite.connect(self.db_path)
        conn.row_factory = aiosqlite.Row
        try:
            yield conn
        finally:
            await conn.close()

    async def get_user_by_platform_user(self, platform: Platform, platform_user_id: str) -> User | None:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                """
                SELECT * FROM users
                WHERE platform = ? AND platform_user_id = ?
                """,
                (platform.value, platform_user_id),
            )
            return _row_to_user(row)

    async def get_user_by_bridge_id(self, bridge_id: str) -> User | None:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                """
                SELECT * FROM users
                WHERE bridge_id = ?
                """,
                (bridge_id.upper(),),
            )
            return _row_to_user(row)

    async def get_user_by_id(self, user_id: int) -> User | None:
        async with self._connect() as conn:
            row = await _fetchone(conn, "SELECT * FROM users WHERE id = ?", (user_id,))
            return _row_to_user(row)

    async def upsert_user_presence(
        self,
        platform: Platform,
        platform_user_id: str,
        chat_id: str,
        username: str | None,
        display_name: str | None,
    ) -> User:
        now = utc_iso()
        username = normalize_username(username)

        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                "SELECT id FROM users WHERE platform = ? AND platform_user_id = ?",
                (platform.value, platform_user_id),
            )
            if row:
                await conn.execute(
                    """
                    UPDATE users
                    SET chat_id = ?, username = ?, display_name = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (chat_id, username, display_name, now, int(row["id"])),
                )
                user_id = int(row["id"])
            else:
                bridge_id = await self._new_unique_bridge_id(conn)
                cur = await conn.execute(
                    """
                    INSERT INTO users (
                        platform, platform_user_id, chat_id, bridge_id,
                        username, display_name, created_at, updated_at,
                        is_registered
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        platform.value,
                        platform_user_id,
                        chat_id,
                        bridge_id,
                        username,
                        display_name,
                        now,
                        now,
                    ),
                )
                user_id = int(cur.lastrowid)
            await conn.commit()

        user = await self.get_user_by_id(user_id)
        if not user:
            raise RuntimeError("Failed to upsert user presence")
        return user

    async def upsert_user(
        self,
        platform: Platform,
        platform_user_id: str,
        chat_id: str,
        username: str | None,
        display_name: str | None,
    ) -> User:
        # Backward-compatible wrapper used by older tests/code paths.
        return await self.upsert_user_presence(platform, platform_user_id, chat_id, username, display_name)

    async def mark_terms_accepted(self, user_id: int) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                UPDATE users
                SET terms_accepted_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (now, now, user_id),
            )
            await conn.commit()

    async def complete_registration(self, user_id: int, phone_number: str) -> None:
        now = utc_iso()
        normalized_phone = normalize_phone(phone_number)
        if not normalized_phone:
            raise ValueError("Invalid phone number")

        async with self._connect() as conn:
            await conn.execute(
                """
                UPDATE users
                SET phone_number = ?, is_registered = 1,
                    registration_completed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (normalized_phone, now, now, user_id),
            )
            await conn.commit()

    async def find_registered_users_by_identifier(
        self,
        identifier: str,
        target_platform: Platform | None = None,
    ) -> list[User]:
        identifier = identifier.strip()
        if not identifier:
            return []

        platform_filter_sql = ""
        params: list[str] = []
        if target_platform:
            platform_filter_sql = " AND platform = ?"
            params.append(target_platform.value)

        async with self._connect() as conn:
            if identifier.upper().startswith(self.bridge_id_prefix.upper()):
                sql = f"SELECT * FROM users WHERE is_registered = 1 AND bridge_id = ?{platform_filter_sql}"
                rows = await _fetchall(conn, sql, (identifier.upper(), *params))
                return [_row_to_user(r) for r in rows if r is not None]

            normalized_phone = normalize_phone(identifier)
            if normalized_phone:
                sql = f"SELECT * FROM users WHERE is_registered = 1 AND phone_number = ?{platform_filter_sql}"
                rows = await _fetchall(conn, sql, (normalized_phone, *params))
                return [_row_to_user(r) for r in rows if r is not None]

            normalized_user = normalize_username(identifier)
            if normalized_user:
                sql = f"SELECT * FROM users WHERE is_registered = 1 AND username = ?{platform_filter_sql}"
                rows = await _fetchall(conn, sql, (normalized_user, *params))
                return [_row_to_user(r) for r in rows if r is not None]

        return []

    async def set_active_session(self, user_id: int, target_user_id: int) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO active_sessions(user_id, target_user_id, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id)
                DO UPDATE SET target_user_id = excluded.target_user_id, updated_at = excluded.updated_at
                """,
                (user_id, target_user_id, now),
            )
            await conn.commit()

    async def clear_active_session(self, user_id: int) -> None:
        async with self._connect() as conn:
            await conn.execute("DELETE FROM active_sessions WHERE user_id = ?", (user_id,))
            await conn.commit()

    async def get_active_target(self, user_id: int) -> User | None:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                """
                SELECT u.*
                FROM active_sessions s
                JOIN users u ON u.id = s.target_user_id
                WHERE s.user_id = ?
                """,
                (user_id,),
            )
            return _row_to_user(row)

    async def add_block(self, blocker_user_id: int, blocked_user_id: int) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                INSERT OR REPLACE INTO blocks(blocker_user_id, blocked_user_id, created_at)
                VALUES (?, ?, ?)
                """,
                (blocker_user_id, blocked_user_id, now),
            )
            await conn.commit()

    async def remove_block(self, blocker_user_id: int, blocked_user_id: int) -> None:
        async with self._connect() as conn:
            await conn.execute(
                "DELETE FROM blocks WHERE blocker_user_id = ? AND blocked_user_id = ?",
                (blocker_user_id, blocked_user_id),
            )
            await conn.commit()

    async def is_blocked(self, blocker_user_id: int, blocked_user_id: int) -> bool:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                """
                SELECT 1 FROM blocks
                WHERE blocker_user_id = ? AND blocked_user_id = ?
                LIMIT 1
                """,
                (blocker_user_id, blocked_user_id),
            )
            return row is not None

    async def add_contact(self, owner_user_id: int, target_user_id: int, alias: str) -> ContactEntry:
        alias_clean = alias.strip()
        if not alias_clean:
            raise ValueError("alias is required")
        now = utc_iso()

        async with self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO contacts(owner_user_id, target_user_id, alias, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(owner_user_id, target_user_id)
                DO UPDATE SET alias = excluded.alias, updated_at = excluded.updated_at
                """,
                (owner_user_id, target_user_id, alias_clean, now, now),
            )
            await conn.commit()

            row = await _fetchone(
                conn,
                """
                SELECT * FROM contacts
                WHERE owner_user_id = ? AND target_user_id = ?
                """,
                (owner_user_id, target_user_id),
            )

        if not row:
            raise RuntimeError("Failed to add contact")
        return _row_to_contact(row)

    async def list_contacts(self, owner_user_id: int) -> list[ContactEntry]:
        async with self._connect() as conn:
            rows = await _fetchall(
                conn,
                """
                SELECT * FROM contacts
                WHERE owner_user_id = ?
                ORDER BY updated_at DESC
                """,
                (owner_user_id,),
            )
            return [_row_to_contact(r) for r in rows]

    async def get_contact(self, owner_user_id: int, contact_id: int) -> ContactEntry | None:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                "SELECT * FROM contacts WHERE owner_user_id = ? AND id = ?",
                (owner_user_id, contact_id),
            )
            return _row_to_contact(row) if row else None

    async def delete_contact(self, owner_user_id: int, contact_id: int) -> None:
        async with self._connect() as conn:
            await conn.execute(
                "DELETE FROM contacts WHERE owner_user_id = ? AND id = ?",
                (owner_user_id, contact_id),
            )
            await conn.commit()

    async def get_user_state(self, user_id: int) -> UserState | None:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                "SELECT state, state_data FROM user_states WHERE user_id = ?",
                (user_id,),
            )
            if not row:
                return None
            data = json.loads(row["state_data"] or "{}")
            return UserState(state=str(row["state"]), data=data)

    async def set_user_state(self, user_id: int, state: str, data: dict) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO user_states(user_id, state, state_data, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id)
                DO UPDATE SET state = excluded.state, state_data = excluded.state_data, updated_at = excluded.updated_at
                """,
                (user_id, state, json.dumps(data, ensure_ascii=False), now),
            )
            await conn.commit()

    async def clear_user_state(self, user_id: int) -> None:
        async with self._connect() as conn:
            await conn.execute("DELETE FROM user_states WHERE user_id = ?", (user_id,))
            await conn.commit()

    async def create_admin_request(
        self,
        requester_user_id: int,
        target_platform: Platform,
        target_identifier: str,
        target_phone: str | None,
        target_username: str | None,
        note: str | None,
    ) -> int:
        now = utc_iso()
        async with self._connect() as conn:
            cur = await conn.execute(
                """
                INSERT INTO admin_requests(
                    requester_user_id, target_platform, target_identifier,
                    target_phone, target_username, note, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    requester_user_id,
                    target_platform.value,
                    target_identifier,
                    normalize_phone(target_phone),
                    normalize_username(target_username),
                    note,
                    "OPEN",
                    now,
                ),
            )
            await conn.commit()
            return int(cur.lastrowid)

    async def find_open_admin_requests(
        self,
        *,
        target_platform: Platform,
        target_phone: str | None,
        target_username: str | None,
    ) -> list[tuple[int, int]]:
        phone = normalize_phone(target_phone)
        username = normalize_username(target_username)
        clauses: list[str] = []
        params: list[str] = [target_platform.value, "OPEN"]
        if phone:
            clauses.append("(target_phone = ? OR target_identifier LIKE ?)")
            params.append(phone)
            params.append(f"%{phone}%")
        if username:
            clauses.append("(target_username = ? OR target_identifier LIKE ?)")
            params.append(username)
            params.append(f"%{username}%")
        if not clauses:
            return []

        where_match = " OR ".join(clauses)
        async with self._connect() as conn:
            rows = await _fetchall(
                conn,
                f"""
                SELECT id, requester_user_id
                FROM admin_requests
                WHERE target_platform = ?
                  AND status = ?
                  AND ({where_match})
                ORDER BY id ASC
                """,
                tuple(params),
            )
            return [(int(row["id"]), int(row["requester_user_id"])) for row in rows]

    async def claim_message_read_receipt(self, reader_user_id: int, source_user_id: int, source_message_id: int) -> bool:
        if source_message_id <= 0:
            return False
        now = utc_iso()
        async with self._connect() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO message_read_receipts(
                        reader_user_id, source_user_id, source_message_id, created_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (reader_user_id, source_user_id, source_message_id, now),
                )
                await conn.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def mark_admin_request_matched(self, request_id: int, matched_user_id: int) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                UPDATE admin_requests
                SET status = 'MATCHED',
                    matched_user_id = ?,
                    matched_at = ?
                WHERE id = ? AND status = 'OPEN'
                """,
                (matched_user_id, now, request_id),
            )
            await conn.commit()

    async def list_users_by_phone(self, phone_number: str) -> list[User]:
        normalized_phone = normalize_phone(phone_number)
        if not normalized_phone:
            return []
        async with self._connect() as conn:
            rows = await _fetchall(
                conn,
                "SELECT * FROM users WHERE is_registered = 1 AND phone_number = ? ORDER BY platform, id",
                (normalized_phone,),
            )
            return [_row_to_user(row) for row in rows if row is not None]

    async def get_wallet(self, identity_key: str) -> CreditWallet:
        now = utc_iso()
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                "SELECT * FROM credit_wallets WHERE identity_key = ?",
                (identity_key,),
            )
            if row is None:
                await conn.execute(
                    """
                    INSERT INTO credit_wallets(
                        identity_key, text_units_remaining, voice_minutes_remaining, photo_count_remaining, updated_at
                    ) VALUES (?, 0, 0, 0, ?)
                    """,
                    (identity_key, now),
                )
                await conn.commit()
                row = await _fetchone(
                    conn,
                    "SELECT * FROM credit_wallets WHERE identity_key = ?",
                    (identity_key,),
                )
        wallet = _row_to_wallet(row)
        if wallet is None:
            raise RuntimeError("Failed to load wallet")
        return wallet

    async def has_credit_entry(self, identity_key: str, entry_type: str) -> bool:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                """
                SELECT 1 FROM credit_ledger
                WHERE identity_key = ? AND entry_type = ?
                LIMIT 1
                """,
                (identity_key, entry_type),
            )
            return row is not None

    async def has_payment_credit_entry(self, payment_order_id: int) -> bool:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                """
                SELECT 1 FROM credit_ledger
                WHERE payment_order_id = ?
                LIMIT 1
                """,
                (payment_order_id,),
            )
            return row is not None

    async def apply_credit_delta(
        self,
        *,
        identity_key: str,
        user_id: int | None,
        entry_type: str,
        text_units_delta: int,
        voice_minutes_delta: int,
        photo_count_delta: int,
        package_id: str | None,
        payment_order_id: int | None,
        note: str | None,
    ) -> CreditWallet:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO credit_wallets(
                    identity_key, text_units_remaining, voice_minutes_remaining, photo_count_remaining, updated_at
                ) VALUES (?, 0, 0, 0, ?)
                ON CONFLICT(identity_key) DO NOTHING
                """,
                (identity_key, now),
            )
            await conn.execute(
                """
                UPDATE credit_wallets
                SET text_units_remaining = text_units_remaining + ?,
                    voice_minutes_remaining = voice_minutes_remaining + ?,
                    photo_count_remaining = photo_count_remaining + ?,
                    updated_at = ?
                WHERE identity_key = ?
                """,
                (text_units_delta, voice_minutes_delta, photo_count_delta, now, identity_key),
            )
            await conn.execute(
                """
                INSERT INTO credit_ledger(
                    identity_key, user_id, entry_type,
                    text_units_delta, voice_minutes_delta, photo_count_delta,
                    package_id, payment_order_id, note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    identity_key,
                    user_id,
                    entry_type,
                    text_units_delta,
                    voice_minutes_delta,
                    photo_count_delta,
                    package_id,
                    payment_order_id,
                    note,
                    now,
                ),
            )
            await conn.commit()
            row = await _fetchone(conn, "SELECT * FROM credit_wallets WHERE identity_key = ?", (identity_key,))
        wallet = _row_to_wallet(row)
        if wallet is None:
            raise RuntimeError("Failed to load wallet after update")
        return wallet

    async def consume_credits(
        self,
        *,
        identity_key: str,
        user_id: int,
        text_units: int,
        voice_minutes: int,
        photo_count: int,
        note: str,
    ) -> CreditWallet | None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO credit_wallets(
                    identity_key, text_units_remaining, voice_minutes_remaining, photo_count_remaining, updated_at
                ) VALUES (?, 0, 0, 0, ?)
                ON CONFLICT(identity_key) DO NOTHING
                """,
                (identity_key, now),
            )
            cur = await conn.execute(
                """
                UPDATE credit_wallets
                SET text_units_remaining = text_units_remaining - ?,
                    voice_minutes_remaining = voice_minutes_remaining - ?,
                    photo_count_remaining = photo_count_remaining - ?,
                    updated_at = ?
                WHERE identity_key = ?
                  AND text_units_remaining >= ?
                  AND voice_minutes_remaining >= ?
                  AND photo_count_remaining >= ?
                """,
                (
                    text_units,
                    voice_minutes,
                    photo_count,
                    now,
                    identity_key,
                    text_units,
                    voice_minutes,
                    photo_count,
                ),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return None
            await conn.execute(
                """
                INSERT INTO credit_ledger(
                    identity_key, user_id, entry_type,
                    text_units_delta, voice_minutes_delta, photo_count_delta,
                    package_id, payment_order_id, note, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    identity_key,
                    user_id,
                    "USAGE",
                    -text_units,
                    -voice_minutes,
                    -photo_count,
                    None,
                    None,
                    note,
                    now,
                ),
            )
            await conn.commit()
            row = await _fetchone(conn, "SELECT * FROM credit_wallets WHERE identity_key = ?", (identity_key,))
        return _row_to_wallet(row)

    async def upsert_usdt_rate(
        self,
        *,
        date_local: str,
        rate_toman: int,
        source: str | None,
        raw_text: str | None,
        fetched_at: str,
    ) -> UsdtRateRecord:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO usdt_rates(date_local, rate_toman, source, raw_text, fetched_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date_local)
                DO UPDATE SET
                    rate_toman = excluded.rate_toman,
                    source = excluded.source,
                    raw_text = excluded.raw_text,
                    fetched_at = excluded.fetched_at,
                    updated_at = excluded.updated_at
                """,
                (date_local, rate_toman, source, raw_text, fetched_at, now),
            )
            await conn.commit()
            row = await _fetchone(conn, "SELECT * FROM usdt_rates WHERE date_local = ?", (date_local,))
        record = _row_to_usdt_rate(row)
        if record is None:
            raise RuntimeError("Failed to upsert usdt rate")
        return record

    async def get_usdt_rate_by_date(self, date_local: str) -> UsdtRateRecord | None:
        async with self._connect() as conn:
            row = await _fetchone(conn, "SELECT * FROM usdt_rates WHERE date_local = ?", (date_local,))
            return _row_to_usdt_rate(row)

    async def get_latest_usdt_rate(self) -> UsdtRateRecord | None:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                """
                SELECT * FROM usdt_rates
                ORDER BY date_local DESC
                LIMIT 1
                """,
            )
            return _row_to_usdt_rate(row)

    async def create_payment_order(
        self,
        *,
        requester_user_id: int,
        beneficiary_user_id: int,
        identity_key: str,
        package_id: str,
        payment_method: str,
        status: str,
        amount_usd: float,
        amount_stars: int | None,
        invoice_payload: str | None,
        account_id: str | None,
        receipt_file_id: str | None,
        receipt_file_platform: Platform | None,
        receipt_caption: str | None,
    ) -> PaymentOrder:
        now = utc_iso()
        async with self._connect() as conn:
            cur = await conn.execute(
                """
                INSERT INTO payment_orders(
                    requester_user_id, beneficiary_user_id, identity_key, package_id, payment_method, status,
                    amount_usd, amount_stars, invoice_payload, account_id,
                    receipt_file_id, receipt_file_platform, receipt_caption,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    requester_user_id,
                    beneficiary_user_id,
                    identity_key,
                    package_id,
                    payment_method,
                    status,
                    amount_usd,
                    amount_stars,
                    invoice_payload,
                    account_id,
                    receipt_file_id,
                    receipt_file_platform.value if receipt_file_platform else None,
                    receipt_caption,
                    now,
                    now,
                ),
            )
            order_id = int(cur.lastrowid)
            await conn.commit()
            row = await _fetchone(conn, "SELECT * FROM payment_orders WHERE id = ?", (order_id,))
        order = _row_to_payment_order(row)
        if order is None:
            raise RuntimeError("Failed to create payment order")
        return order

    async def get_payment_order(self, order_id: int) -> PaymentOrder | None:
        async with self._connect() as conn:
            row = await _fetchone(conn, "SELECT * FROM payment_orders WHERE id = ?", (order_id,))
            return _row_to_payment_order(row)

    async def list_payment_orders_for_user(self, user_id: int, *, limit: int = 10, offset: int = 0) -> list[PaymentOrder]:
        async with self._connect() as conn:
            rows = await _fetchall(
                conn,
                """
                SELECT * FROM payment_orders
                WHERE requester_user_id = ? OR beneficiary_user_id = ?
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (user_id, user_id, limit, offset),
            )
            return [item for item in (_row_to_payment_order(row) for row in rows) if item is not None]

    async def count_payment_orders_for_user(self, user_id: int) -> int:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                """
                SELECT COUNT(1) AS cnt
                FROM payment_orders
                WHERE requester_user_id = ? OR beneficiary_user_id = ?
                """,
                (user_id, user_id),
            )
            return int(row["cnt"]) if row and row["cnt"] is not None else 0

    async def get_payment_order_by_invoice_payload(self, invoice_payload: str) -> PaymentOrder | None:
        async with self._connect() as conn:
            row = await _fetchone(
                conn,
                "SELECT * FROM payment_orders WHERE invoice_payload = ?",
                (invoice_payload,),
            )
            return _row_to_payment_order(row)

    async def set_payment_order_admin_message(self, order_id: int, chat_id: str, message_id: int) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                UPDATE payment_orders
                SET admin_channel_chat_id = ?, admin_channel_message_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (chat_id, message_id, now, order_id),
            )
            await conn.commit()

    async def mark_manual_payment_approved(
        self,
        *,
        order_id: int,
        approved_by_platform: Platform,
        approved_by_user_id: str,
        approval_note: str | None,
    ) -> PaymentOrder | None:
        now = utc_iso()
        async with self._connect() as conn:
            cur = await conn.execute(
                """
                UPDATE payment_orders
                SET status = 'APPROVED',
                    approved_by_platform = ?,
                    approved_by_user_id = ?,
                    approval_note = ?,
                    updated_at = ?
                WHERE id = ? AND status IN ('PENDING_MANUAL', 'PENDING_REVIEW')
                """,
                (approved_by_platform.value, approved_by_user_id, approval_note, now, order_id),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return None
            await conn.commit()
            row = await _fetchone(conn, "SELECT * FROM payment_orders WHERE id = ?", (order_id,))
            return _row_to_payment_order(row)

    async def mark_manual_payment_rejected(
        self,
        *,
        order_id: int,
        approved_by_platform: Platform,
        approved_by_user_id: str,
        approval_note: str | None,
    ) -> PaymentOrder | None:
        now = utc_iso()
        async with self._connect() as conn:
            cur = await conn.execute(
                """
                UPDATE payment_orders
                SET status = 'REJECTED',
                    approved_by_platform = ?,
                    approved_by_user_id = ?,
                    approval_note = ?,
                    updated_at = ?
                WHERE id = ? AND status IN ('PENDING_MANUAL', 'PENDING_REVIEW')
                """,
                (approved_by_platform.value, approved_by_user_id, approval_note, now, order_id),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return None
            await conn.commit()
            row = await _fetchone(conn, "SELECT * FROM payment_orders WHERE id = ?", (order_id,))
            return _row_to_payment_order(row)

    async def mark_invoice_payment_received(
        self,
        *,
        invoice_payload: str,
        charge_id: str,
        provider_charge_id: str | None,
        expected_methods: tuple[str, ...],
    ) -> PaymentOrder | None:
        if not expected_methods:
            return None
        now = utc_iso()
        method_placeholders = ", ".join(["?"] * len(expected_methods))
        params: tuple = (
            "PENDING_REVIEW",
            charge_id,
            provider_charge_id,
            now,
            invoice_payload,
            *expected_methods,
            "INVOICE_SENT",
            "PENDING_STARS",
        )
        async with self._connect() as conn:
            cur = await conn.execute(
                f"""
                UPDATE payment_orders
                SET status = ?,
                    telegram_charge_id = ?,
                    provider_charge_id = ?,
                    updated_at = ?
                WHERE invoice_payload = ?
                  AND payment_method IN ({method_placeholders})
                  AND status IN (?, ?)
                """,
                params,
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return None
            await conn.commit()
            row = await _fetchone(conn, "SELECT * FROM payment_orders WHERE invoice_payload = ?", (invoice_payload,))
            return _row_to_payment_order(row)

    async def fetch_payment_orders_for_method(
        self,
        *,
        payment_method: str,
        statuses: tuple[str, ...],
        limit: int = 100,
    ) -> list[PaymentOrder]:
        if not statuses:
            return []
        placeholders = ", ".join(["?"] * len(statuses))
        params: tuple = (payment_method, *statuses, limit)
        async with self._connect() as conn:
            rows = await _fetchall(
                conn,
                f"""
                SELECT * FROM payment_orders
                WHERE payment_method = ?
                  AND status IN ({placeholders})
                ORDER BY id ASC
                LIMIT ?
                """,
                params,
            )
            return [item for item in (_row_to_payment_order(row) for row in rows) if item is not None]

    async def mark_external_payment_approved(
        self,
        *,
        order_id: int,
        external_charge_id: str | None,
        provider_charge_id: str | None,
        approval_note: str | None,
    ) -> PaymentOrder | None:
        now = utc_iso()
        async with self._connect() as conn:
            cur = await conn.execute(
                """
                UPDATE payment_orders
                SET status = 'APPROVED',
                    telegram_charge_id = COALESCE(?, telegram_charge_id),
                    provider_charge_id = COALESCE(?, provider_charge_id),
                    approval_note = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'INVOICE_SENT'
                """,
                (external_charge_id, provider_charge_id, approval_note, now, order_id),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return None
            await conn.commit()
            row = await _fetchone(conn, "SELECT * FROM payment_orders WHERE id = ?", (order_id,))
            return _row_to_payment_order(row)

    async def mark_external_payment_rejected(
        self,
        *,
        order_id: int,
        approval_note: str | None,
    ) -> PaymentOrder | None:
        now = utc_iso()
        async with self._connect() as conn:
            cur = await conn.execute(
                """
                UPDATE payment_orders
                SET status = 'REJECTED',
                    approval_note = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'INVOICE_SENT'
                """,
                (approval_note, now, order_id),
            )
            if cur.rowcount != 1:
                await conn.rollback()
                return None
            await conn.commit()
            row = await _fetchone(conn, "SELECT * FROM payment_orders WHERE id = ?", (order_id,))
            return _row_to_payment_order(row)

    async def claim_update(self, platform: Platform, update_id: int) -> bool:
        now = utc_iso()
        async with self._connect() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO processed_updates(platform, update_id, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (platform.value, update_id, now),
                )
                await conn.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    async def log_message(
        self,
        source_user_id: int,
        dest_user_id: int,
        source_platform: Platform,
        dest_platform: Platform,
        content_type: ContentType,
        status: DeliveryStatus,
        error: str | None,
    ) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO message_log(
                    source_user_id, dest_user_id, source_platform, dest_platform,
                    content_type, status, error, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_user_id,
                    dest_user_id,
                    source_platform.value,
                    dest_platform.value,
                    content_type.value,
                    status.value,
                    error,
                    now,
                ),
            )
            await conn.commit()

    async def enqueue_outbox(
        self,
        *,
        source_user_id: int,
        dest_user_id: int,
        content_type: ContentType,
        text: str | None,
        source_file_id: str | None,
        source_file_platform: Platform | None,
        caption: str | None,
        next_retry_at: str,
        expires_at: str,
    ) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO outbox(
                    source_user_id, dest_user_id, content_type, text,
                    source_file_id, source_file_platform, caption,
                    attempts, status, next_retry_at, expires_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
                """,
                (
                    source_user_id,
                    dest_user_id,
                    content_type.value,
                    text,
                    source_file_id,
                    source_file_platform.value if source_file_platform else None,
                    caption,
                    DeliveryStatus.PENDING.value,
                    next_retry_at,
                    expires_at,
                    now,
                    now,
                ),
            )
            await conn.commit()

    async def fetch_due_outbox(self, now_iso: str, limit: int = 100) -> Sequence[OutboxItem]:
        async with self._connect() as conn:
            rows = await _fetchall(
                conn,
                """
                SELECT
                    id, source_user_id, dest_user_id, content_type,
                    text, source_file_id, source_file_platform,
                    caption, attempts, next_retry_at, expires_at
                FROM outbox
                WHERE status IN (?, ?) AND next_retry_at <= ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (DeliveryStatus.PENDING.value, DeliveryStatus.FAILED.value, now_iso, limit),
            )
            return [_row_to_outbox(item) for item in rows]

    async def mark_outbox_sent(self, outbox_id: int) -> None:
        async with self._connect() as conn:
            await conn.execute("DELETE FROM outbox WHERE id = ?", (outbox_id,))
            await conn.commit()

    async def mark_outbox_retry(self, outbox_id: int, attempts: int, next_retry_at: str, error: str) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                UPDATE outbox
                SET attempts = ?, status = ?, next_retry_at = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (attempts, DeliveryStatus.FAILED.value, next_retry_at, error, now, outbox_id),
            )
            await conn.commit()

    async def mark_outbox_expired(self, outbox_id: int, error: str) -> None:
        now = utc_iso()
        async with self._connect() as conn:
            await conn.execute(
                """
                UPDATE outbox
                SET status = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (DeliveryStatus.EXPIRED.value, error, now, outbox_id),
            )
            await conn.commit()

    async def _new_unique_bridge_id(self, conn: aiosqlite.Connection) -> str:
        for _ in range(100):
            candidate = generate_bridge_id(self.bridge_id_prefix, self.bridge_id_length)
            row = await _fetchone(conn, "SELECT 1 FROM users WHERE bridge_id = ? LIMIT 1", (candidate,))
            if not row:
                return candidate
        raise RuntimeError("Unable to generate unique bridge_id")


def _row_to_user(row: aiosqlite.Row | None) -> User | None:
    if row is None:
        return None
    return User(
        id=int(row["id"]),
        platform=Platform(row["platform"]),
        platform_user_id=str(row["platform_user_id"]),
        chat_id=str(row["chat_id"]),
        bridge_id=str(row["bridge_id"]),
        username=row["username"],
        display_name=row["display_name"],
        phone_number=row["phone_number"] if "phone_number" in row.keys() else None,
        is_registered=bool(row["is_registered"]) if "is_registered" in row.keys() else False,
        terms_accepted_at=row["terms_accepted_at"] if "terms_accepted_at" in row.keys() else None,
        registration_completed_at=row["registration_completed_at"] if "registration_completed_at" in row.keys() else None,
    )


def _row_to_contact(row: aiosqlite.Row) -> ContactEntry:
    return ContactEntry(
        id=int(row["id"]),
        owner_user_id=int(row["owner_user_id"]),
        target_user_id=int(row["target_user_id"]),
        alias=str(row["alias"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _row_to_outbox(row: aiosqlite.Row) -> OutboxItem:
    return OutboxItem(
        id=int(row["id"]),
        source_user_id=int(row["source_user_id"]),
        dest_user_id=int(row["dest_user_id"]),
        content_type=ContentType(row["content_type"]),
        text=row["text"],
        source_file_id=row["source_file_id"],
        source_file_platform=Platform(row["source_file_platform"]) if row["source_file_platform"] else None,
        caption=row["caption"],
        attempts=int(row["attempts"]),
        next_retry_at=str(row["next_retry_at"]),
        expires_at=str(row["expires_at"]),
    )


def _row_to_wallet(row: aiosqlite.Row | None) -> CreditWallet | None:
    if row is None:
        return None
    return CreditWallet(
        identity_key=str(row["identity_key"]),
        text_units_remaining=int(row["text_units_remaining"]),
        voice_minutes_remaining=int(row["voice_minutes_remaining"]),
        photo_count_remaining=int(row["photo_count_remaining"]),
        updated_at=str(row["updated_at"]),
    )


def _row_to_payment_order(row: aiosqlite.Row | None) -> PaymentOrder | None:
    if row is None:
        return None
    return PaymentOrder(
        id=int(row["id"]),
        requester_user_id=int(row["requester_user_id"]),
        beneficiary_user_id=int(row["beneficiary_user_id"]) if row["beneficiary_user_id"] is not None else int(row["requester_user_id"]),
        identity_key=str(row["identity_key"]),
        package_id=str(row["package_id"]),
        payment_method=str(row["payment_method"]),
        status=str(row["status"]),
        amount_usd=float(row["amount_usd"]),
        amount_stars=int(row["amount_stars"]) if row["amount_stars"] is not None else None,
        invoice_payload=row["invoice_payload"],
        telegram_charge_id=row["telegram_charge_id"],
        provider_charge_id=row["provider_charge_id"],
        receipt_file_id=row["receipt_file_id"],
        receipt_file_platform=Platform(row["receipt_file_platform"]) if row["receipt_file_platform"] else None,
        receipt_caption=row["receipt_caption"],
        account_id=row["account_id"],
        admin_channel_chat_id=row["admin_channel_chat_id"],
        admin_channel_message_id=int(row["admin_channel_message_id"]) if row["admin_channel_message_id"] is not None else None,
        approved_by_platform=Platform(row["approved_by_platform"]) if row["approved_by_platform"] else None,
        approved_by_user_id=row["approved_by_user_id"],
        approval_note=row["approval_note"],
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _row_to_usdt_rate(row: aiosqlite.Row | None) -> UsdtRateRecord | None:
    if row is None:
        return None
    return UsdtRateRecord(
        date_local=str(row["date_local"]),
        rate_toman=int(row["rate_toman"]),
        source=row["source"],
        raw_text=row["raw_text"],
        fetched_at=str(row["fetched_at"]),
    )


async def _fetchone(conn: aiosqlite.Connection, sql: str, params: tuple | None = None):
    cursor = await conn.execute(sql, params or ())
    try:
        return await cursor.fetchone()
    finally:
        await cursor.close()


async def _fetchall(conn: aiosqlite.Connection, sql: str, params: tuple | None = None):
    cursor = await conn.execute(sql, params or ())
    try:
        return await cursor.fetchall()
    finally:
        await cursor.close()
