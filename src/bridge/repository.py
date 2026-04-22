from __future__ import annotations

import json
from collections.abc import Sequence
from contextlib import asynccontextmanager

import aiosqlite

from bridge.types import ContactEntry, ContentType, DeliveryStatus, OutboxItem, Platform, User, UserState
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
        note: str | None,
    ) -> int:
        now = utc_iso()
        async with self._connect() as conn:
            cur = await conn.execute(
                """
                INSERT INTO admin_requests(
                    requester_user_id, target_platform, target_identifier,
                    note, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    requester_user_id,
                    target_platform.value,
                    target_identifier,
                    note,
                    "OPEN",
                    now,
                ),
            )
            await conn.commit()
            return int(cur.lastrowid)

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
