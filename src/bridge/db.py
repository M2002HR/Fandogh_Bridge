from __future__ import annotations

import aiosqlite

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    platform_user_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    bridge_id TEXT NOT NULL UNIQUE,
    username TEXT,
    display_name TEXT,
    phone_number TEXT,
    is_registered INTEGER NOT NULL DEFAULT 0,
    terms_accepted_at TEXT,
    registration_completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(platform, platform_user_id)
);

CREATE TABLE IF NOT EXISTS active_sessions (
    user_id INTEGER PRIMARY KEY,
    target_user_id INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(target_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS blocks (
    blocker_user_id INTEGER NOT NULL,
    blocked_user_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(blocker_user_id, blocked_user_id),
    FOREIGN KEY(blocker_user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(blocked_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_user_id INTEGER NOT NULL,
    target_user_id INTEGER NOT NULL,
    alias TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(target_user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(owner_user_id, alias),
    UNIQUE(owner_user_id, target_user_id)
);

CREATE TABLE IF NOT EXISTS user_states (
    user_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    state_data TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS admin_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    requester_user_id INTEGER NOT NULL,
    target_platform TEXT NOT NULL,
    target_identifier TEXT NOT NULL,
    note TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(requester_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS message_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_user_id INTEGER NOT NULL,
    dest_user_id INTEGER NOT NULL,
    source_platform TEXT NOT NULL,
    dest_platform TEXT NOT NULL,
    content_type TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_user_id INTEGER NOT NULL,
    dest_user_id INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    text TEXT,
    source_file_id TEXT,
    source_file_platform TEXT,
    caption TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    next_retry_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(source_user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(dest_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_outbox_due
ON outbox(status, next_retry_at);
"""


async def init_db(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as conn:
        await conn.executescript(SCHEMA_SQL)
        await _migrate_users_table(conn)
        await _ensure_indexes(conn)
        await conn.commit()


async def _migrate_users_table(conn: aiosqlite.Connection) -> None:
    cursor = await conn.execute("PRAGMA table_info(users)")
    try:
        rows = await cursor.fetchall()
    finally:
        await cursor.close()
    existing = {row[1] for row in rows}

    required = {
        "phone_number": "ALTER TABLE users ADD COLUMN phone_number TEXT",
        "is_registered": "ALTER TABLE users ADD COLUMN is_registered INTEGER NOT NULL DEFAULT 0",
        "terms_accepted_at": "ALTER TABLE users ADD COLUMN terms_accepted_at TEXT",
        "registration_completed_at": "ALTER TABLE users ADD COLUMN registration_completed_at TEXT",
    }

    for col, ddl in required.items():
        if col not in existing:
            await conn.execute(ddl)


async def _ensure_indexes(conn: aiosqlite.Connection) -> None:
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_registered ON users(is_registered, platform)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_phone ON users(phone_number)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
