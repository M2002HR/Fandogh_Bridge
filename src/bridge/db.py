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
    target_phone TEXT,
    target_username TEXT,
    note TEXT,
    status TEXT NOT NULL,
    matched_user_id INTEGER,
    matched_at TEXT,
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

CREATE TABLE IF NOT EXISTS message_read_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reader_user_id INTEGER NOT NULL,
    source_user_id INTEGER NOT NULL,
    source_message_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(reader_user_id, source_user_id, source_message_id),
    FOREIGN KEY(reader_user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(source_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS processed_updates (
    platform TEXT NOT NULL,
    update_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(platform, update_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_updates_created
ON processed_updates(created_at);

CREATE TABLE IF NOT EXISTS credit_wallets (
    identity_key TEXT PRIMARY KEY,
    text_units_remaining INTEGER NOT NULL DEFAULT 0,
    voice_minutes_remaining INTEGER NOT NULL DEFAULT 0,
    photo_count_remaining INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS credit_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_key TEXT NOT NULL,
    user_id INTEGER,
    entry_type TEXT NOT NULL,
    text_units_delta INTEGER NOT NULL,
    voice_minutes_delta INTEGER NOT NULL,
    photo_count_delta INTEGER NOT NULL,
    package_id TEXT,
    payment_order_id INTEGER,
    note TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_credit_ledger_identity
ON credit_ledger(identity_key, created_at);

CREATE TABLE IF NOT EXISTS usdt_rates (
    date_local TEXT PRIMARY KEY,
    rate_toman INTEGER NOT NULL,
    source TEXT,
    raw_text TEXT,
    fetched_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_usdt_rates_fetched_at
ON usdt_rates(fetched_at DESC);

CREATE TABLE IF NOT EXISTS payment_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    requester_user_id INTEGER NOT NULL,
    beneficiary_user_id INTEGER,
    identity_key TEXT NOT NULL,
    package_id TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    status TEXT NOT NULL,
    amount_usd REAL NOT NULL,
    amount_stars INTEGER,
    invoice_payload TEXT,
    telegram_charge_id TEXT,
    provider_charge_id TEXT,
    receipt_file_id TEXT,
    receipt_file_platform TEXT,
    receipt_caption TEXT,
    account_id TEXT,
    admin_channel_chat_id TEXT,
    admin_channel_message_id INTEGER,
    approved_by_platform TEXT,
    approved_by_user_id TEXT,
    approval_note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(requester_user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(beneficiary_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_orders_invoice_payload
ON payment_orders(invoice_payload)
WHERE invoice_payload IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_orders_telegram_charge
ON payment_orders(telegram_charge_id)
WHERE telegram_charge_id IS NOT NULL;
"""


async def init_db(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as conn:
        await conn.executescript(SCHEMA_SQL)
        await _migrate_users_table(conn)
        await _migrate_admin_requests_table(conn)
        await _migrate_usdt_rates_table(conn)
        await _migrate_payment_orders_table(conn)
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
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_requests_lookup ON admin_requests(status, target_platform, target_phone, target_username)"
    )


async def _migrate_admin_requests_table(conn: aiosqlite.Connection) -> None:
    cursor = await conn.execute("PRAGMA table_info(admin_requests)")
    try:
        rows = await cursor.fetchall()
    finally:
        await cursor.close()
    existing = {row[1] for row in rows}

    required = {
        "target_phone": "ALTER TABLE admin_requests ADD COLUMN target_phone TEXT",
        "target_username": "ALTER TABLE admin_requests ADD COLUMN target_username TEXT",
        "matched_user_id": "ALTER TABLE admin_requests ADD COLUMN matched_user_id INTEGER",
        "matched_at": "ALTER TABLE admin_requests ADD COLUMN matched_at TEXT",
    }

    for col, ddl in required.items():
        if col not in existing:
            await conn.execute(ddl)


async def _migrate_payment_orders_table(conn: aiosqlite.Connection) -> None:
    cursor = await conn.execute("PRAGMA table_info(payment_orders)")
    try:
        rows = await cursor.fetchall()
    finally:
        await cursor.close()
    existing = {row[1] for row in rows}

    required = {
        "beneficiary_user_id": "ALTER TABLE payment_orders ADD COLUMN beneficiary_user_id INTEGER",
    }
    for col, ddl in required.items():
        if col not in existing:
            await conn.execute(ddl)


async def _migrate_usdt_rates_table(conn: aiosqlite.Connection) -> None:
    cursor = await conn.execute("PRAGMA table_info(usdt_rates)")
    try:
        rows = await cursor.fetchall()
    finally:
        await cursor.close()
    if not rows:
        return
    existing = {row[1] for row in rows}

    required = {
        "source": "ALTER TABLE usdt_rates ADD COLUMN source TEXT",
        "raw_text": "ALTER TABLE usdt_rates ADD COLUMN raw_text TEXT",
        "updated_at": "ALTER TABLE usdt_rates ADD COLUMN updated_at TEXT",
    }
    for col, ddl in required.items():
        if col not in existing:
            await conn.execute(ddl)
