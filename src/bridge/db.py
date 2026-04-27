from __future__ import annotations

import aiosqlite

from bridge.db_url import ParsedDbUrl, parse_db_url

try:
    import aiomysql  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - handled at runtime when mysql backend is selected
    aiomysql = None


SQLITE_SCHEMA_SQL = """
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

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    platform TEXT,
    user_id INTEGER,
    chat_id TEXT,
    target_user_id INTEGER,
    message_id INTEGER,
    text_raw TEXT,
    payload_json TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_events_created_at
ON audit_events(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_events_user_created
ON audit_events(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_events_type_created
ON audit_events(event_type, created_at DESC);

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


MYSQL_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        platform VARCHAR(32) NOT NULL,
        platform_user_id VARCHAR(128) NOT NULL,
        chat_id VARCHAR(128) NOT NULL,
        bridge_id VARCHAR(64) NOT NULL,
        username VARCHAR(255) NULL,
        display_name VARCHAR(255) NULL,
        phone_number VARCHAR(64) NULL,
        is_registered TINYINT NOT NULL DEFAULT 0,
        terms_accepted_at VARCHAR(40) NULL,
        registration_completed_at VARCHAR(40) NULL,
        created_at VARCHAR(40) NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        UNIQUE KEY uq_users_bridge_id (bridge_id),
        UNIQUE KEY uq_users_platform_user (platform, platform_user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS active_sessions (
        user_id BIGINT NOT NULL PRIMARY KEY,
        target_user_id BIGINT NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        CONSTRAINT fk_active_sessions_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        CONSTRAINT fk_active_sessions_target FOREIGN KEY (target_user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS blocks (
        blocker_user_id BIGINT NOT NULL,
        blocked_user_id BIGINT NOT NULL,
        created_at VARCHAR(40) NOT NULL,
        PRIMARY KEY (blocker_user_id, blocked_user_id),
        CONSTRAINT fk_blocks_blocker FOREIGN KEY (blocker_user_id) REFERENCES users(id) ON DELETE CASCADE,
        CONSTRAINT fk_blocks_blocked FOREIGN KEY (blocked_user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS contacts (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        owner_user_id BIGINT NOT NULL,
        target_user_id BIGINT NOT NULL,
        alias VARCHAR(255) NOT NULL,
        created_at VARCHAR(40) NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        UNIQUE KEY uq_contacts_owner_alias (owner_user_id, alias),
        UNIQUE KEY uq_contacts_owner_target (owner_user_id, target_user_id),
        CONSTRAINT fk_contacts_owner FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE CASCADE,
        CONSTRAINT fk_contacts_target FOREIGN KEY (target_user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS user_states (
        user_id BIGINT NOT NULL PRIMARY KEY,
        state VARCHAR(128) NOT NULL,
        state_data LONGTEXT NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        CONSTRAINT fk_user_states_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS admin_requests (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        requester_user_id BIGINT NOT NULL,
        target_platform VARCHAR(32) NOT NULL,
        target_identifier VARCHAR(255) NOT NULL,
        target_phone VARCHAR(64) NULL,
        target_username VARCHAR(255) NULL,
        note TEXT NULL,
        status VARCHAR(32) NOT NULL,
        matched_user_id BIGINT NULL,
        matched_at VARCHAR(40) NULL,
        created_at VARCHAR(40) NOT NULL,
        KEY idx_admin_requests_lookup (status, target_platform, target_phone, target_username),
        CONSTRAINT fk_admin_requests_requester FOREIGN KEY (requester_user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS message_log (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        source_user_id BIGINT NOT NULL,
        dest_user_id BIGINT NOT NULL,
        source_platform VARCHAR(32) NOT NULL,
        dest_platform VARCHAR(32) NOT NULL,
        content_type VARCHAR(32) NOT NULL,
        status VARCHAR(32) NOT NULL,
        error TEXT NULL,
        created_at VARCHAR(40) NOT NULL,
        KEY idx_message_log_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_events (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        event_type VARCHAR(128) NOT NULL,
        status VARCHAR(32) NOT NULL,
        platform VARCHAR(32) NULL,
        user_id BIGINT NULL,
        chat_id VARCHAR(128) NULL,
        target_user_id BIGINT NULL,
        message_id BIGINT NULL,
        text_raw LONGTEXT NULL,
        payload_json LONGTEXT NULL,
        created_at VARCHAR(40) NOT NULL,
        KEY idx_audit_events_created_at (created_at),
        KEY idx_audit_events_user_created (user_id, created_at),
        KEY idx_audit_events_type_created (event_type, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS outbox (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        source_user_id BIGINT NOT NULL,
        dest_user_id BIGINT NOT NULL,
        content_type VARCHAR(32) NOT NULL,
        text LONGTEXT NULL,
        source_file_id VARCHAR(255) NULL,
        source_file_platform VARCHAR(32) NULL,
        caption LONGTEXT NULL,
        attempts INT NOT NULL DEFAULT 0,
        status VARCHAR(32) NOT NULL,
        next_retry_at VARCHAR(40) NOT NULL,
        expires_at VARCHAR(40) NOT NULL,
        last_error TEXT NULL,
        created_at VARCHAR(40) NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        KEY idx_outbox_due (status, next_retry_at),
        CONSTRAINT fk_outbox_source FOREIGN KEY (source_user_id) REFERENCES users(id) ON DELETE CASCADE,
        CONSTRAINT fk_outbox_dest FOREIGN KEY (dest_user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS message_read_receipts (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        reader_user_id BIGINT NOT NULL,
        source_user_id BIGINT NOT NULL,
        source_message_id BIGINT NOT NULL,
        created_at VARCHAR(40) NOT NULL,
        UNIQUE KEY uq_message_read (reader_user_id, source_user_id, source_message_id),
        CONSTRAINT fk_message_read_reader FOREIGN KEY (reader_user_id) REFERENCES users(id) ON DELETE CASCADE,
        CONSTRAINT fk_message_read_source FOREIGN KEY (source_user_id) REFERENCES users(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS processed_updates (
        platform VARCHAR(32) NOT NULL,
        update_id BIGINT NOT NULL,
        created_at VARCHAR(40) NOT NULL,
        PRIMARY KEY (platform, update_id),
        KEY idx_processed_updates_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS credit_wallets (
        identity_key VARCHAR(190) NOT NULL PRIMARY KEY,
        text_units_remaining INT NOT NULL DEFAULT 0,
        voice_minutes_remaining INT NOT NULL DEFAULT 0,
        photo_count_remaining INT NOT NULL DEFAULT 0,
        updated_at VARCHAR(40) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS credit_ledger (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        identity_key VARCHAR(190) NOT NULL,
        user_id BIGINT NULL,
        entry_type VARCHAR(64) NOT NULL,
        text_units_delta INT NOT NULL,
        voice_minutes_delta INT NOT NULL,
        photo_count_delta INT NOT NULL,
        package_id VARCHAR(128) NULL,
        payment_order_id BIGINT NULL,
        note TEXT NULL,
        created_at VARCHAR(40) NOT NULL,
        KEY idx_credit_ledger_identity (identity_key, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS usdt_rates (
        date_local VARCHAR(32) NOT NULL PRIMARY KEY,
        rate_toman INT NOT NULL,
        source VARCHAR(255) NULL,
        raw_text TEXT NULL,
        fetched_at VARCHAR(40) NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        KEY idx_usdt_rates_fetched_at (fetched_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS payment_orders (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        requester_user_id BIGINT NOT NULL,
        beneficiary_user_id BIGINT NULL,
        identity_key VARCHAR(190) NOT NULL,
        package_id VARCHAR(128) NOT NULL,
        payment_method VARCHAR(64) NOT NULL,
        status VARCHAR(32) NOT NULL,
        amount_usd DOUBLE NOT NULL,
        amount_stars INT NULL,
        invoice_payload VARCHAR(255) NULL,
        telegram_charge_id VARCHAR(255) NULL,
        provider_charge_id VARCHAR(255) NULL,
        receipt_file_id VARCHAR(255) NULL,
        receipt_file_platform VARCHAR(32) NULL,
        receipt_caption TEXT NULL,
        account_id VARCHAR(255) NULL,
        admin_channel_chat_id VARCHAR(128) NULL,
        admin_channel_message_id BIGINT NULL,
        approved_by_platform VARCHAR(32) NULL,
        approved_by_user_id VARCHAR(128) NULL,
        approval_note TEXT NULL,
        created_at VARCHAR(40) NOT NULL,
        updated_at VARCHAR(40) NOT NULL,
        UNIQUE KEY uq_payment_orders_invoice_payload (invoice_payload),
        UNIQUE KEY uq_payment_orders_telegram_charge (telegram_charge_id),
        CONSTRAINT fk_payment_orders_requester FOREIGN KEY (requester_user_id) REFERENCES users(id) ON DELETE CASCADE,
        CONSTRAINT fk_payment_orders_beneficiary FOREIGN KEY (beneficiary_user_id) REFERENCES users(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
]


async def init_db(db_url_or_path: str) -> None:
    parsed = parse_db_url(db_url_or_path)
    if parsed.backend == "sqlite":
        await _init_sqlite(parsed.sqlite_path or "")
        return
    await _init_mysql(parsed)


async def _init_sqlite(db_path: str) -> None:
    async with aiosqlite.connect(db_path) as conn:
        await conn.executescript(SQLITE_SCHEMA_SQL)
        await _migrate_users_table(conn)
        await _migrate_admin_requests_table(conn)
        await _migrate_usdt_rates_table(conn)
        await _migrate_payment_orders_table(conn)
        await _ensure_sqlite_indexes(conn)
        await conn.commit()


async def _init_mysql(parsed: ParsedDbUrl) -> None:
    if aiomysql is None:  # pragma: no cover
        raise RuntimeError("aiomysql is required for mysql backend. Install dependencies again.")

    if not parsed.database:
        raise ValueError("DB_URL for mysql backend must include a database name")

    conn = None
    try:
        conn = await aiomysql.connect(
            host=parsed.host,
            port=int(parsed.port or 3306),
            user=parsed.user,
            password=parsed.password,
            db=parsed.database,
            autocommit=True,
            charset="utf8mb4",
        )
    except aiomysql.OperationalError as exc:
        code = int(exc.args[0]) if exc.args else 0
        # 1049 = unknown database
        if code != 1049:
            raise
        admin_conn = await aiomysql.connect(
            host=parsed.host,
            port=int(parsed.port or 3306),
            user=parsed.user,
            password=parsed.password,
            autocommit=True,
            charset="utf8mb4",
        )
        try:
            async with admin_conn.cursor() as cur:
                await cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{parsed.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
        finally:
            admin_conn.close()
        conn = await aiomysql.connect(
            host=parsed.host,
            port=int(parsed.port or 3306),
            user=parsed.user,
            password=parsed.password,
            db=parsed.database,
            autocommit=True,
            charset="utf8mb4",
        )

    if conn is None:
        raise RuntimeError("Failed to initialize MySQL connection")
    try:
        async with conn.cursor() as cur:
            # Suppress harmless "already exists" warnings on startup.
            await cur.execute("SET sql_notes = 0")
            try:
                for stmt in MYSQL_SCHEMA_STATEMENTS:
                    await cur.execute(stmt)
            finally:
                await cur.execute("SET sql_notes = 1")
    finally:
        conn.close()


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


async def _ensure_sqlite_indexes(conn: aiosqlite.Connection) -> None:
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_registered ON users(is_registered, platform)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_phone ON users(phone_number)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_message_log_created_at ON message_log(created_at DESC)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events(created_at DESC)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_user_created ON audit_events(user_id, created_at DESC)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_type_created ON audit_events(event_type, created_at DESC)")
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
