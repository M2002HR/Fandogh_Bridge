from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
from pathlib import Path

import aiomysql

from bridge.db import init_db
from bridge.db_url import parse_db_url

TABLE_ORDER = [
    "users",
    "active_sessions",
    "blocks",
    "contacts",
    "user_states",
    "admin_requests",
    "message_log",
    "audit_events",
    "outbox",
    "message_read_receipts",
    "processed_updates",
    "credit_wallets",
    "credit_ledger",
    "usdt_rates",
    "payment_orders",
]


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row[1]) for row in rows]


async def _migrate(sqlite_path: str, mysql_url: str, truncate: bool) -> None:
    parsed = parse_db_url(mysql_url)
    if parsed.backend != "mysql":
        raise ValueError("Target DB_URL must be mysql+aiomysql://...")

    source = Path(sqlite_path)
    if not source.exists():
        raise FileNotFoundError(f"SQLite source not found: {source}")

    await init_db(mysql_url)

    sqlite_conn = sqlite3.connect(str(source))
    sqlite_conn.row_factory = sqlite3.Row

    mysql_conn = await aiomysql.connect(
        host=parsed.host,
        port=int(parsed.port or 3306),
        user=parsed.user,
        password=parsed.password,
        db=parsed.database,
        autocommit=False,
        charset="utf8mb4",
    )

    try:
        async with mysql_conn.cursor() as cur:
            await cur.execute("SET FOREIGN_KEY_CHECKS=0")
            if truncate:
                for table in reversed(TABLE_ORDER):
                    await cur.execute(f"DELETE FROM `{table}`")

            for table in TABLE_ORDER:
                cols = _sqlite_columns(sqlite_conn, table)
                if not cols:
                    continue
                rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
                if not rows:
                    continue

                placeholders = ", ".join(["%s"] * len(cols))
                columns_sql = ", ".join([f"`{c}`" for c in cols])
                insert_sql = f"INSERT INTO `{table}` ({columns_sql}) VALUES ({placeholders})"
                values = [tuple(row[c] for c in cols) for row in rows]
                await cur.executemany(insert_sql, values)
                print(f"[OK] {table}: migrated {len(values)} rows")

            await cur.execute("SET FOREIGN_KEY_CHECKS=1")
        await mysql_conn.commit()
    except Exception:
        await mysql_conn.rollback()
        raise
    finally:
        mysql_conn.close()
        sqlite_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Fandogh data from SQLite to MySQL.")
    parser.add_argument("--sqlite", default="./app_data/bridge.db", help="Source sqlite file path")
    parser.add_argument("--mysql-url", default=os.getenv("DB_URL", ""), help="Target mysql+aiomysql DB URL")
    parser.add_argument("--no-truncate", action="store_true", help="Do not clear target tables before insert")
    args = parser.parse_args()

    if not args.mysql_url:
        raise ValueError("--mysql-url is required (or set DB_URL in environment)")

    asyncio.run(_migrate(args.sqlite, args.mysql_url, truncate=not args.no_truncate))


if __name__ == "__main__":
    main()
