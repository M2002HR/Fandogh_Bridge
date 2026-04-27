from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import unquote, urlparse


@dataclass(slots=True)
class ParsedDbUrl:
    backend: str  # "sqlite" | "mysql"
    raw: str
    sqlite_path: str | None = None
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str | None = None


def parse_db_url(db_url_or_path: str) -> ParsedDbUrl:
    raw = (db_url_or_path or "").strip()
    if not raw:
        raise ValueError("DB_URL is empty")

    # Backward-compatible test/dev support: plain filesystem path => sqlite path.
    if "://" not in raw:
        return ParsedDbUrl(backend="sqlite", raw=raw, sqlite_path=raw)

    if raw.startswith("sqlite+aiosqlite:///"):
        return ParsedDbUrl(backend="sqlite", raw=raw, sqlite_path=raw.removeprefix("sqlite+aiosqlite:///"))

    if raw.startswith("mysql+aiomysql://"):
        parsed = urlparse(raw)
        database = (parsed.path or "").lstrip("/")
        if not parsed.hostname or not parsed.username or database == "":
            raise ValueError("Invalid mysql+aiomysql DB_URL; expected mysql+aiomysql://user:pass@host:port/dbname")
        return ParsedDbUrl(
            backend="mysql",
            raw=raw,
            host=parsed.hostname,
            port=int(parsed.port or 3306),
            user=unquote(parsed.username),
            password=unquote(parsed.password or ""),
            database=unquote(database),
        )

    raise ValueError("Unsupported DB_URL. Use sqlite+aiosqlite:///... or mysql+aiomysql://user:pass@host:port/db")
