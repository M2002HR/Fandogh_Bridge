from __future__ import annotations

from datetime import timedelta

import aiosqlite
import pytest

from bridge.db import init_db
from bridge.repository import Repository
from bridge.types import ContentType, DeliveryStatus, Platform
from bridge.utils import utc_now


@pytest.mark.asyncio
async def test_session_and_block_logic(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))

    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    u1 = await repo.upsert_user(Platform.TELEGRAM, "100", "100", "alice", "Alice")
    u2 = await repo.upsert_user(Platform.BALE, "200", "200", "bob", "Bob")

    await repo.set_active_session(u1.id, u2.id)
    target = await repo.get_active_target(u1.id)
    assert target is not None
    assert target.id == u2.id

    await repo.add_block(u2.id, u1.id)
    assert await repo.is_blocked(u2.id, u1.id) is True

    await repo.remove_block(u2.id, u1.id)
    assert await repo.is_blocked(u2.id, u1.id) is False

    await repo.clear_active_session(u1.id)
    assert await repo.get_active_target(u1.id) is None


@pytest.mark.asyncio
async def test_outbox_due_and_retry_fields(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))

    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)
    src = await repo.upsert_user(Platform.TELEGRAM, "100", "100", "a", "A")
    dst = await repo.upsert_user(Platform.BALE, "200", "200", "b", "B")

    now = utc_now()
    await repo.enqueue_outbox(
        source_user_id=src.id,
        dest_user_id=dst.id,
        content_type=ContentType.TEXT,
        text="hello",
        source_file_id=None,
        source_file_platform=None,
        caption=None,
        next_retry_at=(now - timedelta(seconds=1)).isoformat(),
        expires_at=(now + timedelta(hours=1)).isoformat(),
    )

    due = await repo.fetch_due_outbox(now.isoformat())
    assert len(due) == 1
    assert due[0].content_type == ContentType.TEXT
    assert due[0].text == "hello"


@pytest.mark.asyncio
async def test_audit_event_insert_and_cleanup_old_logs(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    src = await repo.upsert_user(Platform.TELEGRAM, "300", "300", "s", "Source")
    dst = await repo.upsert_user(Platform.BALE, "400", "400", "d", "Dest")

    await repo.log_message(
        source_user_id=src.id,
        dest_user_id=dst.id,
        source_platform=Platform.TELEGRAM,
        dest_platform=Platform.BALE,
        content_type=ContentType.TEXT,
        status=DeliveryStatus.SENT,
        error=None,
    )
    await repo.log_audit_event(
        event_type="delivery.sent",
        status="SENT",
        platform=Platform.TELEGRAM,
        user_id=src.id,
        chat_id=src.chat_id,
        target_user_id=dst.id,
        message_id=123,
        text_raw="hello",
        payload={"content_type": "TEXT", "mode": "test"},
    )

    async with aiosqlite.connect(db_file) as conn:
        audit_row = await (await conn.execute("SELECT COUNT(1) FROM audit_events")).fetchone()
        msg_row = await (await conn.execute("SELECT COUNT(1) FROM message_log")).fetchone()
    assert int(audit_row[0]) == 1
    assert int(msg_row[0]) == 1

    audit_deleted, message_deleted = await repo.cleanup_old_logs((utc_now() + timedelta(days=1)).isoformat())
    assert audit_deleted == 1
    assert message_deleted == 1

    audit_deleted2, message_deleted2 = await repo.cleanup_old_logs((utc_now() + timedelta(days=1)).isoformat())
    assert audit_deleted2 == 0
    assert message_deleted2 == 0
