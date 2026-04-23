from __future__ import annotations

import pytest

from bridge.db import init_db
from bridge.repository import Repository
from bridge.types import Platform


@pytest.mark.asyncio
async def test_registration_state_and_contact_flow(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))

    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    a = await repo.upsert_user_presence(Platform.TELEGRAM, "10", "10", "alice", "Alice")
    b = await repo.upsert_user_presence(Platform.BALE, "20", "20", "bob", "Bob")

    await repo.mark_terms_accepted(a.id)
    await repo.complete_registration(a.id, "+989121234567")
    await repo.mark_terms_accepted(b.id)
    await repo.complete_registration(b.id, "09123334444")

    a2 = await repo.get_user_by_id(a.id)
    b2 = await repo.get_user_by_id(b.id)
    assert a2 is not None and a2.is_registered is True
    assert b2 is not None and b2.is_registered is True

    by_phone = await repo.find_registered_users_by_identifier("09123334444", target_platform=Platform.BALE)
    assert len(by_phone) == 1
    assert by_phone[0].id == b.id

    await repo.set_user_state(a.id, "X", {"k": 1})
    st = await repo.get_user_state(a.id)
    assert st is not None
    assert st.state == "X"
    assert st.data["k"] == 1

    contact = await repo.add_contact(a.id, b.id, "دوست")
    listed = await repo.list_contacts(a.id)
    assert len(listed) == 1
    assert listed[0].id == contact.id

    await repo.delete_contact(a.id, contact.id)
    assert await repo.get_contact(a.id, contact.id) is None


@pytest.mark.asyncio
async def test_claim_update_is_idempotent(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    first = await repo.claim_update(Platform.BALE, 123)
    second = await repo.claim_update(Platform.BALE, 123)
    third = await repo.claim_update(Platform.BALE, 124)

    assert first is True
    assert second is False
    assert third is True
