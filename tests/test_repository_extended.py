from __future__ import annotations

from datetime import UTC, datetime

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


@pytest.mark.asyncio
async def test_wallet_credit_and_consume_flow(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    user = await repo.upsert_user_presence(Platform.TELEGRAM, "10", "10", "alice", "Alice")
    await repo.mark_terms_accepted(user.id)
    await repo.complete_registration(user.id, "09120000001")

    wallet = await repo.apply_credit_delta(
        identity_key="telegram:10",
        user_id=user.id,
        entry_type="STARTER",
        text_units_delta=5,
        voice_minutes_delta=3,
        photo_count_delta=2,
        package_id=None,
        payment_order_id=None,
        note="starter",
    )
    assert wallet.text_units_remaining == 5
    assert wallet.voice_minutes_remaining == 3
    assert wallet.photo_count_remaining == 2

    consumed = await repo.consume_credits(
        identity_key="telegram:10",
        user_id=user.id,
        text_units=2,
        voice_minutes=1,
        photo_count=1,
        note="usage:test",
    )
    assert consumed is not None
    assert consumed.text_units_remaining == 3
    assert consumed.voice_minutes_remaining == 2
    assert consumed.photo_count_remaining == 1

    insufficient = await repo.consume_credits(
        identity_key="telegram:10",
        user_id=user.id,
        text_units=10,
        voice_minutes=0,
        photo_count=0,
        note="usage:too-much",
    )
    assert insufficient is None


@pytest.mark.asyncio
async def test_payment_order_lifecycle_for_manual_and_stars(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    user = await repo.upsert_user_presence(Platform.TELEGRAM, "10", "10", "alice", "Alice")
    await repo.mark_terms_accepted(user.id)
    await repo.complete_registration(user.id, "09120000001")

    manual = await repo.create_payment_order(
        requester_user_id=user.id,
        beneficiary_user_id=user.id,
        identity_key="telegram:10",
        package_id="starter-100",
        payment_method="manual_bank_transfer",
        status="PENDING_MANUAL",
        amount_usd=5.0,
        amount_stars=250,
        invoice_payload=None,
        account_id="iran-main",
        receipt_file_id="file-1",
        receipt_file_platform=Platform.TELEGRAM,
        receipt_caption="receipt",
    )
    await repo.set_payment_order_admin_message(manual.id, "-1003790742908", 77)
    manual_fetched = await repo.get_payment_order(manual.id)
    assert manual_fetched is not None
    assert manual_fetched.admin_channel_chat_id == "-1003790742908"
    assert manual_fetched.admin_channel_message_id == 77

    approved = await repo.mark_manual_payment_approved(
        order_id=manual.id,
        approved_by_platform=Platform.TELEGRAM,
        approved_by_user_id="999",
        approval_note="approved",
    )
    assert approved is not None
    assert approved.status == "APPROVED"
    assert await repo.mark_manual_payment_approved(
        order_id=manual.id,
        approved_by_platform=Platform.TELEGRAM,
        approved_by_user_id="999",
        approval_note="approved-again",
    ) is None

    stars = await repo.create_payment_order(
        requester_user_id=user.id,
        beneficiary_user_id=user.id,
        identity_key="telegram:10",
        package_id="starter-100",
        payment_method="telegram_stars",
        status="INVOICE_SENT",
        amount_usd=5.0,
        amount_stars=250,
        invoice_payload="stars:starter-100:1:abc123",
        account_id=None,
        receipt_file_id=None,
        receipt_file_platform=None,
        receipt_caption=None,
    )
    by_payload = await repo.get_payment_order_by_invoice_payload("stars:starter-100:1:abc123")
    assert by_payload is not None
    assert by_payload.id == stars.id

    paid = await repo.mark_invoice_payment_received(
        invoice_payload="stars:starter-100:1:abc123",
        charge_id="tg-charge",
        provider_charge_id="provider-charge",
        expected_methods=("telegram_stars",),
    )
    assert paid is not None
    assert paid.status == "PENDING_REVIEW"
    assert paid.telegram_charge_id == "tg-charge"


@pytest.mark.asyncio
async def test_list_payment_orders_for_user_includes_requester_and_beneficiary(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    buyer = await repo.upsert_user_presence(Platform.TELEGRAM, "10", "10", "alice", "Alice")
    receiver = await repo.upsert_user_presence(Platform.BALE, "20", "20", "bob", "Bob")
    for item, phone in ((buyer, "09120000001"), (receiver, "09120000002")):
        await repo.mark_terms_accepted(item.id)
        await repo.complete_registration(item.id, phone)

    await repo.create_payment_order(
        requester_user_id=buyer.id,
        beneficiary_user_id=buyer.id,
        identity_key="telegram:10",
        package_id="starter-100",
        payment_method="telegram_stars",
        status="APPROVED",
        amount_usd=1.0,
        amount_stars=67,
        invoice_payload="p1",
        account_id=None,
        receipt_file_id=None,
        receipt_file_platform=None,
        receipt_caption=None,
    )
    gifted = await repo.create_payment_order(
        requester_user_id=buyer.id,
        beneficiary_user_id=receiver.id,
        identity_key="bale:20",
        package_id="standard-280",
        payment_method="manual_bank_transfer",
        status="PENDING_REVIEW",
        amount_usd=2.0,
        amount_stars=None,
        invoice_payload=None,
        account_id="iran-main",
        receipt_file_id=None,
        receipt_file_platform=None,
        receipt_caption=None,
    )

    buyer_count = await repo.count_payment_orders_for_user(buyer.id)
    receiver_count = await repo.count_payment_orders_for_user(receiver.id)
    assert buyer_count == 2
    assert receiver_count == 1

    buyer_orders = await repo.list_payment_orders_for_user(buyer.id, limit=10, offset=0)
    assert [o.id for o in buyer_orders] == [gifted.id, gifted.id - 1]

    receiver_orders = await repo.list_payment_orders_for_user(receiver.id, limit=10, offset=0)
    assert [o.id for o in receiver_orders] == [gifted.id]


@pytest.mark.asyncio
async def test_admin_request_match_lookup_and_resolution(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    requester = await repo.upsert_user_presence(Platform.TELEGRAM, "10", "10", "alice", "Alice")
    await repo.mark_terms_accepted(requester.id)
    await repo.complete_registration(requester.id, "09120000001")

    request_id = await repo.create_admin_request(
        requester_user_id=requester.id,
        target_platform=Platform.BALE,
        target_identifier="phone=09120000002 | username=@target_user",
        target_phone="09120000002",
        target_username="target_user",
        note="please notify",
    )
    matches = await repo.find_open_admin_requests(
        target_platform=Platform.BALE,
        target_phone="09120000002",
        target_username=None,
    )
    assert matches == [(request_id, requester.id)]

    await repo.mark_admin_request_matched(request_id, matched_user_id=99)
    matches_after = await repo.find_open_admin_requests(
        target_platform=Platform.BALE,
        target_phone="09120000002",
        target_username="target_user",
    )
    assert matches_after == []


@pytest.mark.asyncio
async def test_usdt_rate_store_and_latest_lookup(tmp_path) -> None:
    db_file = tmp_path / "bridge.db"
    await init_db(str(db_file))
    repo = Repository(str(db_file), bridge_id_prefix="FDG", bridge_id_length=10)

    today = datetime.now(UTC).date().isoformat()
    record = await repo.upsert_usdt_rate(
        date_local=today,
        rate_toman=153000,
        source="usd_iran",
        raw_text="تتر 153000",
        fetched_at=datetime.now(UTC).isoformat(),
    )
    assert record.rate_toman == 153000

    same_day = await repo.get_usdt_rate_by_date(today)
    assert same_day is not None
    assert same_day.source == "usd_iran"

    latest = await repo.get_latest_usdt_rate()
    assert latest is not None
    assert latest.date_local == today
