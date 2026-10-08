import asyncio
import sqlite3
import threading
from dataclasses import replace
from datetime import timedelta

import pytest

import beeper_events_sidecar.db as db_module
from beeper_events_sidecar.db import Database
from beeper_events_sidecar.identity import SourceIdentity
from beeper_events_sidecar.models import (
    SourceEvent,
    Subscription,
    isoformat_z,
    utc_now,
)


def _subscription(now: str) -> Subscription:
    return Subscription(
        id="sub_test",
        principal="test",
        name="message.created",
        arguments={},
        callback_url="https://example.com/callback",
        secret="whsec_" + "YQ" * 24,
        previous_secret=None,
        previous_secret_expires_at=None,
        expires_at=None,
        active=True,
        created_at=now,
    )


def _event(timestamp: str, message_id: str = "m1") -> SourceEvent:
    return SourceEvent(
        source_key=SourceIdentity("beeper", "default", "a", "c", message_id).source_key,
        source_event_id=SourceIdentity("beeper", "default", "a", "c", message_id).source_event_id,
        name="message.created",
        occurred_at=timestamp,
        account_id="a",
        chat_id="c",
        local_chat_id="42",
        network="WhatsApp",
        message_id=message_id,
        sender_id="sender",
        sender_name="Alice",
        discovered_via="test",
    )


@pytest.mark.asyncio
async def test_async_db_calls_do_not_block_loop_and_are_serialized(tmp_path):
    db = Database(tmp_path / "state.sqlite3")
    db.initialize()
    active = 0
    max_active = 0
    state_lock = threading.Lock()
    first_started = threading.Event()
    second_started = threading.Event()
    release = threading.Event()

    def blocking_work(started: threading.Event) -> None:
        nonlocal active, max_active
        with state_lock:
            active += 1
            max_active = max(max_active, active)
        started.set()
        release.wait(timeout=1.0)
        with state_lock:
            active -= 1

    first = asyncio.create_task(db.run_async(blocking_work, first_started))
    await asyncio.wait_for(asyncio.to_thread(first_started.wait), timeout=1.0)

    second = asyncio.create_task(db.run_async(blocking_work, second_started))
    heartbeat = asyncio.create_task(asyncio.sleep(0.02))
    await asyncio.wait_for(heartbeat, timeout=0.2)

    assert not second_started.is_set()
    release.set()
    await asyncio.wait_for(asyncio.gather(first, second), timeout=1.0)
    assert max_active == 1


def test_database_context_closes_connections(tmp_path, monkeypatch):
    real_connect = sqlite3.connect
    connections = []

    def tracking_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        connections.append(conn)
        return conn

    monkeypatch.setattr(db_module.sqlite3, "connect", tracking_connect)

    db = Database(tmp_path / "state.sqlite3")
    db.initialize()
    db.stats(isoformat_z(utc_now()))

    assert connections
    for conn in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            conn.execute("SELECT 1")


def test_subscription_activation_rolls_back_if_backfill_fails(
    tmp_path,
    monkeypatch,
):
    db = Database(tmp_path / "state.sqlite3")
    db.initialize()
    now_text = isoformat_z(utc_now())
    subscription = _subscription(now_text)

    def fail_backfill(*_args, **_kwargs):
        raise RuntimeError("backfill failed")

    monkeypatch.setattr(
        db,
        "_enqueue_existing_events_for_subscription",
        fail_backfill,
    )

    with pytest.raises(RuntimeError, match="backfill failed"):
        db.activate_subscription(
            subscription,
            now=now_text,
            backfill_existing=True,
        )

    assert db.get_subscription(subscription.id) is None


def test_event_insert_is_idempotent_and_enqueues_once(tmp_path):
    db = Database(tmp_path / "state.sqlite3")
    db.initialize()
    now = utc_now()
    now_text = isoformat_z(now)
    db.upsert_subscription(_subscription(now_text), now=now_text)

    event = _event(isoformat_z(now + timedelta(seconds=1)))
    assert db.record_event_and_enqueue(
        event, now=isoformat_z(now + timedelta(seconds=2))
    )
    assert not db.record_event_and_enqueue(
        event, now=isoformat_z(now + timedelta(seconds=3))
    )

    stats = db.stats(isoformat_z(now + timedelta(seconds=4)))
    assert stats["source_events_seen"] == 1
    assert stats["pending_deliveries"] == 1


def test_duplicate_event_enrichment_can_create_previously_missed_delivery(
    tmp_path,
):
    db = Database(tmp_path / "state.sqlite3")
    db.initialize()
    now = utc_now()
    now_text = isoformat_z(now)
    subscription = replace(
        _subscription(now_text),
        id="sub_sender",
        arguments={"sender_ids": ["sender"]},
    )
    db.upsert_subscription(subscription, now=now_text)

    occurred_at = isoformat_z(now + timedelta(seconds=1))
    incomplete = SourceEvent(
        source_key=SourceIdentity("beeper", "default", "a", "c", "m-enriched").source_key,
        source_event_id=SourceIdentity("beeper", "default", "a", "c", "m-enriched").source_event_id,
        name="message.created",
        occurred_at=occurred_at,
        account_id="a",
        chat_id="c",
        local_chat_id=None,
        network=None,
        message_id="m-enriched",
        sender_id=None,
        sender_name=None,
        discovered_via="websocket",
    )
    assert db.record_event_and_enqueue(
        incomplete, now=isoformat_z(now + timedelta(seconds=2))
    )
    assert db.stats(isoformat_z(now + timedelta(seconds=2)))[
        "pending_deliveries"
    ] == 0

    enriched = replace(
        incomplete,
        local_chat_id="42",
        network="WhatsApp",
        sender_id="sender",
        sender_name="Alice",
        discovered_via="reconciliation",
    )
    assert not db.record_event_and_enqueue(
        enriched, now=isoformat_z(now + timedelta(seconds=3))
    )

    deliveries = db.get_due_deliveries(
        isoformat_z(now + timedelta(seconds=4))
    )
    assert len(deliveries) == 1
    recovered = deliveries[0].source_event
    assert recovered.sender_id == "sender"
    assert recovered.sender_name == "Alice"
    assert recovered.local_chat_id == "42"
    assert recovered.network == "WhatsApp"


def test_pre_subscription_message_is_recorded_but_not_delivered(tmp_path):
    db = Database(tmp_path / "state.sqlite3")
    db.initialize()
    now = utc_now()
    now_text = isoformat_z(now)
    db.upsert_subscription(_subscription(now_text), now=now_text)

    event = _event(isoformat_z(now - timedelta(seconds=1)))
    assert db.record_event_and_enqueue(event, now=now_text)

    stats = db.stats(now_text)
    assert stats["source_events_seen"] == 1
    assert stats["pending_deliveries"] == 0
