import asyncio
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

from beeper_events_sidecar.config import Settings
from beeper_events_sidecar.db import Database
from beeper_events_sidecar.models import (
    Subscription,
    isoformat_z,
    utc_now,
)
from beeper_events_sidecar.service import EventService


class FakeBeeper:
    def __init__(self, messages, hydrated=None):
        self.messages = messages
        self.hydrated = hydrated or {}

    async def search_messages(self, *, date_after, date_before):
        return list(self.messages)

    async def message(self, chat_id, message_id):
        return self.hydrated[(chat_id, message_id)]

    async def chat_metadata(self, chat_id):
        return {
            "id": chat_id,
            "localChatID": "84",
            "network": "WhatsApp",
        }


class UnusedSender:
    pass


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        host="127.0.0.1",
        port=23375,
        db_path=tmp_path / "state.sqlite3",
        beeper_base_url="http://127.0.0.1:23374",
        beeper_auth_file=tmp_path / "authorization",
        mcp_bearer_file=tmp_path / "mcp-bearer",
        reconcile_interval_seconds=60,
        reconcile_overlap_seconds=3600,
        reconnect_max_seconds=30,
        delivery_max_attempts=8,
        delivery_base_backoff_seconds=1,
        delivery_max_backoff_seconds=300,
        default_subscription_ttl_seconds=604800,
        secret_rotation_seconds=300,
        callback_verification_cache_seconds=600,
        callback_timeout_seconds=10,
        log_level="INFO",
    )


@pytest.mark.asyncio
async def test_reconciliation_recovers_missed_message_once(tmp_path):
    now = utc_now()
    message_time = isoformat_z(now - timedelta(minutes=1))
    message = {
        "id": "m-recovered",
        "accountID": "whatsapp",
        "chatID": "chat-a",
        "senderID": "sender-a",
        "senderName": "Alice",
        "timestamp": message_time,
        "isSender": False,
        "isDeleted": False,
    }

    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()

    subscription_created = isoformat_z(now - timedelta(minutes=10))
    subscription = Subscription(
        id="sub-test",
        principal="test",
        name="message.created",
        arguments={},
        callback_url="https://example.com/callback",
        secret="whsec_" + "eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHg=",
        previous_secret=None,
        previous_secret_expires_at=None,
        expires_at=None,
        active=True,
        created_at=subscription_created,
    )
    db.upsert_subscription(subscription, now=subscription_created)

    checkpoint = isoformat_z(now - timedelta(minutes=5))
    db.set_checkpoint("beeper_messages", checkpoint, checkpoint)

    service = EventService(
        settings=settings,
        db=db,
        beeper=FakeBeeper([message]),
        webhook_sender=UnusedSender(),
    )

    await service.reconcile_once()
    first = db.stats(isoformat_z(utc_now()))
    assert first["source_events_seen"] == 1
    assert first["pending_deliveries"] == 1

    await service.reconcile_once()
    second = db.stats(isoformat_z(utc_now()))
    assert second["source_events_seen"] == 1
    assert second["pending_deliveries"] == 1


@pytest.mark.asyncio
async def test_self_message_is_not_normalized(tmp_path):
    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    service = EventService(
        settings=settings,
        db=db,
        beeper=FakeBeeper([]),
        webhook_sender=UnusedSender(),
    )
    message = {
        "id": "m-self",
        "accountID": "whatsapp",
        "chatID": "chat-a",
        "timestamp": isoformat_z(utc_now()),
        "isSender": True,
    }
    assert (
        await service._normalize_message(
            message, discovered_via="test"
        )
        is None
    )


class StatusSender:
    def __init__(self, status: int):
        self.status = status
        self.event_ids = []

    async def send_event(self, **kwargs):
        self.event_ids.append(kwargs["event"]["eventId"])
        return self.status


def _delivery_fixture(tmp_path, sender):
    import base64

    from beeper_events_sidecar.models import SourceEvent, Subscription

    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    now = utc_now()
    now_text = isoformat_z(now)
    secret = "whsec_" + base64.b64encode(b"x" * 32).decode()
    subscription = Subscription(
        id="sub-delivery",
        principal="test",
        name="message.created",
        arguments={},
        callback_url="https://example.com/callback",
        secret=secret,
        previous_secret=None,
        previous_secret_expires_at=None,
        expires_at=None,
        active=True,
        created_at=isoformat_z(now - timedelta(minutes=1)),
    )
    db.upsert_subscription(subscription, now=now_text)
    event = SourceEvent(
        source_key="beeper:whatsapp:chat:message",
        source_event_id="src-delivery",
        name="message.created",
        occurred_at=now_text,
        account_id="whatsapp",
        chat_id="chat",
        local_chat_id=None,
        network="WhatsApp",
        message_id="message",
        sender_id="sender",
        sender_name="Alice",
        discovered_via="test",
    )
    assert db.record_event_and_enqueue(event, now=now_text)
    delivery = db.get_due_deliveries(now_text)[0]
    service = EventService(
        settings=settings,
        db=db,
        beeper=FakeBeeper([]),
        webhook_sender=sender,
    )
    return db, service, delivery, now_text


@pytest.mark.asyncio
async def test_transient_delivery_failure_retries_same_event_id(tmp_path):
    sender = StatusSender(500)
    db, service, delivery, now_text = _delivery_fixture(
        tmp_path, sender
    )

    await service._deliver(delivery)

    stats = db.stats(now_text)
    assert stats["pending_deliveries"] == 1
    assert stats["dead_letter_deliveries"] == 0
    assert sender.event_ids == [delivery.event_id]


@pytest.mark.asyncio
async def test_http_410_goes_directly_to_dead_letter(tmp_path):
    sender = StatusSender(410)
    db, service, delivery, now_text = _delivery_fixture(
        tmp_path, sender
    )

    await service._deliver(delivery)

    stats = db.stats(now_text)
    assert stats["pending_deliveries"] == 0
    assert stats["dead_letter_deliveries"] == 1
    assert sender.event_ids == [delivery.event_id]


@pytest.mark.asyncio
async def test_websocket_partial_entry_is_hydrated_before_delivery(tmp_path):
    import base64

    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    now = utc_now()
    now_text = isoformat_z(now)

    subscription = Subscription(
        id="sub-ws",
        principal="test",
        name="message.created",
        arguments={},
        callback_url="https://example.com/callback",
        secret="whsec_" + base64.b64encode(b"x" * 32).decode(),
        previous_secret=None,
        previous_secret_expires_at=None,
        expires_at=None,
        active=True,
        created_at=isoformat_z(now - timedelta(minutes=1)),
    )
    db.upsert_subscription(subscription, now=now_text)

    hydrated = {
        "id": "m-ws",
        "accountID": "whatsapp",
        "chatID": "chat-ws",
        "senderID": "sender-a",
        "senderName": "Alice",
        "timestamp": now_text,
        "isSender": False,
        "isDeleted": False,
    }
    beeper = FakeBeeper(
        [],
        hydrated={("chat-ws", "m-ws"): hydrated},
    )
    service = EventService(
        settings=settings,
        db=db,
        beeper=beeper,
        webhook_sender=UnusedSender(),
    )

    await service._ingest_ws_event(
        {
            "type": "message.upserted",
            "seq": 1,
            "ts": now_text,
            "chatID": "chat-ws",
            "ids": ["m-ws"],
            "entries": [{"id": "m-ws"}],
        }
    )

    stats = db.stats(isoformat_z(utc_now()))
    assert stats["source_events_seen"] == 1
    assert stats["pending_deliveries"] == 1


@pytest.mark.asyncio
async def test_websocket_unknown_sender_direction_defers_to_reconcile(
    tmp_path,
):
    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    now_text = isoformat_z(utc_now())
    hydrated = {
        "id": "m-unknown",
        "accountID": "whatsapp",
        "chatID": "chat-ws",
        "senderID": "sender-a",
        "timestamp": now_text,
        # Deliberately no isSender.
    }
    service = EventService(
        settings=settings,
        db=db,
        beeper=FakeBeeper(
            [],
            hydrated={("chat-ws", "m-unknown"): hydrated},
        ),
        webhook_sender=UnusedSender(),
    )

    await service._ingest_ws_event(
        {
            "type": "message.upserted",
            "seq": 1,
            "ts": now_text,
            "chatID": "chat-ws",
            "ids": ["m-unknown"],
        }
    )

    assert db.stats(now_text)["source_events_seen"] == 0
    assert service._reconcile_requested.is_set()


@pytest.mark.asyncio
async def test_delivery_loop_recovers_from_transient_sqlite_operational_error(
    tmp_path,
    monkeypatch,
):
    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    service = EventService(
        settings=settings,
        db=db,
        beeper=FakeBeeper([]),
        webhook_sender=UnusedSender(),
    )

    original_get_due_deliveries = db.get_due_deliveries
    calls = 0

    def flaky_get_due_deliveries(now, limit=20):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise sqlite3.OperationalError("database is locked")
        service._stop.set()
        return original_get_due_deliveries(now, limit=limit)

    monkeypatch.setattr(db, "get_due_deliveries", flaky_get_due_deliveries)

    task = asyncio.create_task(service._delivery_loop())
    await asyncio.wait_for(task, timeout=2.5)

    assert calls >= 2
