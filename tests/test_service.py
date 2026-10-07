import asyncio
import sqlite3
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from beeper_events_sidecar.beeper import ReconciliationWindowTooLarge
from beeper_events_sidecar.config import Settings
from beeper_events_sidecar.db import Database
from beeper_events_sidecar.models import (
    Subscription,
    isoformat_z,
    parse_timestamp,
    utc_now,
)
from beeper_events_sidecar.service import EventService


class FakeBeeper:
    def __init__(self, messages, hydrated=None):
        self.messages = messages
        self.hydrated = hydrated or {}

    async def search_messages(self, *, date_after, date_before):
        for message in self.messages:
            yield message

    async def message(self, chat_id, message_id):
        return self.hydrated[(chat_id, message_id)]

    async def chat_metadata(self, chat_id):
        return {
            "id": chat_id,
            "localChatID": "84",
            "network": "WhatsApp",
        }


class WindowLimitedBeeper(FakeBeeper):
    def __init__(self, messages, *, max_window_seconds):
        super().__init__(messages)
        self.max_window_seconds = max_window_seconds
        self.calls = []

    async def search_messages(self, *, date_after, date_before):
        lower = parse_timestamp(date_after)
        upper = parse_timestamp(date_before)
        self.calls.append((lower, upper))
        matching = [
            message
            for message in self.messages
            if lower < parse_timestamp(message["timestamp"]) < upper
        ]
        if (upper - lower).total_seconds() > self.max_window_seconds:
            if matching:
                yield matching[0]
            raise ReconciliationWindowTooLarge("window too large")
        for message in matching:
            yield message


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
async def test_reconciliation_splits_oversized_window_and_advances_checkpoint(
    tmp_path,
):
    now = utc_now()
    settings = replace(_settings(tmp_path), reconcile_overlap_seconds=0)
    db = Database(settings.db_path)
    db.initialize()

    subscription_created = isoformat_z(now - timedelta(minutes=20))
    subscription = Subscription(
        id="sub-split",
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

    checkpoint = isoformat_z(now - timedelta(minutes=10))
    db.set_checkpoint("beeper_messages", checkpoint, checkpoint)
    messages = [
        {
            "id": f"m-{minutes}",
            "accountID": "whatsapp",
            "chatID": "chat-a",
            "senderID": "sender-a",
            "senderName": "Alice",
            "timestamp": isoformat_z(now - timedelta(minutes=minutes)),
            "isSender": False,
            "isDeleted": False,
        }
        for minutes in (8, 4, 1)
    ]
    beeper = WindowLimitedBeeper(messages, max_window_seconds=180)
    service = EventService(
        settings=settings,
        db=db,
        beeper=beeper,
        webhook_sender=UnusedSender(),
    )

    await service.reconcile_once()

    stats = db.stats(isoformat_z(utc_now()))
    assert stats["source_events_seen"] == 3
    assert stats["pending_deliveries"] == 3
    advanced = db.get_checkpoint("beeper_messages")
    assert advanced is not None
    assert parse_timestamp(advanced) > parse_timestamp(checkpoint)
    assert len(beeper.calls) > 1
    assert any(
        (upper - lower).total_seconds() > beeper.max_window_seconds
        for lower, upper in beeper.calls
    )
    assert any(
        (upper - lower).total_seconds() <= beeper.max_window_seconds
        for lower, upper in beeper.calls
    )

    await service.reconcile_once()
    second = db.stats(isoformat_z(utc_now()))
    assert second["source_events_seen"] == 3
    assert second["pending_deliveries"] == 3


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
        self.calls = []
        self.verified = []

    async def verify_callback(self, url, secret, subscription_id):
        self.verified.append((url, secret, subscription_id))

    async def send_event(self, **kwargs):
        self.event_ids.append(kwargs["event"]["eventId"])
        self.calls.append(kwargs)
        return self.status


class BlockingSender(StatusSender):
    def __init__(self, status: int):
        super().__init__(status)
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.verified = []

    async def verify_callback(self, url, secret, subscription_id):
        self.verified.append((url, secret, subscription_id))

    async def send_event(self, **kwargs):
        self.event_ids.append(kwargs["event"]["eventId"])
        self.calls.append(kwargs)
        self.started.set()
        await self.release.wait()
        return self.status


class ConcurrentBlockingSender(StatusSender):
    def __init__(self, status: int, target_started: int):
        super().__init__(status)
        self.target_started = target_started
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def send_event(self, **kwargs):
        self.event_ids.append(kwargs["event"]["eventId"])
        self.calls.append(kwargs)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        if len(self.event_ids) >= self.target_started:
            self.started.set()
        try:
            await self.release.wait()
            return self.status
        finally:
            self.active -= 1


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


async def _mcp_delivery_fixture(tmp_path, sender, *, message_id: str):
    import base64

    from beeper_events_sidecar.mcp import McpApi
    from beeper_events_sidecar.models import SourceEvent

    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    service = EventService(
        settings=settings,
        db=db,
        beeper=FakeBeeper([]),
        webhook_sender=sender,
    )
    api = McpApi(
        settings=settings,
        db=db,
        service=service,
        webhook_sender=sender,
    )
    secret = "whsec_" + base64.b64encode(b"x" * 32).decode()
    params = {
        "name": "message.created",
        "arguments": {},
        "delivery": {
            "mode": "webhook",
            "url": "https://example.com/callback",
            "secret": secret,
        },
        "cursor": None,
    }
    subscribed = await api._events_subscribe(params)

    event_time = utc_now() + timedelta(seconds=1)
    event = SourceEvent(
        source_key=f"beeper:whatsapp:chat:{message_id}",
        source_event_id=f"src-{message_id}",
        name="message.created",
        occurred_at=isoformat_z(event_time),
        account_id="whatsapp",
        chat_id="chat",
        local_chat_id=None,
        network="WhatsApp",
        message_id=message_id,
        sender_id="sender",
        sender_name="Alice",
        discovered_via="test",
    )
    due_at = isoformat_z(event_time + timedelta(seconds=1))
    assert db.record_event_and_enqueue(event, now=due_at)
    delivery = db.get_due_deliveries(due_at)[0]
    return db, service, api, params, delivery, subscribed["id"], secret


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
async def test_delivery_batch_runs_with_bounded_concurrency(tmp_path):
    from dataclasses import replace

    sender = ConcurrentBlockingSender(204, target_started=2)
    db, service, first_delivery, now_text = _delivery_fixture(tmp_path, sender)
    service.settings = replace(service.settings, delivery_concurrency=2)

    from beeper_events_sidecar.models import SourceEvent

    for suffix in ("two", "three"):
        event = SourceEvent(
            source_key=f"beeper:whatsapp:chat:message-{suffix}",
            source_event_id=f"src-delivery-{suffix}",
            name="message.created",
            occurred_at=now_text,
            account_id="whatsapp",
            chat_id="chat",
            local_chat_id=None,
            network="WhatsApp",
            message_id=f"message-{suffix}",
            sender_id="sender",
            sender_name="Alice",
            discovered_via="test",
        )
        assert db.record_event_and_enqueue(event, now=now_text)

    deliveries = db.get_due_deliveries(now_text, limit=20)
    assert len(deliveries) == 3
    assert first_delivery.event_id in {item.event_id for item in deliveries}

    task = asyncio.create_task(service._deliver_batch(deliveries))
    await asyncio.wait_for(sender.started.wait(), timeout=1.0)
    await asyncio.sleep(0.02)

    assert len(sender.event_ids) == 2
    assert sender.max_active == 2

    sender.release.set()
    await asyncio.wait_for(task, timeout=1.0)
    assert len(sender.event_ids) == 3
    assert sender.max_active == 2


@pytest.mark.asyncio
async def test_unsubscribe_resubscribe_does_not_resurrect_inflight_retry(
    tmp_path,
):
    sender = BlockingSender(500)
    db, service, api, params, delivery, subscription_id, _secret = (
        await _mcp_delivery_fixture(tmp_path, sender, message_id="m-race")
    )

    task = asyncio.create_task(service._deliver(delivery))
    await asyncio.wait_for(sender.started.wait(), timeout=1.0)
    await api._events_unsubscribe(
        {
            "name": "message.created",
            "arguments": {},
            "delivery": {
                "mode": "webhook",
                "url": "https://example.com/callback",
            },
        }
    )
    await api._events_subscribe(params)
    assert db.get_subscription(subscription_id) is not None

    sender.release.set()
    await asyncio.wait_for(task, timeout=1.0)

    assert db.stats(isoformat_z(utc_now()))["pending_deliveries"] == 0


@pytest.mark.asyncio
async def test_stale_delivery_snapshot_is_not_sent_after_resubscribe(tmp_path):
    sender = StatusSender(204)
    db, service, api, params, delivery, _subscription_id, _secret = (
        await _mcp_delivery_fixture(tmp_path, sender, message_id="m-stale")
    )

    await api._events_unsubscribe(
        {
            "name": "message.created",
            "arguments": {},
            "delivery": {
                "mode": "webhook",
                "url": "https://example.com/callback",
            },
        }
    )
    await api._events_subscribe(params)

    await service._deliver(delivery)

    assert sender.event_ids == []
    assert db.stats(isoformat_z(utc_now()))["pending_deliveries"] == 0


@pytest.mark.asyncio
async def test_delivery_refreshes_subscription_before_send(tmp_path):
    import base64

    sender = StatusSender(204)
    _db, service, api, params, delivery, _subscription_id, secret_a = (
        await _mcp_delivery_fixture(tmp_path, sender, message_id="m-rotate")
    )
    secret_b = "whsec_" + base64.b64encode(b"b" * 32).decode()
    refreshed = {
        **params,
        "delivery": {**params["delivery"], "secret": secret_b},
    }
    await api._events_subscribe(refreshed)

    await service._deliver(delivery)

    assert len(sender.calls) == 1
    assert sender.calls[0]["secret"] == secret_b
    assert sender.calls[0]["previous_secret"] == secret_a

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
