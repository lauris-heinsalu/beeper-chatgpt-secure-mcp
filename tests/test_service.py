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
    def __init__(self, messages):
        self.messages = messages

    async def search_messages(self, *, date_after, date_before):
        return list(self.messages)

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
