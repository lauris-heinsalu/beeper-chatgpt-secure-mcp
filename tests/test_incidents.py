"""Stage 3 regression coverage: safe persisted incidents, optional diagnostics and workers."""

import asyncio
import json
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

from beeper_events_sidecar.db import Database
from beeper_events_sidecar.identity import SourceIdentity
from beeper_events_sidecar.main import wait_for_shutdown_or_failure
from beeper_events_sidecar.mcp import McpApi
from beeper_events_sidecar.models import SourceEvent, Subscription
from beeper_events_sidecar.service import EventService

NOW = "2026-10-08T10:00:00Z"


class CapturingSender:
    def __init__(self):
        self.events = []

    async def send_event(self, **kwargs):
        self.events.append(kwargs["event"])
        return 204


def _fixture(tmp_path):
    db = Database(tmp_path / "state.sqlite3")
    db.initialize()
    sub = Subscription(
        id="sub_test",
        principal="local",
        name="message.created",
        arguments={},
        callback_url="https://example.com/callback",
        secret="whsec_" + "eA" * 24,
        previous_secret=None,
        previous_secret_expires_at=None,
        expires_at=None,
        active=True,
        created_at="2026-10-07T10:00:00Z",
    )
    db.upsert_subscription(sub, now=NOW)
    identity = SourceIdentity("beeper", "default", "account", "chat", "message")
    event = SourceEvent(
        source_key=identity.source_key,
        source_event_id=identity.source_event_id,
        name="message.created",
        occurred_at=NOW,
        account_id="account",
        chat_id="chat",
        local_chat_id=None,
        network="WhatsApp",
        message_id="message",
        sender_id="sender",
        sender_name=None,
        discovered_via="test",
    )
    assert db.record_event_and_enqueue(event, now=NOW)
    sender = CapturingSender()
    service = EventService(
        settings=SimpleNamespace(
            delivery_max_attempts=3,
            delivery_base_backoff_seconds=1,
            delivery_max_backoff_seconds=10,
            delivery_concurrency=1,
            reconcile_overlap_seconds=3600,
            reconcile_interval_seconds=60,
            source_instance="default",
        ),
        db=db,
        beeper=None,
        webhook_sender=sender,
    )
    return db, service, sender, db.get_due_deliveries(NOW)[0]


def test_incident_persists_is_deduplicated_and_only_safe_message_is_exposed(tmp_path):
    db = Database(tmp_path / "db.sqlite3")
    db.initialize()
    first = db.record_incident("INGEST_INTEGRITY_ERROR", NOW)
    db.record_incident("INGEST_INTEGRITY_ERROR", "2026-10-08T10:01:00Z")
    reopened = Database(db.path)
    reopened.initialize()
    status = reopened.stats(NOW)
    assert status["unresolved_incidents"] == 1
    item = status["recent_incidents"][0]
    assert item["incident_id"] == first
    assert item["code"] == "INGEST_INTEGRITY_ERROR"
    assert item["count"] == 2
    assert (
        item["message"]
        == "A source occurrence failed identity or database integrity validation."
    )
    assert "secret" not in json.dumps(status).lower()
    with pytest.raises(ValueError, match="Unsupported"):
        db.record_incident("secret:BEARER-TOKEN", NOW)


def test_incident_can_be_resolved_without_deleting_history(tmp_path):
    db = Database(tmp_path / "db.sqlite3")
    db.initialize()
    first = db.record_incident("INGEST_INTEGRITY_ERROR", NOW)
    assert db.resolve_incident("INGEST_INTEGRITY_ERROR", NOW)
    assert db.stats(NOW)["unresolved_incidents"] == 0
    second = db.record_incident("INGEST_INTEGRITY_ERROR", NOW)
    assert second != first  # A new incident episode must trigger a fresh alert.
    assert db.stats(NOW)["recent_incidents"][0]["count"] == 1
    with closing(sqlite3.connect(db.path)) as conn:
        assert conn.execute("SELECT count(*) FROM incidents").fetchone()[0] == 2


@pytest.mark.asyncio
async def test_message_payload_has_no_diagnostics_without_incidents(tmp_path):
    _db, service, sender, delivery = _fixture(tmp_path)
    await service._deliver(delivery)
    assert len(sender.events) == 1
    assert "diagnostics" not in sender.events[0]["data"]


@pytest.mark.asyncio
async def test_message_payload_gets_only_safe_diagnostics_when_incident_open(tmp_path):
    db, service, sender, delivery = _fixture(tmp_path)
    incident_id = db.record_incident("INGEST_INTEGRITY_ERROR", NOW)
    await service._deliver(delivery)
    data = sender.events[0]["data"]
    assert data["diagnostics"] == {
        "incident_id": incident_id,
        "code": "INGEST_INTEGRITY_ERROR",
        "severity": "error",
        "unresolved_incidents": 1,
    }
    assert "message" not in data["diagnostics"]
    schema = McpApi._events_list({})["events"][0]["payloadSchema"]
    assert "diagnostics" in schema["properties"]
    assert "diagnostics" not in schema["required"]
    assert schema["additionalProperties"] is False


@pytest.mark.asyncio
async def test_unexpected_worker_death_persists_incident_and_is_not_silent(tmp_path):
    db, service, _sender, _delivery = _fixture(tmp_path)

    async def unexpectedly_crash():
        await asyncio.sleep(0)
        raise ValueError("never expose secret credential XYZ in incidents")

    service._tasks = [
        asyncio.create_task(unexpectedly_crash(), name="webhook-delivery")
    ]
    with pytest.raises(RuntimeError, match="webhook-delivery"):
        await asyncio.wait_for(service.wait_for_worker_failure(), timeout=1)
    status = db.stats(NOW)
    assert status["unresolved_incidents"] == 1
    assert status["recent_incidents"][0]["code"] == "WORKER_FAILURE"
    assert "XYZ" not in json.dumps(status)


@pytest.mark.asyncio
async def test_process_shutdown_waiter_exits_on_worker_failure(tmp_path):
    _db, service, _sender, _delivery = _fixture(tmp_path)

    async def unexpectedly_crash():
        await asyncio.sleep(0)
        raise RuntimeError("worker dead")

    service._tasks = [
        asyncio.create_task(unexpectedly_crash(), name="webhook-delivery")
    ]
    with pytest.raises(RuntimeError, match="webhook-delivery"):
        await asyncio.wait_for(
            wait_for_shutdown_or_failure(service, asyncio.Event()), timeout=1
        )


@pytest.mark.asyncio
async def test_reconciliation_integrity_failure_preserves_checkpoint_and_records_incident(
    tmp_path,
):
    db, service, _sender, _delivery = _fixture(tmp_path)
    db.set_checkpoint("beeper_messages", NOW, NOW)
    before = db.get_checkpoint("beeper_messages")

    class BadBeeper:
        async def search_messages(self, *, date_after, date_before):
            raise sqlite3.IntegrityError("injected failure")
            yield

    service.beeper = BadBeeper()
    with pytest.raises(sqlite3.IntegrityError):
        await service.reconcile_once()
    assert db.get_checkpoint("beeper_messages") == before
    # Stage 3 requires the periodic reconcile supervisor to capture this incident.
    task = asyncio.create_task(service._reconcile_loop())
    service._reconcile_requested.set()
    for _ in range(30):
        if db.stats(NOW)["unresolved_incidents"]:
            break
        await asyncio.sleep(0.01)
    service._stop.set()
    service._reconcile_requested.set()
    await asyncio.wait_for(task, timeout=1)
    assert db.stats(NOW)["recent_incidents"][0]["code"] == "INGEST_INTEGRITY_ERROR"


@pytest.mark.asyncio
async def test_permanently_failed_webhook_records_visible_incident(tmp_path):
    db, service, sender, delivery = _fixture(tmp_path)

    async def reject_event(**kwargs):
        return 410

    sender.send_event = reject_event
    await service._deliver(delivery)
    assert db.stats(NOW)["dead_letter_deliveries"] == 1
    assert db.stats(NOW)["recent_incidents"][0]["code"] == "DELIVERY_DEAD_LETTER"


@pytest.mark.asyncio
async def test_ingest_integrity_error_persists_incident_without_payload(
    tmp_path, monkeypatch
):
    db, service, _sender, _delivery = _fixture(tmp_path)

    class Beeper:
        async def chat_metadata(self, chat_id):
            return {"network": "WhatsApp"}

    service.beeper = Beeper()

    def fail_ingest(*args, **kwargs):
        raise sqlite3.IntegrityError("sensitive source identity")

    monkeypatch.setattr(db, "record_event_and_enqueue", fail_ingest)
    payload = {
        "type": "message.upserted",
        "chatID": "chat",
        "ids": ["new-message"],
        "entries": [
            {
                "id": "new-message",
                "messageID": "new-message",
                "accountID": "account",
                "chatID": "chat",
                "timestamp": NOW,
                "isSender": False,
            }
        ],
    }
    with pytest.raises(RuntimeError, match="integrity validation"):
        await service._ingest_ws_event(payload)
    incident = db.stats(NOW)["recent_incidents"][0]
    assert incident["code"] == "INGEST_INTEGRITY_ERROR"
    assert "sensitive" not in json.dumps(db.stats(NOW))
    assert service._reconcile_requested.is_set()


@pytest.mark.asyncio
async def test_events_status_mcp_tool_returns_safe_incident_details(tmp_path):
    db, service, _sender, _delivery = _fixture(tmp_path)
    incident = db.record_incident("INGEST_INTEGRITY_ERROR", NOW)
    settings = SimpleNamespace(
        mcp_bearer_file=tmp_path / "token",
        mcp_bearer_refresh_seconds=2,
    )
    api = McpApi(
        settings=settings,
        db=db,
        service=service,
        webhook_sender=CapturingSender(),
    )
    response = await api._tools_call({"name": "events_status", "arguments": {}})
    assert response["isError"] is False
    structured = response["structuredContent"]
    assert structured["unresolved_incidents"] == 1
    assert structured["recent_incidents"][0]["incident_id"] == incident
    assert structured["recent_incidents"][0]["message"].startswith(
        "A source occurrence failed"
    )


@pytest.mark.asyncio
async def test_clean_shutdown_has_no_worker_failure_incident(tmp_path):
    db, service, _sender, _delivery = _fixture(tmp_path)
    task = asyncio.create_task(asyncio.sleep(10), name="healthy-worker")
    service._tasks = [task]
    signal = asyncio.Event()
    signal.set()
    await asyncio.wait_for(wait_for_shutdown_or_failure(service, signal), timeout=1)
    await service.stop()
    assert db.stats(NOW)["unresolved_incidents"] == 0
