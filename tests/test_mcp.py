import asyncio
import base64
from pathlib import Path

import pytest

from beeper_events_sidecar.config import Settings
from beeper_events_sidecar.db import Database
from beeper_events_sidecar.mcp import McpApi, RpcError


class FakeService:
    def status(self):
        return {"source_connected": True, "workers_healthy": True}


class DeadWorkerService:
    def status(self):
        return {
            "source_connected": True,
            "workers_healthy": False,
            "workers": {"webhook-delivery": "failed"},
        }


class FakeSender:
    def __init__(self):
        self.verified = []

    async def verify_callback(self, url, secret, subscription_id):
        self.verified.append((url, secret, subscription_id))


class BlockingVerifySender(FakeSender):
    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def verify_callback(self, url, secret, subscription_id):
        self.verified.append((url, secret, subscription_id))
        self.started.set()
        await self.release.wait()


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


def _secret(byte: bytes) -> str:
    return "whsec_" + base64.b64encode(byte * 32).decode()


@pytest.mark.asyncio
async def test_subscription_is_persistent_idempotent_and_rotates_secret(
    tmp_path,
):
    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    sender = FakeSender()
    api = McpApi(
        settings=settings,
        db=db,
        service=FakeService(),
        webhook_sender=sender,
    )

    params = {
        "name": "message.created",
        "arguments": {"account_ids": ["whatsapp"]},
        "delivery": {
            "mode": "webhook",
            "url": "https://example.com/callback",
            "secret": _secret(b"a"),
        },
        "ttlMs": 60_000,
    }

    first = await api._events_subscribe(params)
    assert first["id"].startswith("sub_")
    assert len(sender.verified) == 1

    second_params = {
        **params,
        "delivery": {
            **params["delivery"],
            "secret": _secret(b"b"),
        },
    }
    second = await api._events_subscribe(second_params)
    assert second["id"] == first["id"]
    # Callback verification is cached by principal + URL as recommended.
    assert len(sender.verified) == 1

    stored = db.get_subscription(first["id"])
    assert stored is not None
    assert stored.secret == _secret(b"b")
    assert stored.previous_secret == _secret(b"a")
    assert stored.previous_secret_expires_at is not None


@pytest.mark.asyncio
async def test_unsubscribe_during_verification_prevents_stale_reactivation(
    tmp_path,
):
    from beeper_events_sidecar.models import isoformat_z, utc_now

    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    sender = BlockingVerifySender()
    api = McpApi(
        settings=settings,
        db=db,
        service=FakeService(),
        webhook_sender=sender,
    )
    params = {
        "name": "message.created",
        "arguments": {},
        "delivery": {
            "mode": "webhook",
            "url": "https://example.com/callback",
            "secret": _secret(b"a"),
        },
        "cursor": None,
    }

    subscribe_task = asyncio.create_task(api._events_subscribe(params))
    await asyncio.wait_for(sender.started.wait(), timeout=1.0)
    api._events_unsubscribe(
        {
            "name": "message.created",
            "arguments": {},
            "delivery": {
                "mode": "webhook",
                "url": "https://example.com/callback",
            },
        }
    )
    sender.release.set()

    with pytest.raises(RpcError, match="superseded"):
        await asyncio.wait_for(subscribe_task, timeout=1.0)

    assert db.stats(isoformat_z(utc_now()))["active_subscriptions"] == 0


@pytest.mark.asyncio
async def test_event_seen_during_initial_verification_is_backfilled(tmp_path):
    from datetime import timedelta

    from beeper_events_sidecar.models import SourceEvent, isoformat_z, utc_now

    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    sender = BlockingVerifySender()
    api = McpApi(
        settings=settings,
        db=db,
        service=FakeService(),
        webhook_sender=sender,
    )
    params = {
        "name": "message.created",
        "arguments": {"sender_ids": ["sender"]},
        "delivery": {
            "mode": "webhook",
            "url": "https://example.com/callback",
            "secret": _secret(b"a"),
        },
        "cursor": None,
    }

    subscribe_task = asyncio.create_task(api._events_subscribe(params))
    await asyncio.wait_for(sender.started.wait(), timeout=1.0)

    event_time = utc_now()
    event = SourceEvent(
        source_key="beeper:a:c:m-during-verify",
        source_event_id="src_during_verify",
        name="message.created",
        occurred_at=isoformat_z(event_time),
        account_id="a",
        chat_id="c",
        local_chat_id=None,
        network="WhatsApp",
        message_id="m-during-verify",
        sender_id="sender",
        sender_name="Alice",
        discovered_via="test",
    )
    assert db.record_event_and_enqueue(
        event,
        now=isoformat_z(event_time + timedelta(milliseconds=1)),
    )
    assert db.stats(isoformat_z(utc_now()))["pending_deliveries"] == 0

    sender.release.set()
    await asyncio.wait_for(subscribe_task, timeout=1.0)

    assert db.stats(isoformat_z(utc_now()))["pending_deliveries"] == 1


def test_discover_advertises_only_events_and_status_tool(tmp_path):
    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    api = McpApi(
        settings=settings,
        db=db,
        service=FakeService(),
        webhook_sender=FakeSender(),
    )

    discover = api._discover()
    assert discover["supportedVersions"] == ["2026-07-28"]
    assert "events" in discover["capabilities"]

    event_catalog = api._events_list({})
    assert event_catalog["events"][0]["name"] == "message.created"

    tools = api._tools_list()["tools"]
    assert [tool["name"] for tool in tools] == ["events_status"]


@pytest.mark.asyncio
async def test_mcp_endpoint_requires_bearer(tmp_path):
    from aiohttp.test_utils import TestClient, TestServer

    settings = _settings(tmp_path)
    settings.mcp_bearer_file.write_text(
        "Bearer test-secret", encoding="utf-8"
    )
    db = Database(settings.db_path)
    db.initialize()
    api = McpApi(
        settings=settings,
        db=db,
        service=FakeService(),
        webhook_sender=FakeSender(),
    )

    client = TestClient(TestServer(api.application()))
    await client.start_server()
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientCapabilities": {},
                }
            },
        }

        unauthorized = await client.post("/mcp", json=payload)
        assert unauthorized.status == 401

        authorized = await client.post(
            "/mcp",
            json=payload,
            headers={"Authorization": "Bearer test-secret"},
        )
        assert authorized.status == 200
        body = await authorized.json()
        assert body["result"]["supportedVersions"] == ["2026-07-28"]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_unsubscribe_cancels_pending_and_resubscribe_starts_fresh(
    tmp_path,
):
    from datetime import timedelta

    from beeper_events_sidecar.models import SourceEvent, isoformat_z, utc_now

    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    sender = FakeSender()
    api = McpApi(
        settings=settings,
        db=db,
        service=FakeService(),
        webhook_sender=sender,
    )

    params = {
        "name": "message.created",
        "arguments": {},
        "delivery": {
            "mode": "webhook",
            "url": "https://example.com/callback",
            "secret": _secret(b"a"),
        },
        "cursor": None,
    }
    subscribed = await api._events_subscribe(params)
    first_subscription = db.get_subscription(subscribed["id"])
    assert first_subscription is not None

    event_time = utc_now() + timedelta(seconds=1)
    event = SourceEvent(
        source_key="beeper:a:c:m-old",
        source_event_id="src_old",
        name="message.created",
        occurred_at=isoformat_z(event_time),
        account_id="a",
        chat_id="c",
        local_chat_id=None,
        network="WhatsApp",
        message_id="m-old",
        sender_id="sender",
        sender_name="Alice",
        discovered_via="test",
    )
    assert db.record_event_and_enqueue(
        event,
        now=isoformat_z(event_time + timedelta(seconds=1)),
    )
    assert db.stats(isoformat_z(event_time))["pending_deliveries"] == 1

    api._events_unsubscribe(
        {
            "name": "message.created",
            "arguments": {},
            "delivery": {
                "mode": "webhook",
                "url": "https://example.com/callback",
            },
        }
    )
    assert db.stats(isoformat_z(event_time))["pending_deliveries"] == 0

    await api._events_subscribe(params)
    second_subscription = db.get_subscription(subscribed["id"])
    assert second_subscription is not None
    assert second_subscription.active
    assert (
        second_subscription.created_at
        != first_subscription.created_at
    )
    # Cancelled work from the old subscription lifecycle never reappears.
    assert db.stats(isoformat_z(utc_now()))["pending_deliveries"] == 0


@pytest.mark.asyncio
async def test_health_and_readiness_fail_when_worker_is_dead(tmp_path):
    from aiohttp.test_utils import TestClient, TestServer

    settings = _settings(tmp_path)
    db = Database(settings.db_path)
    db.initialize()
    api = McpApi(
        settings=settings,
        db=db,
        service=DeadWorkerService(),
        webhook_sender=FakeSender(),
    )

    client = TestClient(TestServer(api.application()))
    await client.start_server()
    try:
        health = await client.get("/healthz")
        ready = await client.get("/readyz")
        assert health.status == 503
        assert ready.status == 503
    finally:
        await client.close()
