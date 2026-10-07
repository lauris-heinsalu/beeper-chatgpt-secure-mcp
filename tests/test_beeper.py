import asyncio
import time
from dataclasses import replace
from pathlib import Path

import pytest

from beeper_events_sidecar.beeper import BeeperClient, BeeperError
from beeper_events_sidecar.config import Settings


class RecordingSession:
    def __init__(self):
        self.ws_calls = []

    async def ws_connect(self, url, **kwargs):
        self.ws_calls.append((url, kwargs))
        raise AssertionError("ws_connect must not be reached for untrusted endpoint")


def _settings(tmp_path: Path) -> Settings:
    auth_file = tmp_path / "authorization"
    auth_file.write_text("Bearer test-secret", encoding="utf-8")
    return Settings(
        host="127.0.0.1",
        port=23375,
        db_path=tmp_path / "state.sqlite3",
        beeper_base_url="http://127.0.0.1:23374",
        beeper_auth_file=auth_file,
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
async def test_beeper_authorization_read_is_offloaded_and_rotates(
    tmp_path,
    monkeypatch,
):
    settings = _settings(tmp_path)
    client = BeeperClient(settings, RecordingSession())
    original_read_text = Path.read_text

    def slow_read_text(path, *args, **kwargs):
        time.sleep(0.05)
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", slow_read_text)
    task = asyncio.create_task(client._authorization())
    await asyncio.sleep(0.01)
    assert not task.done()
    assert await task == "Bearer test-secret"

    settings.beeper_auth_file.write_text(
        "Bearer rotated-secret", encoding="utf-8"
    )
    assert await client._authorization() == "Bearer rotated-secret"

    settings.beeper_auth_file.write_text("Bearer ", encoding="utf-8")
    with pytest.raises(BeeperError, match="Invalid Beeper Authorization"):
        await client._authorization()

    settings.beeper_auth_file.unlink()
    with pytest.raises(BeeperError, match="Could not read Beeper Authorization"):
        await client._authorization()


@pytest.mark.asyncio
async def test_message_search_streams_pages_and_dedupes(tmp_path, monkeypatch):
    client = BeeperClient(_settings(tmp_path), RecordingSession())
    calls = []

    async def fake_get_json(path, *, params=None, authenticated=True):
        assert path == "/v1/messages/search"
        assert authenticated is True
        calls.append(dict(params or {}))
        if len(calls) == 1:
            return {
                "items": [
                    {
                        "id": "m-newer",
                        "chatID": "chat-a",
                        "timestamp": "2026-10-07T10:02:00Z",
                    },
                    {
                        "id": "m-new",
                        "chatID": "chat-a",
                        "timestamp": "2026-10-07T10:01:00Z",
                    },
                ],
                "hasMore": True,
                "oldestCursor": "cursor-1",
            }
        return {
            "items": [
                {
                    "id": "m-new",
                    "chatID": "chat-a",
                    "timestamp": "2026-10-07T10:01:00Z",
                },
                {
                    "id": "m-old",
                    "chatID": "chat-a",
                    "timestamp": "2026-10-07T09:59:00Z",
                },
            ],
            "hasMore": False,
        }

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    stream = client.search_messages(
        date_after="2026-10-07T09:00:00Z",
        date_before="2026-10-07T11:00:00Z",
    )
    first = await anext(stream)
    assert first["id"] == "m-new"
    assert len(calls) == 1

    remaining = [item async for item in stream]
    assert [item["id"] for item in remaining] == ["m-newer", "m-old"]
    assert len(calls) == 2
    assert calls[1]["cursor"] == "cursor-1"
    assert calls[1]["direction"] == "before"


@pytest.mark.asyncio
async def test_chat_metadata_cache_is_bounded_lru(tmp_path, monkeypatch):
    settings = replace(_settings(tmp_path), chat_cache_size=2)
    client = BeeperClient(settings, RecordingSession())
    calls = []

    async def fake_get_json(path, *, params=None, authenticated=True):
        calls.append(path)
        chat_id = path.rsplit("/", 1)[-1]
        return {"id": chat_id}

    monkeypatch.setattr(client, "_get_json", fake_get_json)

    assert (await client.chat_metadata("chat-a"))["id"] == "chat-a"
    assert (await client.chat_metadata("chat-b"))["id"] == "chat-b"
    assert (await client.chat_metadata("chat-a"))["id"] == "chat-a"
    assert (await client.chat_metadata("chat-c"))["id"] == "chat-c"
    assert list(client._chat_cache) == ["chat-a", "chat-c"]

    assert (await client.chat_metadata("chat-b"))["id"] == "chat-b"
    assert calls.count("/v1/chats/chat-a") == 1
    assert calls.count("/v1/chats/chat-b") == 2
    assert calls.count("/v1/chats/chat-c") == 1
    assert list(client._chat_cache) == ["chat-c", "chat-b"]


@pytest.mark.asyncio
async def test_event_websocket_rejects_cross_origin_endpoint(tmp_path, monkeypatch):
    session = RecordingSession()
    client = BeeperClient(_settings(tmp_path), session)

    async def fake_server_info():
        return {"endpoints": {"ws_events": "http://evil.example/v1/ws"}}

    monkeypatch.setattr(client, "server_info", fake_server_info)

    with pytest.raises(BeeperError, match="origin"):
        await client.open_event_websocket()

    assert session.ws_calls == []


@pytest.mark.asyncio
async def test_event_websocket_rejects_unsupported_endpoint_scheme(
    tmp_path,
    monkeypatch,
):
    session = RecordingSession()
    client = BeeperClient(_settings(tmp_path), session)

    async def fake_server_info():
        return {"endpoints": {"ws_events": "ftp://127.0.0.1:23374/v1/ws"}}

    monkeypatch.setattr(client, "server_info", fake_server_info)

    with pytest.raises(BeeperError, match="scheme"):
        await client.open_event_websocket()

    assert session.ws_calls == []


def test_event_websocket_accepts_same_origin_http_or_ws(tmp_path):
    client = BeeperClient(_settings(tmp_path), RecordingSession())

    assert (
        client._event_websocket_url("http://127.0.0.1:23374/v1/ws")
        == "ws://127.0.0.1:23374/v1/ws"
    )
    assert (
        client._event_websocket_url("ws://localhost:23374/v1/ws?x=1")
        == "ws://localhost:23374/v1/ws?x=1"
    )


def test_event_websocket_preserves_https_security_boundary(tmp_path):
    settings = replace(
        _settings(tmp_path),
        beeper_base_url="https://beeper.example",
    )
    client = BeeperClient(settings, RecordingSession())

    assert (
        client._event_websocket_url("wss://beeper.example/v1/ws")
        == "wss://beeper.example/v1/ws"
    )
    with pytest.raises(BeeperError, match="scheme"):
        client._event_websocket_url("ws://beeper.example/v1/ws")
