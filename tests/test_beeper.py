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
