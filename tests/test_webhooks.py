import asyncio
import base64
import socket

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from beeper_events_sidecar.webhooks import (
    CallbackEndpointError,
    CallbackHttpClient,
    validate_webhook_secret,
)


def _secret(size: int = 32) -> str:
    return "whsec_" + base64.b64encode(b"x" * size).decode()


def test_valid_webhook_secret():
    validate_webhook_secret(_secret())


@pytest.mark.parametrize("size", [1, 23, 65, 100])
def test_invalid_webhook_secret_length(size):
    with pytest.raises(ValueError):
        validate_webhook_secret(_secret(size))


@pytest.mark.asyncio
async def test_callback_rejects_loopback():
    client = CallbackHttpClient()
    with pytest.raises(CallbackEndpointError) as exc:
        await client.resolve_public("https://127.0.0.1/callback")
    assert exc.value.reason == "private_address"


@pytest.mark.asyncio
async def test_callback_requires_https():
    client = CallbackHttpClient()
    with pytest.raises(CallbackEndpointError) as exc:
        await client.resolve_public("http://example.com/callback")
    assert exc.value.reason == "invalid_url"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "https://example.com:99999/callback",
        "https://[invalid/callback",
    ],
)
async def test_callback_rejects_malformed_url(url):
    client = CallbackHttpClient()
    with pytest.raises(CallbackEndpointError) as exc:
        await client.resolve_public(url)
    assert exc.value.reason == "invalid_url"


@pytest.mark.asyncio
async def test_callback_post_reads_split_response_to_eof():
    first = b'{"challenge":"abc'
    second = b'def"}'

    async def handler(_request):
        response = web.StreamResponse(status=200)
        await response.prepare(_request)
        await response.write(first)
        await asyncio.sleep(0.05)
        await response.write(second)
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_post("/callback", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        from beeper_events_sidecar.webhooks import ResolvedCallback

        port = server.port
        target = ResolvedCallback(
            url=f"http://127.0.0.1:{port}/callback",
            hostname="127.0.0.1",
            port=port,
            addresses=((socket.AF_INET, "127.0.0.1"),),
        )
        status, payload = await CallbackHttpClient().post(
            target,
            body=b"{}",
            headers={"Content-Type": "application/json"},
        )
    finally:
        await server.close()

    assert status == 200
    assert payload == first + second


@pytest.mark.asyncio
async def test_callback_post_rejects_split_response_over_limit():
    chunk = b"x" * 40_000

    async def handler(_request):
        response = web.StreamResponse(status=200)
        await response.prepare(_request)
        await response.write(chunk)
        await asyncio.sleep(0.05)
        await response.write(chunk)
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_post("/callback", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        from beeper_events_sidecar.webhooks import ResolvedCallback

        port = server.port
        target = ResolvedCallback(
            url=f"http://127.0.0.1:{port}/callback",
            hostname="127.0.0.1",
            port=port,
            addresses=((socket.AF_INET, "127.0.0.1"),),
        )
        with pytest.raises(CallbackEndpointError) as exc:
            await CallbackHttpClient().post(
                target,
                body=b"{}",
                headers={"Content-Type": "application/json"},
            )
    finally:
        await server.close()

    assert exc.value.reason == "response_too_large"


class FakeCallbackClient:
    def __init__(self):
        self.posts = []

    async def resolve_public(self, url):
        return type(
            "Target",
            (),
            {
                "url": url,
                "hostname": "example.com",
                "port": 443,
                "addresses": ((2, "93.184.216.34"),),
            },
        )()

    async def post(self, target, *, body, headers):
        self.posts.append((target, body, headers))
        import json

        payload = json.loads(body)
        if payload.get("type") == "verification":
            return 200, json.dumps(
                {"challenge": payload["challenge"]}
            ).encode()
        return 204, b""


@pytest.mark.asyncio
async def test_verification_includes_subscription_header():
    from beeper_events_sidecar.webhooks import WebhookSender

    client = FakeCallbackClient()
    sender = WebhookSender(client)
    await sender.verify_callback(
        "https://example.com/callback",
        _secret(),
        "sub_123",
    )

    assert len(client.posts) == 1
    _target, _body, headers = client.posts[0]
    assert headers["X-MCP-Subscription-Id"] == "sub_123"
    assert headers["webhook-id"].startswith("msg_verification_")
    assert headers["webhook-signature"].startswith("v1,")


@pytest.mark.asyncio
async def test_event_rotation_emits_two_signatures():
    from datetime import timedelta

    from beeper_events_sidecar.models import isoformat_z, utc_now
    from beeper_events_sidecar.webhooks import WebhookSender

    client = FakeCallbackClient()
    sender = WebhookSender(client)
    event = {
        "eventId": "evt_123",
        "name": "message.created",
        "timestamp": isoformat_z(utc_now()),
        "data": {
            "account_id": "whatsapp",
            "chat_id": "chat",
            "message_id": "msg",
            "timestamp": isoformat_z(utc_now()),
        },
        "cursor": None,
    }

    status = await sender.send_event(
        callback_url="https://example.com/callback",
        subscription_id="sub_123",
        event=event,
        secret=_secret(),
        previous_secret=_secret(24),
        previous_secret_expires_at=isoformat_z(
            utc_now() + timedelta(minutes=5)
        ),
    )

    assert status == 204
    _target, _body, headers = client.posts[0]
    signatures = headers["webhook-signature"].split(" ")
    assert len(signatures) == 2
    assert all(value.startswith("v1,") for value in signatures)
