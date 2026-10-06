import base64

import pytest

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
