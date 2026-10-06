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
