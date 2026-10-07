from __future__ import annotations

import asyncio
import base64
import binascii
import hmac
import ipaddress
import json
import secrets
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from urllib.parse import urlsplit

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult
from standardwebhooks.webhooks import Webhook

from .models import parse_timestamp, utc_now


class CallbackEndpointError(RuntimeError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def validate_webhook_secret(secret: str) -> None:
    if not secret.startswith("whsec_"):
        raise ValueError("Webhook secret must start with whsec_")
    encoded = secret[len("whsec_") :]
    try:
        padding = "=" * (-len(encoded) % 4)
        raw = base64.b64decode(encoded + padding, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Webhook secret is not valid base64") from exc
    if not 24 <= len(raw) <= 64:
        raise ValueError(
            "Webhook signing key must decode to between 24 and 64 bytes"
        )


@dataclass(frozen=True, slots=True)
class ResolvedCallback:
    url: str
    hostname: str
    port: int
    addresses: tuple[tuple[int, str], ...]


class PinnedResolver(AbstractResolver):
    def __init__(
        self,
        hostname: str,
        addresses: tuple[tuple[int, str], ...],
    ) -> None:
        self.hostname = hostname
        self.addresses = addresses

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: socket.AddressFamily = socket.AF_INET,
    ) -> list[ResolveResult]:
        if host != self.hostname:
            raise OSError("Unexpected hostname for pinned callback resolver")
        return [
            {
                "hostname": host,
                "host": address,
                "port": port,
                "family": address_family,
                "proto": socket.IPPROTO_TCP,
                "flags": socket.AI_NUMERICHOST,
            }
            for address_family, address in self.addresses
        ]

    async def close(self) -> None:
        return None


class CallbackHttpClient:
    def __init__(self, timeout_seconds: int = 10) -> None:
        self.timeout_seconds = timeout_seconds

    async def resolve_public(self, url: str) -> ResolvedCallback:
        try:
            parts = urlsplit(url)
            hostname = parts.hostname
            port = parts.port or 443
        except ValueError as exc:
            raise CallbackEndpointError(
                "invalid_url", "Callback URL is malformed"
            ) from exc
        if parts.scheme != "https":
            raise CallbackEndpointError(
                "invalid_url", "Callback URL must use HTTPS"
            )
        if parts.username or parts.password:
            raise CallbackEndpointError(
                "invalid_url",
                "Callback URL must not contain user information",
            )
        if not hostname:
            raise CallbackEndpointError(
                "invalid_url", "Callback URL is missing a hostname"
            )
        if parts.fragment:
            raise CallbackEndpointError(
                "invalid_url", "Callback URL must not contain a fragment"
            )

        loop = asyncio.get_running_loop()
        try:
            results = await loop.getaddrinfo(
                hostname,
                port,
                type=socket.SOCK_STREAM,
            )
        except OSError as exc:
            raise CallbackEndpointError(
                "dns_error",
                f"Callback hostname could not be resolved: {exc}",
            ) from exc

        addresses: list[tuple[int, str]] = []
        seen: set[tuple[int, str]] = set()
        for family, _socktype, _proto, _canonname, sockaddr in results:
            address = str(sockaddr[0])
            key = (family, address)
            if key in seen:
                continue
            seen.add(key)
            try:
                ip = ipaddress.ip_address(address)
            except ValueError as exc:
                raise CallbackEndpointError(
                    "invalid_address",
                    "Callback resolved to an invalid IP address",
                ) from exc

            if not ip.is_global:
                raise CallbackEndpointError(
                    "private_address",
                    "Callback hostname resolved to a non-public address",
                )
            addresses.append((family, address))

        if not addresses:
            raise CallbackEndpointError(
                "dns_error", "Callback hostname resolved to no addresses"
            )

        return ResolvedCallback(
            url=url,
            hostname=hostname,
            port=port,
            addresses=tuple(addresses),
        )

    async def post(
        self,
        target: ResolvedCallback,
        *,
        body: bytes,
        headers: dict[str, str],
    ) -> tuple[int, bytes]:
        resolver = PinnedResolver(target.hostname, target.addresses)
        connector = aiohttp.TCPConnector(
            resolver=resolver,
            use_dns_cache=False,
            force_close=True,
        )
        timeout = cast(Any, aiohttp.ClientTimeout)(
            total=self.timeout_seconds
        )
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        ) as session:
            try:
                async with session.post(
                    target.url,
                    data=body,
                    headers=headers,
                    allow_redirects=False,
                ) as response:
                    if 300 <= response.status < 400:
                        raise CallbackEndpointError(
                            "redirect",
                            "Callback endpoint returned a redirect",
                        )
                    payload = bytearray()
                    while True:
                        chunk = await response.content.read(16_384)
                        if not chunk:
                            break
                        payload.extend(chunk)
                        if len(payload) > 65_536:
                            raise CallbackEndpointError(
                                "response_too_large",
                                "Callback response exceeded 64 KiB",
                            )
                    return response.status, bytes(payload)
            except TimeoutError as exc:
                raise CallbackEndpointError(
                    "timeout", "Callback endpoint timed out"
                ) from exc
            except aiohttp.ClientError as exc:
                raise CallbackEndpointError(
                    "connection_error",
                    f"Callback connection failed: {exc}",
                ) from exc


def _serialized_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _signatures(
    webhook_id: str,
    timestamp: datetime,
    body: bytes,
    signing_secrets: list[str],
) -> str:
    text = body.decode("utf-8")
    return " ".join(
        Webhook(secret).sign(webhook_id, timestamp, text)
        for secret in signing_secrets
    )


class WebhookSender:
    def __init__(self, client: CallbackHttpClient) -> None:
        self.client = client

    async def verify_callback(
        self,
        callback_url: str,
        secret: str,
        subscription_id: str,
    ) -> None:
        validate_webhook_secret(secret)
        target = await self.client.resolve_public(callback_url)

        challenge = secrets.token_urlsafe(32)
        body = _serialized_json(
            {"type": "verification", "challenge": challenge}
        )
        webhook_id = f"msg_verification_{secrets.token_hex(16)}"
        signed_at = utc_now()
        headers = {
            "Content-Type": "application/json",
            "webhook-id": webhook_id,
            "webhook-timestamp": str(int(signed_at.timestamp())),
            "webhook-signature": _signatures(
                webhook_id,
                signed_at,
                body,
                [secret],
            ),
            "X-MCP-Subscription-Id": subscription_id,
        }

        status, response_body = await self.client.post(
            target,
            body=body,
            headers=headers,
        )
        if not 200 <= status < 300:
            raise CallbackEndpointError(
                "http_status",
                f"Callback verification returned HTTP {status}",
            )

        try:
            result = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise CallbackEndpointError(
                "challenge_failed",
                "Callback verification returned invalid JSON",
            ) from exc

        echoed = result.get("challenge") if isinstance(result, dict) else None
        if not isinstance(echoed, str) or not hmac.compare_digest(
            challenge, echoed
        ):
            raise CallbackEndpointError(
                "challenge_failed",
                "Callback did not echo the verification challenge",
            )

    async def send_event(
        self,
        *,
        callback_url: str,
        subscription_id: str,
        event: dict[str, Any],
        secret: str,
        previous_secret: str | None,
        previous_secret_expires_at: str | None,
    ) -> int:
        validate_webhook_secret(secret)
        target = await self.client.resolve_public(callback_url)

        body = _serialized_json(event)
        if len(body) > 262_144:
            raise CallbackEndpointError(
                "payload_too_large",
                "Event payload exceeds 256 KiB",
            )

        signing_secrets = [secret]
        if (
            previous_secret
            and previous_secret_expires_at
            and parse_timestamp(previous_secret_expires_at) > utc_now()
        ):
            validate_webhook_secret(previous_secret)
            signing_secrets.append(previous_secret)

        event_id = str(event["eventId"])
        signed_at = utc_now()
        headers = {
            "Content-Type": "application/json",
            "webhook-id": event_id,
            "webhook-timestamp": str(int(signed_at.timestamp())),
            "webhook-signature": _signatures(
                event_id,
                signed_at,
                body,
                signing_secrets,
            ),
            "X-MCP-Subscription-Id": subscription_id,
        }
        status, _ = await self.client.post(
            target,
            body=body,
            headers=headers,
        )
        return status
