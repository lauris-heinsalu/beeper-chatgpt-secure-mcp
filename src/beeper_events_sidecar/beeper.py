from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
from collections.abc import AsyncIterator
from typing import Any, cast
from urllib.parse import urlsplit, urlunsplit

import aiohttp

from .config import Settings

logger = logging.getLogger(__name__)


class BeeperError(RuntimeError):
    pass


class ReconciliationWindowTooLarge(BeeperError):
    pass


class BeeperClient:
    def __init__(
        self,
        settings: Settings,
        session: aiohttp.ClientSession,
    ) -> None:
        self.settings = settings
        self.session = session
        self._chat_cache: dict[str, dict[str, Any]] = {}

    def _authorization(self) -> str:
        value = self.settings.beeper_auth_file.read_text(
            encoding="utf-8"
        ).strip()
        if not value.startswith("Bearer ") or len(value) <= len("Bearer "):
            raise BeeperError(
                f"Invalid Beeper Authorization file: "
                f"{self.settings.beeper_auth_file}"
            )
        return value

    def _headers(self) -> dict[str, str]:
        return {"Authorization": self._authorization()}

    async def _get_json(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
        authenticated: bool = True,
    ) -> dict[str, Any]:
        url = f"{self.settings.beeper_base_url}{path}"
        headers = self._headers() if authenticated else {}
        async with self.session.get(
            url,
            params=params,
            headers=headers,
            timeout=cast(Any, aiohttp.ClientTimeout)(total=15),
        ) as response:
            body = await response.text()
            if response.status < 200 or response.status >= 300:
                raise BeeperError(
                    f"GET {path} failed with HTTP {response.status}: "
                    f"{body[:500]}"
                )
            try:
                value = json.loads(body)
            except json.JSONDecodeError as exc:
                raise BeeperError(
                    f"GET {path} returned non-JSON content"
                ) from exc
            if not isinstance(value, dict):
                raise BeeperError(f"GET {path} returned non-object JSON")
            return value

    async def server_info(self) -> dict[str, Any]:
        return await self._get_json("/v1/info", authenticated=False)

    async def chat_metadata(self, chat_id: str) -> dict[str, Any]:
        cached = self._chat_cache.get(chat_id)
        if cached is not None:
            return cached
        value = await self._get_json(f"/v1/chats/{chat_id}")
        self._chat_cache[chat_id] = value
        return value

    async def message(
        self,
        chat_id: str,
        message_id: str,
    ) -> dict[str, Any]:
        return await self._get_json(
            f"/v1/chats/{chat_id}/messages/{message_id}"
        )

    async def search_messages(
        self,
        *,
        date_after: str,
        date_before: str,
    ) -> list[dict[str, Any]]:
        items: dict[tuple[str, str], dict[str, Any]] = {}
        cursor: str | None = None
        page_count = 0

        while True:
            params = {
                "dateAfter": date_after,
                "dateBefore": date_before,
                "sender": "others",
                "excludeLowPriority": "false",
                "includeMuted": "true",
                "limit": "20",
            }
            if cursor:
                params["cursor"] = cursor
                params["direction"] = "before"

            data = await self._get_json(
                "/v1/messages/search",
                params=params,
            )
            page_count += 1
            if page_count > 1000:
                raise ReconciliationWindowTooLarge(
                    "Message reconciliation exceeded 1000 pages"
                )

            page_items = data.get("items") or []
            if not isinstance(page_items, list):
                raise BeeperError(
                    "Message search returned a non-list items field"
                )

            for item in page_items:
                if not isinstance(item, dict):
                    continue
                chat_id = item.get("chatID")
                message_id = item.get("id") or item.get("messageID")
                if isinstance(chat_id, str) and isinstance(
                    message_id, str
                ):
                    items[(chat_id, message_id)] = item

            if not data.get("hasMore"):
                break

            next_cursor = (
                data.get("oldestCursor")
                or data.get("nextCursor")
                or data.get("cursor")
            )
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise BeeperError(
                    "Message search reported hasMore without "
                    "a usable pagination cursor"
                )
            cursor = next_cursor

        return sorted(
            items.values(),
            key=lambda item: (
                str(item.get("timestamp") or ""),
                str(item.get("id") or item.get("messageID") or ""),
            ),
        )

    @staticmethod
    def _is_loopback_host(hostname: str) -> bool:
        if hostname.lower() == "localhost":
            return True
        try:
            return ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            return False

    def _event_websocket_url(self, raw_url: str) -> str:
        base = urlsplit(self.settings.beeper_base_url)
        if base.scheme not in {"http", "https"} or not base.hostname:
            raise BeeperError("Invalid configured Beeper base URL")
        if base.username or base.password or base.fragment:
            raise BeeperError("Invalid configured Beeper base URL")

        parts = urlsplit(raw_url)
        allowed_schemes = (
            {"https", "wss"}
            if base.scheme == "https"
            else {"http", "ws"}
        )
        if parts.scheme not in allowed_schemes:
            raise BeeperError(
                "Beeper WebSocket endpoint uses an unsupported scheme"
            )
        if parts.username or parts.password:
            raise BeeperError(
                "Beeper WebSocket endpoint must not contain user information"
            )
        if not parts.hostname:
            raise BeeperError("Beeper WebSocket endpoint is missing a hostname")
        if parts.fragment:
            raise BeeperError(
                "Beeper WebSocket endpoint must not contain a fragment"
            )

        try:
            base_port = base.port or (443 if base.scheme == "https" else 80)
            endpoint_port = parts.port or (
                443 if parts.scheme in {"https", "wss"} else 80
            )
        except ValueError as exc:
            raise BeeperError("Beeper WebSocket endpoint has an invalid port") from exc

        base_host = base.hostname.lower()
        endpoint_host = parts.hostname.lower()
        same_host = endpoint_host == base_host
        loopback_aliases = self._is_loopback_host(
            base_host
        ) and self._is_loopback_host(endpoint_host)
        if not (same_host or loopback_aliases) or endpoint_port != base_port:
            raise BeeperError(
                "Beeper WebSocket endpoint origin does not match Beeper base URL"
            )

        scheme = "wss" if parts.scheme in {"https", "wss"} else "ws"
        return urlunsplit(
            (scheme, parts.netloc, parts.path, parts.query, "")
        )

    async def open_event_websocket(
        self,
    ) -> aiohttp.ClientWebSocketResponse:
        info = await self.server_info()
        endpoints = info.get("endpoints") or {}
        raw_url = endpoints.get("ws_events")
        if not isinstance(raw_url, str):
            raw_url = f"{self.settings.beeper_base_url}/v1/ws"

        ws_url = self._event_websocket_url(raw_url)

        ws = await self.session.ws_connect(
            ws_url,
            headers=self._headers(),
            heartbeat=30,
            autoping=True,
            timeout=cast(Any, aiohttp.ClientWSTimeout)(
                ws_receive=None,
                ws_close=10,
            ),
        )
        try:
            ready = await self._receive_json(ws, timeout=10)
            if ready.get("type") != "ready":
                raise BeeperError(
                    f"Expected Beeper WebSocket ready, got "
                    f"{ready.get('type')!r}"
                )

            request_id = "sidecar-all-chats"
            await ws.send_json(
                {
                    "type": "subscriptions.set",
                    "requestID": request_id,
                    "chatIDs": ["*"],
                }
            )
            updated = await self._receive_json(ws, timeout=10)
            response_request_id = updated.get("requestID")
            if (
                updated.get("type") != "subscriptions.updated"
                or (
                    response_request_id is not None
                    and response_request_id != request_id
                )
            ):
                raise BeeperError(
                    "Beeper WebSocket did not confirm subscriptions.set"
                )
            logger.info("Beeper WebSocket subscribed to all chats")
            return ws
        except Exception:
            await ws.close()
            raise

    @staticmethod
    async def _receive_json(
        ws: aiohttp.ClientWebSocketResponse,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        try:
            message = await asyncio.wait_for(
                ws.receive(),
                timeout=timeout,
            )
        except TimeoutError as exc:
            raise BeeperError("Timed out waiting for WebSocket data") from exc

        if message.type == aiohttp.WSMsgType.TEXT:
            try:
                value = json.loads(message.data)
            except json.JSONDecodeError as exc:
                raise BeeperError(
                    "Beeper WebSocket returned invalid JSON"
                ) from exc
            if isinstance(value, dict):
                return value
            raise BeeperError(
                "Beeper WebSocket returned non-object JSON"
            )

        if message.type in {
            aiohttp.WSMsgType.CLOSE,
            aiohttp.WSMsgType.CLOSED,
            aiohttp.WSMsgType.CLOSING,
        }:
            raise BeeperError("Beeper WebSocket closed")
        if message.type == aiohttp.WSMsgType.ERROR:
            raise BeeperError(
                f"Beeper WebSocket error: {ws.exception()!r}"
            )

        return {"type": "_ignored"}

    async def websocket_events(
        self,
        ws: aiohttp.ClientWebSocketResponse,
    ) -> AsyncIterator[dict[str, Any]]:
        async for message in ws:
            if message.type == aiohttp.WSMsgType.TEXT:
                try:
                    value = json.loads(message.data)
                except json.JSONDecodeError:
                    logger.warning(
                        "Ignoring invalid JSON from Beeper WebSocket"
                    )
                    continue
                if isinstance(value, dict):
                    yield value
            elif message.type == aiohttp.WSMsgType.ERROR:
                raise BeeperError(
                    f"Beeper WebSocket error: {ws.exception()!r}"
                )
            elif message.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
            }:
                break
