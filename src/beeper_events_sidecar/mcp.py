from __future__ import annotations

import hmac
import json
import logging
from datetime import timedelta
from typing import Any

from aiohttp import web

from .config import Settings
from .db import Database
from .models import (
    Subscription,
    canonical_json,
    isoformat_z,
    parse_timestamp,
    stable_id,
    utc_now,
)
from .service import EventService
from .webhooks import CallbackEndpointError, WebhookSender, validate_webhook_secret

logger = logging.getLogger(__name__)
_PROTOCOL_VERSION = "2026-07-28"
_PRINCIPAL = "private-secure-tunnel"
_EVENT_NAME = "message.created"


class RpcError(RuntimeError):
    def __init__(
        self,
        code: int,
        message: str,
        *,
        data: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


class McpApi:
    def __init__(
        self,
        *,
        settings: Settings,
        db: Database,
        service: EventService,
        webhook_sender: WebhookSender,
    ) -> None:
        self.settings = settings
        self.db = db
        self.service = service
        self.webhook_sender = webhook_sender
        self._subscription_mutations: dict[str, int] = {}

    def _next_subscription_mutation(self, subscription_id: str) -> int:
        token = self._subscription_mutations.get(subscription_id, 0) + 1
        self._subscription_mutations[subscription_id] = token
        return token

    def _require_current_subscription_mutation(
        self,
        subscription_id: str,
        token: int,
    ) -> None:
        if self._subscription_mutations.get(subscription_id) != token:
            raise RpcError(
                -32016,
                "Subscription request was superseded by a newer mutation",
            )

    def application(self) -> web.Application:
        app = web.Application(
            client_max_size=4 * 1024 * 1024,
            middlewares=[self._auth_middleware],
        )
        app.router.add_post("/mcp", self.handle_mcp)
        app.router.add_get("/healthz", self.health)
        app.router.add_get("/readyz", self.ready)
        return app

    @web.middleware
    async def _auth_middleware(
        self,
        request: web.Request,
        handler: Any,
    ) -> web.StreamResponse:
        if request.path != "/mcp":
            return await handler(request)

        try:
            expected = self.settings.mcp_bearer_file.read_text(
                encoding="utf-8"
            ).strip()
        except OSError:
            logger.exception("Could not read MCP bearer secret")
            return web.json_response(
                {"error": "server authentication unavailable"},
                status=503,
            )

        if not expected.startswith("Bearer "):
            logger.error("MCP bearer secret is malformed")
            return web.json_response(
                {"error": "server authentication unavailable"},
                status=503,
            )

        presented = request.headers.get("Authorization", "")
        if not hmac.compare_digest(presented, expected):
            return web.json_response(
                {"error": "unauthorized"},
                status=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
        return await handler(request)

    async def health(self, _request: web.Request) -> web.Response:
        try:
            status = self.service.status()
        except Exception:
            logger.exception("Health check failed")
            return web.json_response(
                {"status": "unhealthy"},
                status=503,
            )
        if not status.get("workers_healthy", False):
            return web.json_response(
                {
                    "status": "unhealthy",
                    "workers": status.get("workers", {}),
                },
                status=503,
            )
        return web.json_response({"status": "ok"})

    async def ready(self, _request: web.Request) -> web.Response:
        # Readiness means the durable MCP/event service can accept
        # subscriptions. Beeper source health is intentionally separate so
        # a temporary messaging outage does not make the control plane vanish.
        try:
            status = self.service.status()
        except Exception:
            logger.exception("Readiness check failed")
            return web.json_response(
                {"status": "not_ready"},
                status=503,
            )
        if not status.get("workers_healthy", False):
            return web.json_response(
                {
                    "status": "not_ready",
                    "source_connected": status["source_connected"],
                    "workers": status.get("workers", {}),
                },
                status=503,
            )
        return web.json_response(
            {
                "status": "ready",
                "source_connected": status["source_connected"],
            }
        )

    async def handle_mcp(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except (json.JSONDecodeError, ValueError):
            return self._error_response(None, -32700, "Parse error")

        if not isinstance(body, dict) or body.get("jsonrpc") != "2.0":
            return self._error_response(
                body.get("id") if isinstance(body, dict) else None,
                -32600,
                "Invalid Request",
            )

        request_id = body.get("id")
        method = body.get("method")
        params = body.get("params") or {}
        if not isinstance(method, str) or not isinstance(params, dict):
            return self._error_response(
                request_id, -32600, "Invalid Request"
            )

        # Notifications carry no id and require no response body.
        if request_id is None:
            return web.Response(status=202)

        try:
            result = await self._dispatch(method, params)
            return web.json_response(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": result,
                }
            )
        except RpcError as exc:
            return self._error_response(
                request_id,
                exc.code,
                exc.message,
                data=exc.data,
            )
        except Exception:
            logger.exception("Unhandled MCP method failure: %s", method)
            return self._error_response(
                request_id,
                -32603,
                "Internal error",
            )

    async def _dispatch(
        self,
        method: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        if method == "server/discover":
            return self._discover()
        if method == "events/list":
            return self._events_list(params)
        if method == "events/subscribe":
            return await self._events_subscribe(params)
        if method == "events/unsubscribe":
            return self._events_unsubscribe(params)
        if method == "tools/list":
            return self._tools_list()
        if method == "tools/call":
            return self._tools_call(params)
        raise RpcError(-32601, f"Method not found: {method}")

    @staticmethod
    def _discover() -> dict[str, Any]:
        return {
            "resultType": "complete",
            "supportedVersions": [_PROTOCOL_VERSION],
            "capabilities": {
                "tools": {},
                "events": {},
            },
        }

    @staticmethod
    def _events_list(
        _params: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "events": [
                {
                    "name": _EVENT_NAME,
                    "description": (
                        "A newly observed incoming Beeper message. "
                        "The payload intentionally omits message text; "
                        "use the native Beeper MCP app to read context."
                    ),
                    "delivery": ["webhook"],
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "account_ids": {
                                "type": "array",
                                "items": {"type": "string"},
                                "uniqueItems": True,
                                "description": (
                                    "Optional Beeper account IDs to monitor."
                                ),
                            },
                            "chat_ids": {
                                "type": "array",
                                "items": {"type": "string"},
                                "uniqueItems": True,
                                "description": (
                                    "Optional canonical or local Beeper "
                                    "chat IDs to monitor."
                                ),
                            },
                            "sender_ids": {
                                "type": "array",
                                "items": {"type": "string"},
                                "uniqueItems": True,
                                "description": (
                                    "Optional Beeper sender IDs to monitor."
                                ),
                            },
                        },
                        "additionalProperties": False,
                    },
                    "payloadSchema": {
                        "type": "object",
                        "properties": {
                            "account_id": {"type": "string"},
                            "chat_id": {"type": "string"},
                            "local_chat_id": {"type": "string"},
                            "network": {"type": "string"},
                            "message_id": {"type": "string"},
                            "sender_id": {"type": "string"},
                            "sender_name": {"type": "string"},
                            "timestamp": {
                                "type": "string",
                                "format": "date-time",
                            },
                        },
                        "required": [
                            "account_id",
                            "chat_id",
                            "message_id",
                            "timestamp",
                        ],
                        "additionalProperties": False,
                    },
                }
            ]
        }

    async def _events_subscribe(
        self,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        name = params.get("name")
        raw_arguments = params.get("arguments")
        delivery = params.get("delivery") or {}

        if name != _EVENT_NAME:
            raise RpcError(-32602, f"Unsupported event: {name!r}")
        arguments = self._canonicalize_arguments(raw_arguments)
        if params.get("cursor") is not None:
            raise RpcError(
                -32602,
                "message.created does not support protocol replay; "
                "cursor must be null",
            )

        if not isinstance(delivery, dict):
            raise RpcError(-32602, "delivery must be an object")
        if delivery.get("mode") != "webhook":
            raise RpcError(
                -32602, "Only webhook event delivery is supported"
            )

        callback_url = delivery.get("url")
        secret = delivery.get("secret")
        if not isinstance(callback_url, str) or not callback_url:
            raise RpcError(-32602, "delivery.url is required")
        if not isinstance(secret, str) or not secret:
            raise RpcError(-32602, "delivery.secret is required")
        try:
            validate_webhook_secret(secret)
        except ValueError as exc:
            raise RpcError(-32602, str(exc)) from exc

        identity = stable_id(
            "sub_",
            _PRINCIPAL,
            callback_url,
            name,
            canonical_json(arguments),
        )
        mutation_token = self._next_subscription_mutation(identity)
        existing = self.db.get_subscription(identity)

        now = utc_now()
        now_text = isoformat_z(now)
        existing_is_live = (
            existing is not None
            and existing.active
            and (
                existing.expires_at is None
                or parse_timestamp(existing.expires_at) > now
            )
        )
        if not self.db.callback_verification_valid(
            _PRINCIPAL,
            callback_url,
            now_text,
        ):
            try:
                await self.webhook_sender.verify_callback(
                    callback_url,
                    secret,
                    identity,
                )
            except CallbackEndpointError as exc:
                raise RpcError(
                    -32015,
                    "CallbackEndpointError",
                    data={"reason": exc.reason},
                ) from exc
            self._require_current_subscription_mutation(
                identity,
                mutation_token,
            )
            self.db.mark_callback_verified(
                _PRINCIPAL,
                callback_url,
                verified_at=now_text,
                expires_at=isoformat_z(
                    now
                    + timedelta(
                        seconds=(
                            self.settings
                            .callback_verification_cache_seconds
                        )
                    )
                ),
            )

        if existing is not None and not existing_is_live:
            self.db.deactivate_subscription(existing.id, now_text)

        previous_secret: str | None = None
        previous_secret_expires_at: str | None = None
        if existing_is_live and existing is not None:
            if existing.secret != secret:
                previous_secret = existing.secret
                previous_secret_expires_at = isoformat_z(
                    now
                    + timedelta(
                        seconds=self.settings.secret_rotation_seconds
                    )
                )
            else:
                previous_secret = existing.previous_secret
                previous_secret_expires_at = (
                    existing.previous_secret_expires_at
                )

        ttl_present = "ttlMs" in params
        ttl_ms = params.get("ttlMs")
        expires_at: str | None
        if ttl_present and ttl_ms is None:
            expires_at = None
        elif ttl_present:
            if (
                not isinstance(ttl_ms, int)
                or isinstance(ttl_ms, bool)
                or ttl_ms <= 0
            ):
                raise RpcError(
                    -32602, "ttlMs must be a positive integer or null"
                )
            # A finite cap bounds forgotten subscriptions while still
            # leaving plenty of time for ChatGPT refresh.
            granted_ms = min(ttl_ms, 30 * 24 * 60 * 60 * 1000)
            expires_at = isoformat_z(
                now + timedelta(milliseconds=granted_ms)
            )
        else:
            expires_at = isoformat_z(
                now
                + timedelta(
                    seconds=self.settings.default_subscription_ttl_seconds
                )
            )

        created_at = (
            existing.created_at
            if existing_is_live and existing is not None
            else now_text
        )
        subscription = Subscription(
            id=identity,
            principal=_PRINCIPAL,
            name=name,
            arguments=arguments,
            callback_url=callback_url,
            secret=secret,
            previous_secret=previous_secret,
            previous_secret_expires_at=previous_secret_expires_at,
            expires_at=expires_at,
            active=True,
            created_at=created_at,
        )
        self.db.activate_subscription(
            subscription,
            now=now_text,
            backfill_existing=not existing_is_live,
        )

        return {
            "id": identity,
            "refreshBefore": expires_at,
            "cursor": None,
            "truncated": False,
        }

    def _events_unsubscribe(
        self,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        name = params.get("name")
        raw_arguments = params.get("arguments")
        delivery = params.get("delivery") or {}
        if name != _EVENT_NAME:
            # Unsubscribe is deliberately idempotent.
            return {}
        arguments = self._canonicalize_arguments(raw_arguments)

        if not isinstance(delivery, dict):
            raise RpcError(-32602, "delivery must be an object")
        if delivery.get("mode") != "webhook":
            raise RpcError(
                -32602, "Only webhook event delivery is supported"
            )
        callback_url = delivery.get("url")
        if not isinstance(callback_url, str) or not callback_url:
            raise RpcError(-32602, "delivery.url is required")

        identity = stable_id(
            "sub_",
            _PRINCIPAL,
            callback_url,
            name,
            canonical_json(arguments),
        )
        self._next_subscription_mutation(identity)
        self.db.deactivate_subscription(
            identity, isoformat_z(utc_now())
        )
        return {}

    @staticmethod
    def _canonicalize_arguments(arguments: Any) -> dict[str, list[str]]:
        if arguments is None:
            return {}
        if not isinstance(arguments, dict):
            raise RpcError(-32602, "arguments must be an object")
        allowed = {"account_ids", "chat_ids", "sender_ids"}
        unknown = set(arguments) - allowed
        if unknown:
            raise RpcError(
                -32602,
                f"Unknown event arguments: {sorted(unknown)!r}",
            )

        canonical: dict[str, list[str]] = {}
        for key, value in arguments.items():
            if not isinstance(value, list) or not all(
                isinstance(item, str) and item for item in value
            ):
                raise RpcError(
                    -32602,
                    f"{key} must be an array of non-empty strings",
                )
            if len(value) != len(set(value)):
                raise RpcError(
                    -32602, f"{key} must not contain duplicates"
                )
            if value:
                canonical[key] = sorted(value)
        return canonical

    @staticmethod
    def _tools_list() -> dict[str, Any]:
        return {
            "tools": [
                {
                    "name": "events_status",
                    "description": (
                        "Read operational status for the isolated Beeper "
                        "Events sidecar. This never reads message content "
                        "and never changes messaging state."
                    ),
                    "inputSchema": {
                        "type": "object",
                        "properties": {},
                        "additionalProperties": False,
                    },
                    "annotations": {
                        "readOnlyHint": True,
                        "destructiveHint": False,
                        "idempotentHint": True,
                        "openWorldHint": False,
                    },
                }
            ]
        }

    def _tools_call(
        self,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if name != "events_status":
            raise RpcError(-32602, f"Unknown tool: {name!r}")
        if arguments not in ({}, None):
            raise RpcError(
                -32602, "events_status accepts no arguments"
            )
        status = self.service.status()
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        status,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                }
            ],
            "structuredContent": status,
            "isError": False,
        }

    @staticmethod
    def _error_response(
        request_id: Any,
        code: int,
        message: str,
        *,
        data: dict[str, Any] | None = None,
    ) -> web.Response:
        error: dict[str, Any] = {
            "code": code,
            "message": message,
        }
        if data is not None:
            error["data"] = data
        return web.json_response(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": error,
            }
        )
