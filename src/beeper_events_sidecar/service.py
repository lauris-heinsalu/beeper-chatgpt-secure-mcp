from __future__ import annotations

import asyncio
import logging
import secrets
import sqlite3
from datetime import timedelta
from typing import Any

import aiohttp

from .beeper import BeeperClient, BeeperError
from .config import Settings
from .db import Database
from .models import (
    SourceEvent,
    isoformat_z,
    parse_timestamp,
    stable_id,
    utc_now,
)
from .webhooks import CallbackEndpointError, WebhookSender

logger = logging.getLogger(__name__)
_CHECKPOINT_NAME = "beeper_messages"


class EventService:
    def __init__(
        self,
        *,
        settings: Settings,
        db: Database,
        beeper: BeeperClient,
        webhook_sender: WebhookSender,
    ) -> None:
        self.settings = settings
        self.db = db
        self.beeper = beeper
        self.webhook_sender = webhook_sender

        self._stop = asyncio.Event()
        self._reconcile_requested = asyncio.Event()
        self._reconcile_lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[Any]] = []

        self.source_connected = False
        self.last_source_event_at: str | None = None
        self.last_reconcile_at: str | None = None
        self.last_source_error: str | None = None

    async def start(self) -> None:
        self.db.initialize()
        now = isoformat_z(utc_now())
        if self.db.get_checkpoint(_CHECKPOINT_NAME) is None:
            # First deployment is a clean baseline: do not turn historical
            # messages into fresh notifications.
            self.db.set_checkpoint(_CHECKPOINT_NAME, now, now)
            logger.info("Initialized reconciliation checkpoint at %s", now)

        self._reconcile_requested.set()
        self._tasks = [
            asyncio.create_task(
                self._source_loop(), name="beeper-event-source"
            ),
            asyncio.create_task(
                self._reconcile_loop(), name="beeper-reconciler"
            ),
            asyncio.create_task(
                self._delivery_loop(), name="webhook-delivery"
            ),
        ]

    async def stop(self) -> None:
        self._stop.set()
        self._reconcile_requested.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def request_reconcile(self) -> None:
        self._reconcile_requested.set()

    async def _source_loop(self) -> None:
        delay = 1
        while not self._stop.is_set():
            ws: aiohttp.ClientWebSocketResponse | None = None
            last_seq: int | None = None
            try:
                ws = await self.beeper.open_event_websocket()
                self.source_connected = True
                self.last_source_error = None
                self.request_reconcile()
                delay = 1
                logger.info("Beeper WebSocket source connected")

                async for event in self.beeper.websocket_events(ws):
                    if self._stop.is_set():
                        return

                    seq = event.get("seq")
                    if isinstance(seq, int):
                        if last_seq is not None and seq != last_seq + 1:
                            logger.warning(
                                "Beeper WebSocket sequence gap: %s -> %s",
                                last_seq,
                                seq,
                            )
                            self.request_reconcile()
                        last_seq = seq

                    if event.get("type") == "message.upserted":
                        self.last_source_event_at = isoformat_z(utc_now())
                        await self._ingest_ws_event(event)
                    elif event.get("type") == "error":
                        logger.warning(
                            "Beeper WebSocket control error: %s",
                            event.get("message"),
                        )

                raise BeeperError("Beeper WebSocket ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - daemon boundary
                self.last_source_error = f"{type(exc).__name__}: {exc}"[:1000]
                logger.warning(
                    "Beeper WebSocket source disconnected: %s",
                    self.last_source_error,
                )
            finally:
                self.source_connected = False
                if ws is not None and not ws.closed:
                    await ws.close()
                self.request_reconcile()

            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except TimeoutError:
                pass
            delay = min(delay * 2, self.settings.reconnect_max_seconds)

    async def _ingest_ws_event(self, event: dict[str, Any]) -> None:
        chat_id = event.get("chatID")
        if not isinstance(chat_id, str):
            return

        entries = event.get("entries")
        by_id: dict[str, dict[str, Any]] = {}
        if isinstance(entries, list):
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                message_id = entry.get("id") or entry.get("messageID")
                if isinstance(message_id, str):
                    by_id[message_id] = entry

        ids = event.get("ids")
        message_ids = [
            value for value in (ids or []) if isinstance(value, str)
        ]
        for message_id in by_id:
            if message_id not in message_ids:
                message_ids.append(message_id)

        for message_id in message_ids:
            message = by_id.get(message_id)
            if not self._ws_message_complete(message):
                try:
                    message = await self.beeper.message(
                        chat_id, message_id
                    )
                except Exception:
                    logger.exception(
                        "Failed to hydrate Beeper message %s",
                        message_id,
                    )
                    self.request_reconcile()
                    continue

            if message is None:
                self.request_reconcile()
                continue

            normalized = await self._normalize_message(
                message,
                discovered_via="websocket",
            )
            if normalized is None:
                continue
            inserted = self.db.record_event_and_enqueue(
                normalized,
                now=isoformat_z(utc_now()),
                deliverable=True,
            )
            if inserted:
                logger.info(
                    "Recorded incoming Beeper message event %s",
                    normalized.source_event_id,
                )

    @staticmethod
    def _ws_message_complete(
        message: dict[str, Any] | None,
    ) -> bool:
        if not isinstance(message, dict):
            return False
        required = (
            message.get("id") or message.get("messageID"),
            message.get("accountID"),
            message.get("chatID"),
            message.get("timestamp"),
        )
        return (
            all(isinstance(value, str) and value for value in required)
            and isinstance(message.get("isSender"), bool)
        )

    async def _normalize_message(
        self,
        message: dict[str, Any],
        *,
        discovered_via: str,
    ) -> SourceEvent | None:
        is_sender = message.get("isSender")
        if is_sender is True:
            return None
        if discovered_via == "websocket" and is_sender is not False:
            # Never risk turning an outgoing message into a self-wake loop.
            # The HTTP reconciliation path is explicitly filtered to
            # sender=others and can safely recover an incoming message.
            self.request_reconcile()
            return None
        if message.get("isDeleted") is True or message.get("isHidden") is True:
            return None

        message_id = message.get("id") or message.get("messageID")
        account_id = message.get("accountID")
        chat_id = message.get("chatID")
        timestamp = message.get("timestamp")

        if (
            not isinstance(message_id, str)
            or not message_id
            or not isinstance(account_id, str)
            or not account_id
            or not isinstance(chat_id, str)
            or not chat_id
            or not isinstance(timestamp, str)
            or not timestamp
        ):
            logger.warning(
                "Skipping message with incomplete identity fields"
            )
            return None

        try:
            occurred_at = isoformat_z(parse_timestamp(timestamp))
        except (TypeError, ValueError):
            logger.warning(
                "Skipping message %s with invalid timestamp",
                message_id,
            )
            return None

        local_chat_id: str | None = None
        network: str | None = None
        try:
            chat = await self.beeper.chat_metadata(chat_id)
            local = chat.get("localChatID")
            network_value = chat.get("network")
            if isinstance(local, str) and local:
                local_chat_id = local
            if isinstance(network_value, str) and network_value:
                network = network_value
        except Exception:
            # Metadata enriches the event but is not required for recovery.
            logger.exception(
                "Could not enrich chat metadata for %s", chat_id
            )

        sender_id = message.get("senderID")
        sender_name = message.get("senderName")
        if not isinstance(sender_id, str):
            sender_id = None
        if not isinstance(sender_name, str):
            sender_name = None

        source_key = (
            f"beeper:{account_id}:{chat_id}:{message_id}"
        )
        return SourceEvent(
            source_key=source_key,
            source_event_id=stable_id("src_", source_key),
            name="message.created",
            occurred_at=occurred_at,
            account_id=account_id,
            chat_id=chat_id,
            local_chat_id=local_chat_id,
            network=network,
            message_id=message_id,
            sender_id=sender_id,
            sender_name=sender_name,
            discovered_via=discovered_via,
        )

    async def _reconcile_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._reconcile_requested.wait(),
                    timeout=self.settings.reconcile_interval_seconds,
                )
            except TimeoutError:
                pass
            self._reconcile_requested.clear()
            if self._stop.is_set():
                return

            try:
                await self.reconcile_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Beeper reconciliation failed")

    async def reconcile_once(self) -> None:
        async with self._reconcile_lock:
            checkpoint = self.db.get_checkpoint(_CHECKPOINT_NAME)
            upper_bound = utc_now()
            now_text = isoformat_z(upper_bound)

            if checkpoint is None:
                self.db.set_checkpoint(
                    _CHECKPOINT_NAME, now_text, now_text
                )
                self.last_reconcile_at = now_text
                return

            lower_bound = parse_timestamp(checkpoint) - timedelta(
                seconds=self.settings.reconcile_overlap_seconds
            )
            messages = await self.beeper.search_messages(
                date_after=isoformat_z(lower_bound),
                date_before=now_text,
            )

            inserted_count = 0
            for message in messages:
                normalized = await self._normalize_message(
                    message,
                    discovered_via="reconciliation",
                )
                if normalized is None:
                    continue
                if self.db.record_event_and_enqueue(
                    normalized,
                    now=now_text,
                    deliverable=True,
                ):
                    inserted_count += 1

            # Advance only after a complete successful scan. upper_bound was
            # captured before the query, so messages arriving during the scan
            # remain eligible for the next reconciliation window.
            self.db.set_checkpoint(
                _CHECKPOINT_NAME, now_text, now_text
            )
            self.last_reconcile_at = now_text
            logger.info(
                "Reconciliation complete: scanned=%d inserted=%d",
                len(messages),
                inserted_count,
            )

    async def _delivery_loop(self) -> None:
        while not self._stop.is_set():
            try:
                now = utc_now()
                now_text = isoformat_z(now)
                deliveries = self.db.get_due_deliveries(now_text, limit=20)
                if not deliveries:
                    try:
                        await asyncio.wait_for(
                            self._stop.wait(), timeout=1.0
                        )
                    except TimeoutError:
                        pass
                    continue

                for delivery in deliveries:
                    if self._stop.is_set():
                        return
                    await self._deliver(delivery)
            except asyncio.CancelledError:
                raise
            except sqlite3.OperationalError as exc:
                logger.warning(
                    "Transient SQLite error in webhook delivery worker: %s",
                    exc,
                )
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=1.0)
                except TimeoutError:
                    pass

    async def _deliver(self, delivery: Any) -> None:
        event = {
            "eventId": delivery.event_id,
            "name": delivery.source_event.name,
            "timestamp": delivery.source_event.occurred_at,
            "data": delivery.source_event.payload(),
            "cursor": None,
        }

        status_code: int | None = None
        error = ""
        try:
            status_code = await self.webhook_sender.send_event(
                callback_url=delivery.subscription.callback_url,
                subscription_id=delivery.subscription.id,
                event=event,
                secret=delivery.subscription.secret,
                previous_secret=delivery.subscription.previous_secret,
                previous_secret_expires_at=(
                    delivery.subscription.previous_secret_expires_at
                ),
            )
            if 200 <= status_code < 300:
                self.db.mark_delivery_success(
                    delivery.event_id,
                    now=isoformat_z(utc_now()),
                    status_code=status_code,
                )
                return
            error = f"Webhook returned HTTP {status_code}"
        except CallbackEndpointError as exc:
            error = f"{exc.reason}: {exc}"
            if exc.reason in {
                "invalid_url",
                "private_address",
                "payload_too_large",
            }:
                self.db.mark_delivery_dead(
                    delivery.event_id,
                    now=isoformat_z(utc_now()),
                    status_code=None,
                    error=error,
                )
                return
        except Exception as exc:  # noqa: BLE001 - persist unexpected delivery failures
            error = f"{type(exc).__name__}: {exc}"

        attempts_after_this = delivery.attempt_count + 1
        permanent_http = (
            status_code in {410, 413}
            or (
                status_code is not None
                and 400 <= status_code < 500
                and status_code not in {408, 425, 429}
            )
        )
        if (
            permanent_http
            or attempts_after_this
            >= self.settings.delivery_max_attempts
        ):
            self.db.mark_delivery_dead(
                delivery.event_id,
                now=isoformat_z(utc_now()),
                status_code=status_code,
                error=error,
            )
            logger.error(
                "Webhook delivery moved to dead letter: %s",
                delivery.event_id,
            )
            return

        base = min(
            self.settings.delivery_base_backoff_seconds
            * (2 ** max(delivery.attempt_count, 0)),
            self.settings.delivery_max_backoff_seconds,
        )
        backoff = min(
            base * secrets.SystemRandom().uniform(1.0, 1.2),
            self.settings.delivery_max_backoff_seconds,
        )
        next_attempt = utc_now() + timedelta(seconds=backoff)
        self.db.mark_delivery_retry(
            delivery.event_id,
            now=isoformat_z(utc_now()),
            next_attempt_at=isoformat_z(next_attempt),
            status_code=status_code,
            error=error,
        )
        logger.warning(
            "Webhook delivery will retry: %s (%s)",
            delivery.event_id,
            error,
        )

    def status(self) -> dict[str, Any]:
        now = isoformat_z(utc_now())
        workers = {
            task.get_name(): (
                "cancelled"
                if task.cancelled()
                else "failed"
                if task.done() and task.exception() is not None
                else "stopped"
                if task.done()
                else "running"
            )
            for task in self._tasks
        }
        workers_healthy = len(self._tasks) == 3 and all(
            state == "running" for state in workers.values()
        )
        return {
            "service": "beeper-events-sidecar",
            "source_connected": self.source_connected,
            "last_source_event_at": self.last_source_event_at,
            "last_reconcile_at": self.last_reconcile_at,
            "last_source_error": self.last_source_error,
            "workers_healthy": workers_healthy,
            "workers": workers,
            "checkpoint": self.db.get_checkpoint(_CHECKPOINT_NAME),
            **self.db.stats(now),
        }
