from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat_z(value: datetime) -> str:
    value = value.astimezone(UTC)
    return value.isoformat().replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def stable_id(prefix: str, *parts: str, length: int = 32) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"{prefix}{digest[:length]}"


@dataclass(frozen=True, slots=True)
class SourceEvent:
    source_key: str
    source_event_id: str
    name: str
    occurred_at: str
    account_id: str
    chat_id: str
    local_chat_id: str | None
    network: str | None
    message_id: str
    sender_id: str | None
    sender_name: str | None
    discovered_via: str

    def payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "account_id": self.account_id,
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "timestamp": self.occurred_at,
        }
        if self.local_chat_id:
            payload["local_chat_id"] = self.local_chat_id
        if self.network:
            payload["network"] = self.network
        if self.sender_id:
            payload["sender_id"] = self.sender_id
        if self.sender_name:
            payload["sender_name"] = self.sender_name
        return payload


@dataclass(frozen=True, slots=True)
class Subscription:
    id: str
    principal: str
    name: str
    arguments: dict[str, Any]
    callback_url: str
    secret: str
    previous_secret: str | None
    previous_secret_expires_at: str | None
    expires_at: str | None
    active: bool
    created_at: str

    def matches(self, event: SourceEvent) -> bool:
        if parse_timestamp(event.occurred_at) < parse_timestamp(
            self.created_at
        ):
            return False

        account_ids = set(self.arguments.get("account_ids") or [])
        if account_ids and event.account_id not in account_ids:
            return False

        chat_ids = set(self.arguments.get("chat_ids") or [])
        if (
            chat_ids
            and event.chat_id not in chat_ids
            and (
                not event.local_chat_id
                or event.local_chat_id not in chat_ids
            )
        ):
            return False

        sender_ids = set(self.arguments.get("sender_ids") or [])
        return not sender_ids or event.sender_id in sender_ids


@dataclass(frozen=True, slots=True)
class PendingDelivery:
    subscription: Subscription
    source_event: SourceEvent
    event_id: str
    attempt_count: int
