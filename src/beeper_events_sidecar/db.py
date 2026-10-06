from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from .models import (
    PendingDelivery,
    SourceEvent,
    Subscription,
    canonical_json,
    stable_id,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    principal TEXT NOT NULL,
    name TEXT NOT NULL,
    arguments_json TEXT NOT NULL,
    callback_url TEXT NOT NULL,
    secret TEXT NOT NULL,
    previous_secret TEXT,
    previous_secret_expires_at TEXT,
    expires_at TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS callback_verifications (
    principal TEXT NOT NULL,
    callback_url TEXT NOT NULL,
    verified_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (principal, callback_url)
);

CREATE TABLE IF NOT EXISTS source_events (
    source_key TEXT PRIMARY KEY,
    source_event_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    account_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    local_chat_id TEXT,
    network TEXT,
    message_id TEXT NOT NULL,
    sender_id TEXT,
    sender_name TEXT,
    discovered_via TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    subscription_id TEXT NOT NULL,
    source_key TEXT NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_attempt_at TEXT,
    last_status_code INTEGER,
    last_error TEXT,
    delivered_at TEXT,
    PRIMARY KEY (subscription_id, source_key),
    FOREIGN KEY (subscription_id) REFERENCES subscriptions(id),
    FOREIGN KEY (source_key) REFERENCES source_events(source_key)
);

CREATE INDEX IF NOT EXISTS deliveries_due_idx
ON deliveries(status, next_attempt_at);

CREATE TABLE IF NOT EXISTS checkpoints (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @staticmethod
    def _subscription_from_row(row: sqlite3.Row) -> Subscription:
        return Subscription(
            id=row["id"],
            principal=row["principal"],
            name=row["name"],
            arguments=json.loads(row["arguments_json"]),
            callback_url=row["callback_url"],
            secret=row["secret"],
            previous_secret=row["previous_secret"],
            previous_secret_expires_at=row["previous_secret_expires_at"],
            expires_at=row["expires_at"],
            active=bool(row["active"]),
            created_at=row["created_at"],
        )

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> SourceEvent:
        return SourceEvent(
            source_key=row["source_key"],
            source_event_id=row["source_event_id"],
            name=row["name"],
            occurred_at=row["occurred_at"],
            account_id=row["account_id"],
            chat_id=row["chat_id"],
            local_chat_id=row["local_chat_id"],
            network=row["network"],
            message_id=row["message_id"],
            sender_id=row["sender_id"],
            sender_name=row["sender_name"],
            discovered_via=row["discovered_via"],
        )

    def get_checkpoint(self, name: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM checkpoints WHERE name = ?", (name,)
            ).fetchone()
        return None if row is None else str(row["value"])

    def set_checkpoint(self, name: str, value: str, now: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO checkpoints(name, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (name, value, now),
            )

    def get_subscription(self, subscription_id: str) -> Subscription | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM subscriptions WHERE id = ?",
                (subscription_id,),
            ).fetchone()
        return None if row is None else self._subscription_from_row(row)

    def upsert_subscription(
        self,
        subscription: Subscription,
        *,
        now: str,
    ) -> None:
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT created_at FROM subscriptions WHERE id = ?",
                (subscription.id,),
            ).fetchone()
            created_at = now if existing is None else existing["created_at"]
            conn.execute(
                """
                INSERT INTO subscriptions(
                    id, principal, name, arguments_json, callback_url,
                    secret, previous_secret, previous_secret_expires_at,
                    expires_at, active, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    principal = excluded.principal,
                    name = excluded.name,
                    arguments_json = excluded.arguments_json,
                    callback_url = excluded.callback_url,
                    secret = excluded.secret,
                    previous_secret = excluded.previous_secret,
                    previous_secret_expires_at =
                        excluded.previous_secret_expires_at,
                    expires_at = excluded.expires_at,
                    active = 1,
                    updated_at = excluded.updated_at
                """,
                (
                    subscription.id,
                    subscription.principal,
                    subscription.name,
                    canonical_json(subscription.arguments),
                    subscription.callback_url,
                    subscription.secret,
                    subscription.previous_secret,
                    subscription.previous_secret_expires_at,
                    subscription.expires_at,
                    created_at,
                    now,
                ),
            )

    def deactivate_subscription(self, subscription_id: str, now: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE subscriptions
                SET active = 0, updated_at = ?
                WHERE id = ?
                """,
                (now, subscription_id),
            )

    def callback_verification_valid(
        self,
        principal: str,
        callback_url: str,
        now: str,
    ) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM callback_verifications
                WHERE principal = ? AND callback_url = ?
                  AND expires_at > ?
                """,
                (principal, callback_url, now),
            ).fetchone()
        return row is not None

    def mark_callback_verified(
        self,
        principal: str,
        callback_url: str,
        *,
        verified_at: str,
        expires_at: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO callback_verifications(
                    principal, callback_url, verified_at, expires_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(principal, callback_url) DO UPDATE SET
                    verified_at = excluded.verified_at,
                    expires_at = excluded.expires_at
                """,
                (principal, callback_url, verified_at, expires_at),
            )

    def list_active_subscriptions(self, now: str) -> list[Subscription]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM subscriptions
                WHERE active = 1
                  AND (expires_at IS NULL OR expires_at > ?)
                """,
                (now,),
            ).fetchall()
        return [self._subscription_from_row(row) for row in rows]

    def record_event_and_enqueue(
        self,
        event: SourceEvent,
        *,
        now: str,
        deliverable: bool = True,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO source_events(
                    source_key, source_event_id, name, occurred_at,
                    account_id, chat_id, local_chat_id, network,
                    message_id, sender_id, sender_name, discovered_via,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.source_key,
                    event.source_event_id,
                    event.name,
                    event.occurred_at,
                    event.account_id,
                    event.chat_id,
                    event.local_chat_id,
                    event.network,
                    event.message_id,
                    event.sender_id,
                    event.sender_name,
                    event.discovered_via,
                    now,
                ),
            )
            if cursor.rowcount == 0:
                return False

            if not deliverable:
                return True

            rows = conn.execute(
                """
                SELECT *
                FROM subscriptions
                WHERE active = 1
                  AND (expires_at IS NULL OR expires_at > ?)
                """,
                (now,),
            ).fetchall()
            for row in rows:
                subscription = self._subscription_from_row(row)
                if not subscription.matches(event):
                    continue
                event_id = stable_id(
                    "evt_", subscription.id, event.source_key
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO deliveries(
                        subscription_id, source_key, event_id, status,
                        attempt_count, next_attempt_at
                    )
                    VALUES (?, ?, ?, 'pending', 0, ?)
                    """,
                    (
                        subscription.id,
                        event.source_key,
                        event_id,
                        now,
                    ),
                )
            return True

    def get_due_deliveries(
        self, now: str, limit: int = 20
    ) -> list[PendingDelivery]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    d.event_id, d.attempt_count,
                    s.id AS s_id, s.principal AS s_principal,
                    s.name AS s_name, s.arguments_json AS s_arguments_json,
                    s.callback_url AS s_callback_url,
                    s.secret AS s_secret,
                    s.previous_secret AS s_previous_secret,
                    s.previous_secret_expires_at
                        AS s_previous_secret_expires_at,
                    s.expires_at AS s_expires_at,
                    s.active AS s_active,
                    s.created_at AS s_created_at,
                    e.*
                FROM deliveries d
                JOIN subscriptions s ON s.id = d.subscription_id
                JOIN source_events e ON e.source_key = d.source_key
                WHERE d.status = 'pending'
                  AND d.next_attempt_at <= ?
                  AND s.active = 1
                  AND (s.expires_at IS NULL OR s.expires_at > ?)
                ORDER BY d.next_attempt_at, d.event_id
                LIMIT ?
                """,
                (now, now, limit),
            ).fetchall()

        deliveries: list[PendingDelivery] = []
        for row in rows:
            subscription = Subscription(
                id=row["s_id"],
                principal=row["s_principal"],
                name=row["s_name"],
                arguments=json.loads(row["s_arguments_json"]),
                callback_url=row["s_callback_url"],
                secret=row["s_secret"],
                previous_secret=row["s_previous_secret"],
                previous_secret_expires_at=row[
                    "s_previous_secret_expires_at"
                ],
                expires_at=row["s_expires_at"],
                active=bool(row["s_active"]),
                created_at=row["s_created_at"],
            )
            deliveries.append(
                PendingDelivery(
                    subscription=subscription,
                    source_event=self._event_from_row(row),
                    event_id=row["event_id"],
                    attempt_count=int(row["attempt_count"]),
                )
            )
        return deliveries

    def mark_delivery_success(
        self,
        event_id: str,
        *,
        now: str,
        status_code: int,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE deliveries
                SET status = 'delivered',
                    attempt_count = attempt_count + 1,
                    last_attempt_at = ?,
                    last_status_code = ?,
                    last_error = NULL,
                    delivered_at = ?
                WHERE event_id = ?
                """,
                (now, status_code, now, event_id),
            )

    def mark_delivery_retry(
        self,
        event_id: str,
        *,
        now: str,
        next_attempt_at: str,
        status_code: int | None,
        error: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE deliveries
                SET status = 'pending',
                    attempt_count = attempt_count + 1,
                    last_attempt_at = ?,
                    next_attempt_at = ?,
                    last_status_code = ?,
                    last_error = ?
                WHERE event_id = ?
                """,
                (
                    now,
                    next_attempt_at,
                    status_code,
                    error[:2000],
                    event_id,
                ),
            )

    def mark_delivery_dead(
        self,
        event_id: str,
        *,
        now: str,
        status_code: int | None,
        error: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE deliveries
                SET status = 'dead_letter',
                    attempt_count = attempt_count + 1,
                    last_attempt_at = ?,
                    last_status_code = ?,
                    last_error = ?
                WHERE event_id = ?
                """,
                (now, status_code, error[:2000], event_id),
            )

    def stats(self, now: str) -> dict[str, Any]:
        with self._connect() as conn:
            active_subscriptions = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM subscriptions
                WHERE active = 1
                  AND (expires_at IS NULL OR expires_at > ?)
                """,
                (now,),
            ).fetchone()["n"]
            pending = conn.execute(
                "SELECT COUNT(*) AS n FROM deliveries WHERE status='pending'"
            ).fetchone()["n"]
            dead = conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM deliveries
                WHERE status='dead_letter'
                """
            ).fetchone()["n"]
            oldest = conn.execute(
                """
                SELECT MIN(next_attempt_at) AS value
                FROM deliveries
                WHERE status='pending'
                """
            ).fetchone()["value"]
            source_events = conn.execute(
                "SELECT COUNT(*) AS n FROM source_events"
            ).fetchone()["n"]
        return {
            "active_subscriptions": int(active_subscriptions),
            "pending_deliveries": int(pending),
            "dead_letter_deliveries": int(dead),
            "oldest_pending_at": oldest,
            "source_events_seen": int(source_events),
        }
