from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, TypeVar

from .identity import SourceIdentity
from .models import (
    PendingDelivery,
    SourceEvent,
    Subscription,
    canonical_json,
    stable_id,
)

_T = TypeVar("_T")

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
    source_system TEXT NOT NULL,
    source_instance TEXT NOT NULL,
    canonical_identity TEXT NOT NULL UNIQUE,
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
    created_at TEXT NOT NULL,
    UNIQUE (source_system, source_instance, account_id, chat_id, message_id)
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

CREATE TABLE IF NOT EXISTS schema_meta (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._async_lock = asyncio.Lock()
        self.source_instance = "default"

    async def run_async(
        self,
        function: Callable[..., _T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        async with self._async_lock:
            return await asyncio.to_thread(function, *args, **kwargs)

    def initialize(self, source_instance: str = "default") -> None:
        if not source_instance or not isinstance(source_instance, str):
            raise ValueError("source_instance must be a non-empty string")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass

        with self._connect() as conn:
            old_table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='source_events'"
            ).fetchone()
            legacy = old_table is not None and "source_system" not in {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(source_events)")
            }
            version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if version > 2:
                raise RuntimeError("SQLite schema is newer than this sidecar")
            if legacy:
                self._backup_legacy(conn)
                self._migrate_legacy(conn, source_instance)
            else:
                if version == 1:
                    raise RuntimeError(
                        "Schema version is v1, but v1 source columns are absent"
                    )
                conn.executescript(_SCHEMA)
                with conn:
                    conn.execute(
                        "INSERT INTO schema_meta(name,value) VALUES('source_instance',?) "
                        "ON CONFLICT(name) DO NOTHING",
                        (source_instance,),
                    )
                    stored = conn.execute(
                        "SELECT value FROM schema_meta WHERE name='source_instance'"
                    ).fetchone()
                    if stored is None or stored["value"] != source_instance:
                        raise ValueError(
                            "source_instance differs from persisted database identity"
                        )
                    conn.execute("PRAGMA user_version=2")
                    if conn.execute("PRAGMA foreign_key_check").fetchone():
                        raise sqlite3.IntegrityError(
                            "Foreign key violation during initialization"
                        )

        self.source_instance = source_instance
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def _backup_legacy(self, conn: sqlite3.Connection) -> None:
        # A fail-closed, once-only backup. Do not overwrite a prior recovery copy.
        backup_path = self.path.with_name(self.path.name + ".pre-v2.bak")
        if backup_path.exists():
            raise RuntimeError(
                "Pre-v2 backup already exists; inspect migration before retry"
            )
        descriptor = os.open(
            backup_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600
        )
        os.close(descriptor)
        try:
            with closing(sqlite3.connect(backup_path)) as destination:
                conn.backup(destination)
            os.chmod(backup_path, 0o600)
        except Exception:
            backup_path.unlink(missing_ok=True)
            raise

    @staticmethod
    def _migrate_legacy(conn: sqlite3.Connection, source_instance: str) -> None:
        # Both tables are rebuilt in one transaction, with FKs kept enabled.
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            CREATE TABLE source_events_v2 (
                source_key TEXT PRIMARY KEY,
                source_event_id TEXT NOT NULL UNIQUE,
                source_system TEXT NOT NULL,
                source_instance TEXT NOT NULL,
                canonical_identity TEXT NOT NULL UNIQUE,
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
                created_at TEXT NOT NULL,
                UNIQUE (source_system,source_instance,account_id,chat_id,message_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE deliveries_v2 (
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
                PRIMARY KEY (subscription_id,source_key),
                FOREIGN KEY (subscription_id) REFERENCES subscriptions(id),
                FOREIGN KEY (source_key) REFERENCES source_events_v2(source_key)
            )
            """
        )
        mapping: dict[str, str] = {}
        rows = conn.execute("SELECT * FROM source_events").fetchall()
        for row in rows:
            identity = SourceIdentity(
                "beeper", source_instance,
                row["account_id"], row["chat_id"], row["message_id"],
            )
            conn.execute(
                """
                INSERT INTO source_events_v2 (
                    source_key,source_event_id,source_system,source_instance,
                    canonical_identity,name,occurred_at,account_id,chat_id,
                    local_chat_id,network,message_id,sender_id,sender_name,
                    discovered_via,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    identity.source_key, identity.source_event_id,
                    identity.source_system, identity.source_instance,
                    identity.canonical, row["name"], row["occurred_at"],
                    row["account_id"], row["chat_id"], row["local_chat_id"],
                    row["network"], row["message_id"], row["sender_id"],
                    row["sender_name"], row["discovered_via"], row["created_at"],
                ),
            )
            mapping[str(row["source_key"])] = identity.source_key

        for row in conn.execute("SELECT * FROM deliveries").fetchall():
            if row["source_key"] not in mapping:
                raise sqlite3.IntegrityError(
                    "Unresolvable legacy delivery source reference"
                )
            conn.execute(
                """
                INSERT INTO deliveries_v2 (
                    subscription_id,source_key,event_id,status,attempt_count,
                    next_attempt_at,last_attempt_at,last_status_code,last_error,delivered_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row["subscription_id"],mapping[row["source_key"]],
                    row["event_id"],row["status"],row["attempt_count"],
                    row["next_attempt_at"],row["last_attempt_at"],
                    row["last_status_code"],row["last_error"],row["delivered_at"],
                ),
            )
        conn.execute("DROP TABLE deliveries")
        conn.execute("DROP TABLE source_events")
        conn.execute("ALTER TABLE source_events_v2 RENAME TO source_events")
        conn.execute("ALTER TABLE deliveries_v2 RENAME TO deliveries")
        conn.execute(
            "CREATE INDEX deliveries_due_idx ON deliveries(status, next_attempt_at)"
        )
        conn.execute(
            "CREATE TABLE schema_meta(name TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO schema_meta(name,value) VALUES('source_instance',?)",
            (source_instance,),
        )
        conn.execute("PRAGMA user_version=2")
        if conn.execute("PRAGMA foreign_key_check").fetchone():
            raise sqlite3.IntegrityError("Foreign key violation during v2 migration")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=5.0)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            with conn:
                yield conn
        finally:
            conn.close()

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
            source_system=row["source_system"],
            source_instance=row["source_instance"],
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

    def _upsert_subscription(
        self,
        conn: sqlite3.Connection,
        subscription: Subscription,
        *,
        now: str,
    ) -> None:
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
                created_at = excluded.created_at,
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
                subscription.created_at,
                now,
            ),
        )

    def upsert_subscription(
        self,
        subscription: Subscription,
        *,
        now: str,
    ) -> None:
        with self._connect() as conn:
            self._upsert_subscription(conn, subscription, now=now)

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
            conn.execute(
                """
                UPDATE deliveries
                SET status = 'cancelled',
                    last_attempt_at = ?,
                    last_error = 'subscription inactive'
                WHERE subscription_id = ?
                  AND status = 'pending'
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
        identity = SourceIdentity(
            event.source_system, event.source_instance,
            event.account_id, event.chat_id, event.message_id,
        )
        if event.source_instance != self.source_instance:
            raise ValueError("source_instance does not match database identity")
        if event.source_key != identity.source_key or event.source_event_id != identity.source_event_id:
            raise ValueError("Source identity key or event ID does not match the canonical identity")
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO source_events(
                    source_key, source_event_id, source_system, source_instance,
                    canonical_identity, name, occurred_at, account_id, chat_id,
                    local_chat_id, network, message_id, sender_id, sender_name,
                    discovered_via, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO NOTHING
                """,
                (
                    event.source_key,
                    event.source_event_id,
                    identity.source_system,
                    identity.source_instance,
                    identity.canonical,
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
            inserted = cursor.rowcount == 1
            effective_event = event
            if not inserted:
                conn.execute(
                    """
                    UPDATE source_events
                    SET local_chat_id = COALESCE(local_chat_id, ?),
                        network = COALESCE(network, ?),
                        sender_id = COALESCE(sender_id, ?),
                        sender_name = COALESCE(sender_name, ?)
                    WHERE source_key = ?
                    """,
                    (
                        event.local_chat_id,
                        event.network,
                        event.sender_id,
                        event.sender_name,
                        event.source_key,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM source_events WHERE source_key = ?",
                    (event.source_key,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        "Source event disappeared during duplicate enrichment"
                    )
                if row["canonical_identity"] != identity.canonical:
                    raise sqlite3.IntegrityError(
                        "Source key collision between distinct source identities"
                    )
                effective_event = self._event_from_row(row)

            if not deliverable:
                return inserted

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
                if not subscription.matches(effective_event):
                    continue
                event_id = stable_id(
                    "evt_", subscription.id, effective_event.source_key
                )
                conn.execute(
                    """
                    INSERT INTO deliveries(
                        subscription_id, source_key, event_id, status,
                        attempt_count, next_attempt_at
                    )
                    VALUES (?, ?, ?, 'pending', 0, ?)
                    ON CONFLICT(subscription_id, source_key) DO NOTHING
                    """,
                    (
                        subscription.id,
                        effective_event.source_key,
                        event_id,
                        now,
                    ),
                )
            return inserted

    def _enqueue_existing_events_for_subscription(
        self,
        conn: sqlite3.Connection,
        subscription: Subscription,
        *,
        now: str,
    ) -> int:
        inserted_count = 0
        rows = conn.execute(
            """
            SELECT *
            FROM source_events
            WHERE occurred_at >= ?
            ORDER BY occurred_at, source_key
            """,
            (subscription.created_at,),
        ).fetchall()
        for row in rows:
            event = self._event_from_row(row)
            if not subscription.matches(event):
                continue
            event_id = stable_id(
                "evt_", subscription.id, event.source_key
            )
            cursor = conn.execute(
                """
                INSERT INTO deliveries(
                    subscription_id, source_key, event_id, status,
                    attempt_count, next_attempt_at
                )
                VALUES (?, ?, ?, 'pending', 0, ?)
                ON CONFLICT(subscription_id, source_key) DO NOTHING
                """,
                (
                    subscription.id,
                    event.source_key,
                    event_id,
                    now,
                ),
            )
            inserted_count += cursor.rowcount
        return inserted_count

    def activate_subscription(
        self,
        subscription: Subscription,
        *,
        now: str,
        backfill_existing: bool,
    ) -> int:
        with self._connect() as conn:
            self._upsert_subscription(conn, subscription, now=now)
            if not backfill_existing:
                return 0
            return self._enqueue_existing_events_for_subscription(
                conn,
                subscription,
                now=now,
            )

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

    def refresh_pending_delivery(
        self,
        delivery: PendingDelivery,
        *,
        now: str,
    ) -> PendingDelivery | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT s.*, d.attempt_count
                FROM deliveries d
                JOIN subscriptions s ON s.id = d.subscription_id
                WHERE d.event_id = ?
                  AND d.subscription_id = ?
                  AND d.status = 'pending'
                  AND s.active = 1
                  AND s.created_at = ?
                  AND (s.expires_at IS NULL OR s.expires_at > ?)
                """,
                (
                    delivery.event_id,
                    delivery.subscription.id,
                    delivery.subscription.created_at,
                    now,
                ),
            ).fetchone()
        if row is None:
            return None
        return PendingDelivery(
            subscription=self._subscription_from_row(row),
            source_event=delivery.source_event,
            event_id=delivery.event_id,
            attempt_count=int(row["attempt_count"]),
        )

    def mark_delivery_success(
        self,
        event_id: str,
        *,
        subscription_id: str,
        subscription_created_at: str,
        now: str,
        status_code: int,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE deliveries
                SET status = 'delivered',
                    attempt_count = attempt_count + 1,
                    last_attempt_at = ?,
                    last_status_code = ?,
                    last_error = NULL,
                    delivered_at = ?
                WHERE event_id = ?
                  AND subscription_id = ?
                  AND status = 'pending'
                  AND EXISTS (
                      SELECT 1
                      FROM subscriptions s
                      WHERE s.id = deliveries.subscription_id
                        AND s.active = 1
                        AND s.created_at = ?
                  )
                """,
                (
                    now,
                    status_code,
                    now,
                    event_id,
                    subscription_id,
                    subscription_created_at,
                ),
            )
        return cursor.rowcount == 1

    def mark_delivery_retry(
        self,
        event_id: str,
        *,
        subscription_id: str,
        subscription_created_at: str,
        now: str,
        next_attempt_at: str,
        status_code: int | None,
        error: str,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE deliveries
                SET status = 'pending',
                    attempt_count = attempt_count + 1,
                    last_attempt_at = ?,
                    next_attempt_at = ?,
                    last_status_code = ?,
                    last_error = ?
                WHERE event_id = ?
                  AND subscription_id = ?
                  AND status = 'pending'
                  AND EXISTS (
                      SELECT 1
                      FROM subscriptions s
                      WHERE s.id = deliveries.subscription_id
                        AND s.active = 1
                        AND s.created_at = ?
                  )
                """,
                (
                    now,
                    next_attempt_at,
                    status_code,
                    error[:2000],
                    event_id,
                    subscription_id,
                    subscription_created_at,
                ),
            )
        return cursor.rowcount == 1

    def mark_delivery_dead(
        self,
        event_id: str,
        *,
        subscription_id: str,
        subscription_created_at: str,
        now: str,
        status_code: int | None,
        error: str,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE deliveries
                SET status = 'dead_letter',
                    attempt_count = attempt_count + 1,
                    last_attempt_at = ?,
                    last_status_code = ?,
                    last_error = ?
                WHERE event_id = ?
                  AND subscription_id = ?
                  AND status = 'pending'
                  AND EXISTS (
                      SELECT 1
                      FROM subscriptions s
                      WHERE s.id = deliveries.subscription_id
                        AND s.active = 1
                        AND s.created_at = ?
                  )
                """,
                (
                    now,
                    status_code,
                    error[:2000],
                    event_id,
                    subscription_id,
                    subscription_created_at,
                ),
            )
        return cursor.rowcount == 1

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
