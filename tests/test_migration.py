"""Stage 2: upgrade real v1-shaped SQLite state without losing delivery identity."""

import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest

from beeper_events_sidecar.db import Database
from beeper_events_sidecar.identity import SourceIdentity
from beeper_events_sidecar.models import SourceEvent

LEGACY = """
CREATE TABLE subscriptions (
 id TEXT PRIMARY KEY, principal TEXT NOT NULL, name TEXT NOT NULL,
 arguments_json TEXT NOT NULL, callback_url TEXT NOT NULL, secret TEXT NOT NULL,
 previous_secret TEXT, previous_secret_expires_at TEXT, expires_at TEXT,
 active INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE source_events (
 source_key TEXT PRIMARY KEY, source_event_id TEXT NOT NULL UNIQUE,
 name TEXT NOT NULL, occurred_at TEXT NOT NULL, account_id TEXT NOT NULL,
 chat_id TEXT NOT NULL, local_chat_id TEXT, network TEXT, message_id TEXT NOT NULL,
 sender_id TEXT, sender_name TEXT, discovered_via TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE deliveries (
 subscription_id TEXT NOT NULL, source_key TEXT NOT NULL,
 event_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL,
 attempt_count INTEGER NOT NULL, next_attempt_at TEXT NOT NULL,
 last_attempt_at TEXT, last_status_code INTEGER, last_error TEXT,
 delivered_at TEXT, PRIMARY KEY(subscription_id,source_key),
 FOREIGN KEY(subscription_id) REFERENCES subscriptions(id),
 FOREIGN KEY(source_key) REFERENCES source_events(source_key)
);
CREATE TABLE checkpoints(name TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE callback_verifications (
 principal TEXT NOT NULL, callback_url TEXT NOT NULL, verified_at TEXT NOT NULL,
 expires_at TEXT NOT NULL, PRIMARY KEY(principal,callback_url)
);
"""


def _legacy_db(path, *, duplicate=False):
    with closing(sqlite3.connect(path)) as c, c:
        c.executescript(LEGACY)
        c.execute(
            "INSERT INTO subscriptions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ("sub", "principal", "message.created", "{}", "https://example.com",
             "whsec_example", None, None, None, 1,
             "2026-10-01T00:00:00Z", "2026-10-01T00:00:00Z"),
        )
        for message, status in [("m1", "delivered"), ("m2", "pending")]:
            key = f"beeper:account:chat:{message}"
            c.execute(
                "INSERT INTO source_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (key, f"src_old_{message}", "message.created",
                 "2026-10-02T00:00:00Z", "account", "chat", None, "WhatsApp",
                 message, "sender", "Alice", "websocket", "2026-10-02T00:00:00Z"),
            )
            c.execute(
                "INSERT INTO deliveries VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("sub", key, f"evt_old_{message}", status, 1,
                 "2026-10-02T00:00:00Z", "2026-10-02T00:00:00Z", 200 if status=="delivered" else None,
                 None, "2026-10-02T00:00:00Z" if status=="delivered" else None),
            )
        c.execute(
            "INSERT INTO checkpoints VALUES (?,?,?)",
            ("beeper_messages", "2026-10-02T00:00:00Z", "2026-10-02T00:00:00Z"),
        )
        if duplicate:
            c.execute(
                "INSERT INTO source_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("malformed_duplicate", "src_old_duplicate", "message.created",
                 "2026-10-02T00:00:00Z", "account", "chat", None,
                 "WhatsApp", "m1", "sender", "Alice", "websocket",
                 "2026-10-02T00:00:00Z"),
            )


def _read(path, sql, params=()):
    with closing(sqlite3.connect(path)) as c, c:
        return c.execute(sql, params).fetchall()


def test_v1_migration_rewrites_all_keys_preserves_delivery_ids_and_checkpoint(tmp_path):
    path = tmp_path / "state.sqlite3"
    _legacy_db(path)
    db = Database(path)
    db.initialize(source_instance="personal")

    assert _read(path, "PRAGMA user_version") == [(2,)]
    assert _read(path, "PRAGMA foreign_key_check") == []
    source_rows = _read(
        path, "SELECT source_key,source_event_id,source_system,source_instance,canonical_identity,message_id FROM source_events ORDER BY message_id"
    )
    assert len(source_rows) == 2
    for key, event_id, system, instance, canonical, message in source_rows:
        identity = SourceIdentity("beeper", "personal", "account", "chat", message)
        assert (key, event_id, system, instance, canonical) == (
            identity.source_key, identity.source_event_id, "beeper", "personal", identity.canonical
        )
    assert _read(path, "SELECT event_id,status FROM deliveries ORDER BY event_id") == [
        ("evt_old_m1", "delivered"), ("evt_old_m2", "pending")
    ]
    assert _read(path, "SELECT value FROM checkpoints") == [("2026-10-02T00:00:00Z",)]
    assert (tmp_path / "state.sqlite3.pre-v2.bak").exists()

    # Reconciliation after migration must not produce a second delivery.
    identity = SourceIdentity("beeper", "personal", "account", "chat", "m2")
    repeat = SourceEvent(
        source_key=identity.source_key,
        source_event_id=identity.source_event_id,
        name="message.created",
        occurred_at="2026-10-02T00:00:00Z",
        account_id="account", chat_id="chat", local_chat_id=None,
        network="WhatsApp", message_id="m2", sender_id="sender",
        sender_name="Alice", discovered_via="reconciliation",
        source_instance="personal",
    )
    assert not db.record_event_and_enqueue(repeat, now="2026-10-03T00:00:00Z")
    assert _read(path, "SELECT count(*) FROM deliveries") == [(2,)]
    db.initialize(source_instance="personal")
    assert _read(path, "SELECT count(*) FROM source_events") == [(2,)]


def test_migration_conflict_fails_closed_and_keeps_legacy_rows(tmp_path):
    path = tmp_path / "state.sqlite3"
    _legacy_db(path, duplicate=True)
    with pytest.raises((sqlite3.IntegrityError, ValueError)):
        Database(path).initialize(source_instance="personal")
    assert _read(path, "PRAGMA user_version") == [(0,)]
    assert _read(path, "SELECT count(*) FROM source_events") == [(3,)]
    assert _read(path, "SELECT count(*) FROM deliveries") == [(2,)]
    assert (tmp_path / "state.sqlite3.pre-v2.bak").exists()


def test_source_instance_is_pinned_to_database_and_cannot_change_silently(tmp_path):
    path = tmp_path / "state.sqlite3"
    db = Database(path)
    db.initialize(source_instance="personal")
    with pytest.raises(ValueError, match="source_instance"):
        db.initialize(source_instance="other")
    assert _read(path, "PRAGMA user_version") == [(2,)]


def test_source_identity_conflict_is_not_silently_ignored(tmp_path):
    path = tmp_path / "state.sqlite3"
    db = Database(path)
    db.initialize(source_instance="personal")
    identity = SourceIdentity("beeper", "personal", "account", "chat", "m1")
    event = SourceEvent(
        source_key=identity.source_key, source_event_id=identity.source_event_id,
        name="message.created", occurred_at="2026-10-02T00:00:00Z",
        account_id="account", chat_id="chat", local_chat_id=None,
        network=None, message_id="m1", sender_id=None, sender_name=None,
        discovered_via="test", source_instance="personal",
    )
    assert db.record_event_and_enqueue(event, now="2026-10-03T00:00:00Z")
    assert not db.record_event_and_enqueue(event, now="2026-10-03T00:00:00Z")
    with pytest.raises(ValueError, match="identity"):
        db.record_event_and_enqueue(replace(event, source_key="source:v2:bogus"), now="2026-10-03T00:00:00Z")
    assert _read(path, "SELECT count(*) FROM source_events") == [(1,)]


def test_distinct_colon_containing_id_tuples_store_separately(tmp_path):
    path = tmp_path / "state.sqlite3"
    db = Database(path)
    db.initialize()
    for account, chat in [("network:account", "chat"), ("network", "account:chat")]:
        identity = SourceIdentity("beeper", "default", account, chat, "message")
        event = SourceEvent(
            source_key=identity.source_key, source_event_id=identity.source_event_id,
            name="message.created", occurred_at="2026-10-02T00:00:00Z",
            account_id=account, chat_id=chat, local_chat_id=None, network=None,
            message_id="message", sender_id=None, sender_name=None,
            discovered_via="test",
        )
        assert db.record_event_and_enqueue(event, now="2026-10-03T00:00:00Z")
    assert _read(path, "SELECT count(*) FROM source_events") == [(2,)]


def test_composite_sql_unique_is_independent_of_hashed_key(tmp_path):
    path = tmp_path / "state.sqlite3"
    db = Database(path)
    db.initialize()
    identity = SourceIdentity("beeper", "default", "a", "c", "m")
    event = SourceEvent(
        source_key=identity.source_key, source_event_id=identity.source_event_id,
        name="message.created", occurred_at="2026-10-02T00:00:00Z",
        account_id="a", chat_id="c", local_chat_id=None, network=None,
        message_id="m", sender_id=None, sender_name=None,
        discovered_via="test",
    )
    assert db.record_event_and_enqueue(event, now="2026-10-03T00:00:00Z")
    with closing(sqlite3.connect(path)) as connection, pytest.raises(
        sqlite3.IntegrityError, match="UNIQUE"
    ):
        connection.execute(
                """
                INSERT INTO source_events
                SELECT ?, ?, source_system, source_instance, ?,
                       name, occurred_at, account_id, chat_id, local_chat_id,
                       network, message_id, sender_id, sender_name,
                       discovered_via, created_at
                  FROM source_events LIMIT 1
                """,
                ("source:v2:unrelated", "src_v2_unrelated", "other-canonical"),
            )
    assert _read(path, "SELECT count(*) FROM source_events") == [(1,)]


def test_orphaned_legacy_delivery_rolls_back_late_migration(tmp_path):
    path = tmp_path / "state.sqlite3"
    _legacy_db(path)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "INSERT INTO deliveries VALUES (?,?,?,?,?,?,?,?,?,?)",
            ("sub", "missing-legacy-source", "evt_orphan", "pending", 0,
             "2026-10-03T00:00:00Z", None, None, None, None),
        )
    with pytest.raises(sqlite3.IntegrityError, match="legacy delivery"):
        Database(path).initialize()
    assert _read(path, "PRAGMA user_version") == [(0,)]
    assert _read(path, "SELECT count(*) FROM source_events") == [(2,)]
    assert _read(path, "SELECT count(*) FROM deliveries") == [(3,)]
    assert not _read(path, "SELECT name FROM sqlite_master WHERE name='source_events_v2'")
    assert not _read(path, "SELECT name FROM sqlite_master WHERE name='schema_meta'")
