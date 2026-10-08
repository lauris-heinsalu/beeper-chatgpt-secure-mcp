"""Regression tests for collision-proof, versioned source identities."""

import pytest

from beeper_events_sidecar.identity import SourceIdentity


def test_delimiter_ambiguity_cannot_merge_distinct_messages():
    first = SourceIdentity("beeper", "private", "network:account", "chat", "message")
    second = SourceIdentity("beeper", "private", "network", "account:chat", "message")

    assert first.source_key != second.source_key
    assert first.source_event_id != second.source_event_id


def test_source_system_and_instance_are_part_of_identity():
    original = SourceIdentity("beeper", "private", "account", "chat", "message")
    new_system = SourceIdentity("other", "private", "account", "chat", "message")
    new_instance = SourceIdentity("beeper", "other", "account", "chat", "message")

    assert len({original.source_key, new_system.source_key, new_instance.source_key}) == 3


def test_identity_is_deterministic_across_reconstruction():
    a = SourceIdentity("beeper", "instance", 'a:,\"雪', "chat/with:colon", "msg:123")
    b = SourceIdentity("beeper", "instance", 'a:,\"雪', "chat/with:colon", "msg:123")

    assert a.canonical == b.canonical
    assert a.source_key == b.source_key
    assert a.source_event_id == b.source_event_id
    assert a.source_key.startswith("source:v2:")
    assert a.source_event_id.startswith("src_v2_")


@pytest.mark.parametrize("field", range(5))
def test_rejects_empty_identity_components(field):
    args = ["beeper", "instance", "account", "chat", "message"]
    args[field] = ""

    with pytest.raises(ValueError, match="identity component"):
        SourceIdentity(*args)
