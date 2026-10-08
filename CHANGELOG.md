# Changelog

All notable public releases are documented here.

## Unreleased — v1.0.1 candidate

- Add unambiguous, versioned source occurrence IDs, explicit source namespace,
  and SQLite uniqueness checks with fail-closed conflict handling.
- Add backed-up, atomic v1-to-v2 SQLite migration preserving subscriptions,
  delivery statuses, prior webhook event IDs and reconciliation checkpoints.
- Add persistent, deduplicated and safely summarized integrity, dead-letter
  and worker-failure incidents to the existing `events_status` MCP tool.
- Add optional `message.created.data.diagnostics` only while incidents remain
  unresolved; normal messages retain their original event payload.
- Supervise background workers and fail the owning process on unexpected exit
  rather than leaving a stalled daemon.
- All changes are staged in an isolated local branch; live deployment and
  release checks have not yet been performed.

## v1.0.0 — 2026-10-07

First public release.

### Highlights

- Preserves Beeper's native MCP tool path unchanged while adding an isolated MCP Events sidecar beside it.
- Converts Beeper's realtime `message.upserted` feed into a minimal `message.created` event surface for ChatGPT.
- Keeps message text out of event payloads; awakened clients fetch conversation context through the native Beeper MCP path.
- Persists source events, subscriptions, delivery state, dead letters, and reconciliation checkpoints in SQLite.
- Recovers WebSocket gaps through overlapping HTTP reconciliation with stable IDs and idempotent ingestion.
- Verifies webhook ownership, pins validated callback DNS results, rejects redirects/private destinations, bounds response bodies, and signs deliveries with Standard Webhooks.
- Supports bounded concurrent delivery, bounded chat metadata caching, fail-closed MCP bearer refresh, and live Beeper credential rotation without synchronous file I/O on the event loop.
- Includes loopback-only deployment examples, separate Secure MCP Tunnel profiles for the stable and Events paths, and systemd user-service examples.

### Verification

The release candidate passed:

- 51 tests with warnings treated as errors;
- Ruff and mypy;
- wheel and source-distribution builds;
- clean wheel install/import smoke testing on Python 3.14;
- GitHub Actions on Python 3.12 and 3.14; and
- dependency audit with no known vulnerabilities in resolved dependencies.

Live reliability checks also covered restart recovery, reconciliation after process downtime, full VM reboot recovery, and failure isolation between the Events path and the stable native Beeper MCP path.

### Known boundaries

- This is a single-owner private-deployment reference implementation, not a multi-tenant authorization system.
- Native MCP Events subscription controls were observed in Cloud Work, not ordinary Chat, on the tested ChatGPT surface.
- The sidecar intentionally does not reimplement Beeper search, conversation reads, or message sends.
