# Durable MCP Events sidecar

This optional service adds incoming-message wakeups without placing experimental
event code in the stable Beeper MCP request path.

The stable path remains:

```text
ChatGPT Chat -> existing Beeper MCP app -> Secure MCP Tunnel
             -> Beeper Server /v0/mcp
```

The Events path is deliberately separate:

```text
Beeper Server /v1/ws -> event sidecar -> signed ChatGPT webhook
ChatGPT Work         -> separate Events MCP app/tunnel -> event sidecar /mcp
```

When an event wakes ChatGPT, the task uses the **existing native Beeper MCP
app** to read conversation context or take an explicitly authorized action.
The sidecar does not reimplement search, chat reads, or message sends.

## Why a sidecar

On 6 October 2026 the deployed Beeper Server was upgraded from 4.3.178 to
4.3.181. A fresh tunnel initialization still negotiated MCP protocol
`2025-06-18`. OpenAI MCP Events currently require MCP 2.0 protocol
`2026-07-28` plus `server/discover`, `events/list`,
`events/subscribe`, and `events/unsubscribe`.

Beeper already provides the other half of the problem: its Desktop API exposes
a realtime WebSocket at `/v1/ws` and emits `message.upserted` events.

That makes the smallest useful custom component an Events-only sidecar rather
than a proxy around Beeper's working MCP implementation.

References:

- https://developers.openai.com/plugins/build/mcp-events
- https://developers.beeper.com/desktop-api/
- https://github.com/openai/tunnel-client

## Reliability model

The service is small in responsibility but intentionally conservative about
state and recovery.

### Fast path

1. Maintain a Beeper `/v1/ws` connection.
2. Subscribe to all chats.
3. Receive `message.upserted`.
4. Hydrate the message if the event contains only an ID.
5. Ignore messages where `isSender=true`.
6. Persist a normalized source event in SQLite.
7. Create one durable outbox row per matching active subscription.
8. Deliver a small signed event to ChatGPT.

The payload intentionally contains identifiers and metadata, not message text.
The awakened task can fetch the current conversation through the native Beeper
MCP tools.

### Recovery path

The WebSocket is treated as a low-latency signal, not as the sole source of
truth. This is not merely defensive guesswork: the generated `/v1/spec` from
the tested Beeper Server 4.3.181 explicitly describes WebSocket delivery as
at-most-once, with no replay after reconnect, and tells clients to refetch via
HTTP after a disconnect to reconcile drift.

A durable checkpoint records the upper bound of the last complete Beeper
message-history reconciliation. On startup, after a WebSocket reconnect, and
periodically, the service queries Beeper for messages after the checkpoint with
an overlap window. Stable message IDs make that scan idempotent.

The checkpoint advances only after the full scan succeeds. If the process dies
during reconciliation, the previous checkpoint remains and the next run repeats
the safe overlap.

This is **at-least-once source ingestion plus idempotent deduplication**, not an
attempt to fake exactly-once delivery.

### Durable delivery

SQLite stores:

- active event subscriptions,
- callback verification cache,
- observed source events,
- webhook outbox/delivery state,
- reconciliation checkpoints.

The database uses WAL mode, foreign keys, a busy timeout, and synchronous durable
commits. Incoming source state is committed before webhook delivery is attempted.

Each subscription/source-message pair gets a stable event ID. Retries reuse that
event ID but create a fresh Standard Webhooks timestamp and signature.

Transient failures retry with bounded exponential backoff. Permanent failures
such as HTTP 410/413 and exhausted retry budgets move to a durable dead-letter
state rather than disappearing.

## Failure boundary

The design goal is that ordinary failures degrade to:

> automatic wakeups stop temporarily

rather than:

> ChatGPT loses Beeper messaging access

The existing native Beeper MCP app, tunnel, service, and credential path are not
modified by the sidecar.

If the sidecar, its SQLite database, its tunnel, or OpenAI Events support fails,
ordinary Chat mode can still use the existing Beeper MCP app.

## Security

The sidecar listens on loopback only.

Its MCP endpoint requires a separate static bearer value stored in an owner-only
file. The Events tunnel injects that header from a file reference; the secret is
not checked into the repository or placed in command arguments.

Webhook callback handling follows the current OpenAI Events guidance:

- HTTPS only;
- callback DNS is resolved and every destination must be globally routable;
- the HTTP connection is pinned to the validated address while retaining the
  original hostname for TLS verification;
- redirects are rejected;
- callback ownership is verified with a signed, single-use challenge;
- successful callback verification is cached only for a bounded period;
- event bodies are capped at 256 KiB;
- Standard Webhooks signatures are generated from the exact bytes sent;
- short signing-secret rotation windows support subscription refreshes.

This repository describes a single-owner private deployment. A multi-user
service would need a real authenticated principal model and per-user
authorization checks rather than the fixed private-tunnel principal used here.

## Local development

Python 3.12+:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
pytest -q
ruff check src tests
mypy --ignore-missing-imports src
```

The current implementation exposes only one harmless MCP tool,
`events_status`, alongside the Events methods. It does not expose any
messaging operation.

## VM deployment outline

The examples assume the repository is cloned to:

```text
/home/USER/beeper-chatgpt-secure-mcp
```

Create a virtual environment and install the package:

```bash
cd "$HOME/beeper-chatgpt-secure-mcp"
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install .
```

Create the MCP bearer secret locally on the VM without printing it:

```bash
install -d -m 700 "$HOME/.config/beeper-events-sidecar"
python3 - <<'PY'
from pathlib import Path
import os
import secrets

path = Path.home() / ".config/beeper-events-sidecar/mcp-bearer"
if path.exists():
    raise SystemExit("mcp-bearer already exists")
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w", encoding="utf-8", newline="") as out:
    out.write("Bearer " + secrets.token_urlsafe(48))
print("Created owner-only MCP bearer file.")
PY
```

Install and start [`examples/beeper-events-sidecar.service`](examples/beeper-events-sidecar.service).

Create a **separate OpenAI tunnel** for Events. Copy
[`examples/beeper-events.yaml.example`](examples/beeper-events.yaml.example)
to `~/.config/tunnel-client/beeper-events.yaml`, replace the tunnel ID and
literal user paths, then validate it with:

```bash
"$HOME/.local/bin/tunnel-client" doctor --profile beeper-events --explain
```

Install and start
[`examples/openai-beeper-events-tunnel.service`](examples/openai-beeper-events-tunnel.service).

The separate tunnel is intentional. It keeps the Events experiment from
changing the already-working native Beeper MCP app.

## Live validation so far

The first VM deployment on 6 October 2026 has already exercised several failure
boundaries rather than only unit-test mocks:

- the sidecar bound only to loopback and connected to Beeper's live WebSocket;
- self-contained `server/discover`, `events/list`, `tools/list`, and the
  read-only `events_status` tool all returned HTTP 200 locally under MCP
  `2026-07-28`;
- a live Beeper validation error exposed that the message-search page limit is
  20, not 200; after correcting it, reconciliation scanned seven incoming
  messages from the outage window, persisted seven source events, and an
  immediate overlapping reconciliation inserted zero duplicates;
- the SQLite database and its WAL/SHM companions were owner-only, and the main
  database file was verified as mode `0600`;
- stopping the Events sidecar left the existing native Beeper MCP tunnel and
  ChatGPT app usable; a live connected-account metadata call succeeded while
  the sidecar was down;
- restarting the sidecar re-established the WebSocket automatically and
  reconciliation again inserted zero duplicates;
- killing the sidecar process with `SIGKILL` exercised the ungraceful-crash
  path: systemd restarted it after the configured five-second delay, the
  WebSocket reconnected, and reconciliation again inserted zero duplicates.

The unsupported boundary is still the important one: the separate OpenAI Events
tunnel/app, callback subscription, signed webhook delivery, and actual ChatGPT
Work wakeup have not yet been exercised end to end. Do not call the Events path
complete until those tests pass.

## Acceptance tests before calling it production-worthy

At minimum:

1. duplicate WebSocket delivery creates one source event and one outbox row;
2. self-authored messages do not create ChatGPT events;
3. sidecar restart preserves subscriptions and pending deliveries;
4. signing-secret refresh keeps a short dual-signature rotation window;
5. callback verification rejects private/non-public destinations and redirects;
6. transient webhook failure retries with the same event ID;
7. HTTP 410/413 are not retried;
8. stop the sidecar, receive a message, restart it, and verify reconciliation
   discovers and delivers the missed event exactly once at the logical level;
9. reboot the VM and verify both the stable native MCP path and the isolated
   Events path recover;
10. confirm a failure of the Events sidecar/tunnel does not affect the existing
    Beeper MCP app.

The final end-to-end test is ChatGPT-specific: connect the Events tunnel as a
separate private MCP app, rescan it, confirm `message.created` appears, create
a Work subscription, send a test message from another account/person, and
confirm ChatGPT wakes and reads context through the existing Beeper MCP app.

As a curiosity after the supported Work test succeeds, ordinary Chat can be
asked to subscribe once. Current OpenAI documentation says MCP Events are
available in Work chats (and dots), so failure in Chat is expected and should
not be treated as a sidecar defect.
