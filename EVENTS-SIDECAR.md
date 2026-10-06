# Durable MCP Events sidecar

This optional service adds incoming-message wakeups without placing experimental
event code in the stable Beeper MCP request path.

> **Reusable pattern:** add MCP Events to an upstream MCP server without modifying
> or proxying its stable tool API. Keep the original MCP intact and attach a
> small event-only sidecar to the upstream application's realtime feed.

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

## Verified end-to-end

On **7 October 2026**, ChatGPT Cloud Work's native event machinery discovered
this sidecar as an Events source. Discovery advertised `message.created` plus
`account_ids`, `chat_ids`, and `sender_ids` filters.

The first live test used a WhatsApp self-chat. Beeper observed those messages,
but they were `isSender=true`, so the sidecar correctly did not emit an incoming
event. The subscription was then narrowed to a test-bot conversation and sender.
A genuine inbound message produced `message.created` and automatically triggered
the Cloud Work automation.

The delivered payload contained identifiers, network/sender metadata, and
timestamps, but **not message text**. Immediately after the successful delivery,
the sidecar reported:

```text
active_subscriptions  = 1
source_events_seen     = 10
pending_deliveries     = 0
dead_letter_deliveries = 0
source_connected       = true
last_source_error      = null
```

The exact test chat, sender, event, message, subscription, and tunnel identifiers
are intentionally not publication material.

### ChatGPT surface matrix

| Surface | Ordinary sidecar MCP tools | Native Events discovery/subscription | Observed result |
|---|---:|---:|---|
| Cloud Work | Yes | Yes | `message.created` subscription verified end-to-end |
| Ordinary Chat | Yes | Not exposed in the tested surface | `events_status` callable; no native subscriber controls available |

The Chat result is an observation about the currently exposed product surface,
not a protocol-level claim that Chat could never support Events.

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

## Automated test coverage

The private checkpoint was validated on **7 October 2026** with:

```text
pytest -q
21 passed

ruff check src tests
All checks passed!

mypy --ignore-missing-imports src
Success: no issues found in 9 source files
```

The tests cover, among other things:

- idempotent source-event insertion and single outbox enqueue;
- persistent/idempotent subscriptions and signing-secret rotation;
- discovery surface and MCP bearer enforcement;
- unsubscribe/resubscribe behavior;
- reconciliation of a missed message exactly once at the logical level;
- self-authored message suppression;
- transient delivery retry with the same event ID;
- HTTP 410 dead-letter handling;
- partial WebSocket-event hydration;
- callback HTTPS/public-destination validation;
- callback verification; and
- dual signatures during secret rotation.

## Remaining public-demo reliability pass

Before calling the demo publication-ready, exercise the important restart and
failure boundaries against the **live deployment**, not only unit tests:

1. restart the sidecar and verify the active subscription and durable state survive;
2. force a Beeper WebSocket disconnect/reconnect, create an inbound message during
   the gap, and verify reconciliation discovers it once at the logical level;
3. reboot the VM and verify the native Beeper MCP service, sidecar, and both
   tunnel paths recover without manual intervention;
4. deliberately stop the sidecar or Events tunnel and confirm the stable native
   Beeper MCP path remains healthy;
5. after each test, confirm there are no unexpected pending/dead-letter
   deliveries or source errors.

The supported Cloud Work end-to-end Events test is already complete. Ordinary
Chat was also checked as a curiosity: it can call the sidecar's normal MCP tool,
but its native Events subscription controls were not exposed in the tested Chat
surface.
