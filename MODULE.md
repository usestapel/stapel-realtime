# MODULE.md — stapel-realtime

The agent-addressable map of this library: what it provides, every seam a host
or module is meant to reach for, and the things it deliberately refuses to do.
Keep it in the same PR that changes a seam.

`stapel-realtime` is an **L1 library**: no models, migrations, views, urls or
comm surface of its own. One documented deviation — it *is* added to
`INSTALLED_APPS`, because it carries system checks and an unregistered check is
a comment. A host that only imports the envelope or the stream-key helpers does
not need the app entry.

---

## What this library provides

The delivery half of the **Signal** primitive. `stapel_core.comm.signal()` is
the emitter (free, no-op without a backend, importable by every library);
everything the frame touches after that call lives here:

- `deliver(stream_key, frame)` — the v1 backend, registered into the core's
  seam as `"channels"` from `AppConfig.ready()` and selected by the host with
  `STAPEL_COMM["SIGNAL_TRANSPORT"] = "channels"` (the default stays `"none"`,
  which makes `signal()` a silent no-op). The frame the core built is
  forwarded **verbatim**.
- `EphemeralStreamConsumer` — at-most-once fan-out to whoever is watching.
- `ResumableStreamConsumer` — `hello{last_seq}` → `welcome` → replay → live,
  `seq`-deduplicated, with a bounded window and a `resync` verdict beyond it.
- Wire envelope v1, published as `schemas/wire/envelope.v1.json`.
- Stream-key parsing and the group names keys map to. The key *canon* is the
  core's (`comm.stream_key`) and is re-exported, never re-implemented — a
  module that only emits must be able to build one without this library.
- A fail-closed `authorize()` seam, and `revoke()` to end a subscription now.
- `build_websocket_application()` — the host's whole WebSocket stack.
- **Presence** — a TTL lease in the fleet-shared cache, written by the base
  consumer on connect / heartbeat / disconnect, read over the bus as the
  `realtime.is_live` and `realtime.live_batch` Functions. The substrate is the
  only place that knows which sockets are open, so the oracle lives here rather
  than in the module that happens to need it first.
- Eight system checks.
- `stapel_realtime.testing` — the Channels test client for module consumers.

**Contract of a Signal** (do not design around anything stronger):

| | guaranteed | not guaranteed |
|---|---|---|
| delivery | only to sockets connected at emit time | delivery at all |
| ordering | within one `stream_key` | across stream keys |
| commit | never precedes the transaction it describes | — |
| history | none — the primitive has no memory | replay, retry, receipts |

> **Client invariant.** State that cannot be recovered by a REST request must
> not travel on a Signal. A signal saves a poll; it does not replace the truth.

---

## Extension points

### Settings (`STAPEL_REALTIME`)

Resolution per key: `settings.STAPEL_REALTIME` dict → flat setting → env →
default. **`URL_PREFIX` is the one exception** — it reads the namespaced
dict only, never a flat `URL_PREFIX` setting (every service's own HTTP
mount collides with that name; `realtime.W007` warns for one release).
Full table with sources in [CONFIG.MD](CONFIG.MD).

| Key | Default | What it customizes |
|---|---|---|
| `HEARTBEAT_S` | `25` | Seconds between server `ping` frames. **Also** the JWT-`exp` re-check interval — `0` disables both (`realtime.W003`). |
| `HEARTBEAT_TIMEOUT_S` | `10` | Grace period for the client's `pong` before close 4408. |
| `MAX_REPLAY` | `500` | Replay window, and the `limit` handed to `get_replay_rows`. A wider gap answers a `resync` frame. |
| `SEND_QUEUE_SIZE` | `100` | Per-socket outbound buffer before close 4413. |
| `AUTHORIZE_CACHE_S` | `30` | How long a subscription verdict is reused within one socket. |
| `PRESENCE_TTL_S` | `60` | Presence lease length. Must stay above `HEARTBEAT_S` (`realtime.W006`); `0` disables the registry. |
| `ALLOWED_ORIGINS` | `[]` | Exact origins **with port**. Empty disables the guard (`realtime.W002`). |
| `URL_PREFIX` | `"ws"` | Edge convention for socket routes (`realtime.W004`). Namespaced key only — never a flat `URL_PREFIX` setting (`realtime.W007`). |
| `LAYER_SOCKET_TIMEOUT_MIN` | `None` | Floor for the redis layer's `socket_timeout`; `None` derives it from `expiry + 10` (`realtime.E002`). |

### `authorize()` — the required hook (fail-closed)

Every consumer answers "may this connection watch this stream?" and the base
class's answer is **no**. Two ways to say yes:

```python
# 1. the canonical one, for <mod>:ws:<workspace_id> streams
class RecordingsConsumer(EphemeralStreamConsumer):
    authorizer = WorkspaceCapability("recordings.read")

# 2. anything else — an async callable, or an override
class ConversationConsumer(ResumableStreamConsumer):
    async def authorize(self, scope, stream_key):
        key = parse_stream_key(stream_key)
        return await is_participant(scope["user"].pk, key.scope_id)
```

The hook runs **on connect and again on every `hello`** — subscription and
re-subscription, per substrate §1.6 — with the verdict cached for
`AUTHORIZE_CACHE_S` so a reconnect does not pay a capability round-trip. A
`hello` that fails re-authorization gets `error{code=unauthorized}` and close
4403.

`WorkspaceCapability` asks `require_capability` — the same predicate the HTTP
path uses, with the same 30-second cache — and refuses on every non-answer:
no capability, no user, an unparseable key, a key that is not `ws`-scoped, or
an unreachable workspaces peer.

**Payload minimalism** (review checklist item): content may travel in a frame
only on a stream whose `authorize()` gate equals the right to read that
content. A chat message in `chat:conv:<id>` — yes. A message body on a
workspace-wide stream — no: send the id and a status change, and let REST
apply its per-object checks on the refetch.

### Consumer base classes

`EphemeralStreamConsumer` needs no hooks. `ResumableStreamConsumer` needs two:

| Hook | Contract |
|---|---|
| `async get_server_seq() -> int` | Highest `seq` the module has persisted for this stream. |
| `async get_replay_rows(after_seq, limit) -> Sequence[JournalRow]` | Rows with `seq > after_seq`, ascending, at most `limit`. |

Declarative stream keys: set `module`, `scope_type`, `stream_key_kwarg` (and
optionally `topic`), or override `async get_stream_key()`.

**Store-first is the module's job**, not something the base class can enforce:
write the row, then `deliver_frame(...)` on commit. Then a dropped socket costs
nothing, because the journal — not the transport — is the durable thing.

### Presence (`realtime.is_live`, `realtime.live_batch`)

The comm surface, and the only one this library has. There is **no HTTP route**
— it has no views, no urls and no gate registry — so a peer asks the way it
asks any Function.

```python
from stapel_core.comm import call

call("realtime.is_live", {"user_id": "…"})
# -> {"live": True, "sessions": 2, "last_seen": "2026-09-05T…+00:00"}

call("realtime.live_batch", {"user_ids": [...]})   # ≤ 100, every id comes back
# -> {"users": {"…": {"live": …, "sessions": …, "last_seen": …}}}
```

Both take an optional `"family"` — the module segment of the stream key — to
ask "live on `chat`" rather than "live anywhere". Unguarded, like the rest of
the fleet's read family: the bus is a trusted boundary and *who may ask* is the
caller's deployment policy. The answer never names a stream, only a family, so
it cannot leak which conversation someone is in.

In-process, `stapel_realtime.is_live` / `live_batch` give the same answer;
`presence.record_connect` / `record_heartbeat` / `record_disconnect` are the
write side, for a host serving a socket this library's consumers do not.

### Transport (`deliver`)

The core's seam is `transport(stream_key, frame)`, called after the emitting
transaction commits, allowed to fail, and expected to fan out and return
rather than wait on any client. `deliver` implements exactly that and is
registered under `"channels"`; `register_transport()` is public for a host
that builds app config by hand. Replace the axis with a dotted path to move
Signal onto another bus (NATS `stapel.ws.*` is the anticipated microservice
value — not v1).

### Host assembly

`build_websocket_application(patterns=None, http_application=None, allowed_origins=None, authenticate=True)`
builds `OriginGuard(JWTAuthMiddlewareStack(URLRouter(patterns)))`. With no
patterns it discovers `<app>.routing.websocket_urlpatterns` from
`INSTALLED_APPS`. `OriginGuard` is usable standalone for a host composing its
own stack.

### Frame types

Eleven names are reserved fleet-wide by the core (`comm.signals.RESERVED_FRAME_TYPES`)
and the core refuses to let a signal type claim one: a signal travels under
**its own** type in the same `type` field, so the reserved list is what keeps a
courtesy frame from being read as protocol. What this substrate emits:
`welcome`, `replay`, `replay_done`, `live`, `resync`, `kick`, `error`, `ping`,
`pong`; it accepts `hello`, `ping`, `pong`. `ephemeral` stays reserved and
unused — a signal wears its own name.

Frame kind is **structural**: `seq` present ⇒ journal (`replay`/`live`), absent
⇒ ephemeral. There is no mode flag to get wrong.

The two sets are **equal** as of core 0.33.2, and a test asserts that in both
directions: either half growing a frame type the other does not know is a wire
break, and that is where it surfaces.

### Close codes

| Code | Name | Meaning |
|---|---|---|
| 4400 | `protocol_error` | Repeated non-envelope frames |
| 4401 | `unauthenticated` | No/invalid token at handshake, or `exp` passed mid-socket |
| 4403 | `forbidden` | `authorize()` said no |
| 4404 | `stream_unknown` | The URL did not resolve to a servable stream |
| 4408 | `heartbeat_timeout` | No `pong` in the window |
| 4410 | `revoked` | Rights withdrawn while connected (a `kick` frame precedes it) |
| 4413 | `overflow` | Client too slow; send queue overflowed |
| 4503 | `data_home_unavailable` | Tenant data home unresolvable (L2+ isolation) |

`TERMINAL_CLOSE_CODES` names the ones a client must not retry with the same
credentials.

### System checks

| Id | Level | Fires when |
|---|---|---|
| `realtime.E001` | error | In-memory channel layer with >1 worker — the fan-out cannot cross processes |
| `realtime.E002` | error | Redis layer `socket_timeout` below the floor — kills a consumer parked in `BZPOPMIN` |
| `realtime.E003` | error | An `ALLOWED_ORIGINS` entry that is not `scheme://host[:port]`, so it can never match |
| `realtime.W001` | warning | No channel layer configured — delivery is a no-op |
| `realtime.W002` | warning | Empty `ALLOWED_ORIGINS` — the origin guard is off |
| `realtime.W003` | warning | `HEARTBEAT_S = 0` — no liveness *and* no `exp` re-check |
| `realtime.W004` | warning | A socket route outside `/<URL_PREFIX>/<module>/…` |
| `realtime.W005` | warning | The default cache is locmem or dummy — presence is per-process, or stored nowhere |
| `realtime.W006` | warning | `PRESENCE_TTL_S` is 0, or `HEARTBEAT_S` is not below it — the lease expires between two beats |
| `realtime.W007` | warning | A bare `URL_PREFIX` Django setting is present and `STAPEL_REALTIME['URL_PREFIX']` is not — one-release pointer to the retired fallback |

---

## Anti-patterns

- **An open `authorize()`.** Returning `True` "for now" is the leak this
  library's fail-closed default exists to prevent. If a stream is genuinely
  public, say so in a named authorizer whose name says it.
- **Content on a wide stream.** See payload minimalism above.
- **Treating a Signal as durable.** No history, no retry, no receipts. If the
  module needs durability it belongs in the module's model, and the stream
  becomes resumable.
- **Delivering Actions to the browser as-is.** The outbox retries for five
  minutes; a five-minute-late "typing…" is worse than nothing. The canonical
  bridge is an `@on_action` subscriber that calls `signal()`.
- **Client→server writes over the socket.** Writes are REST/Function. (Chat's
  `send` frame is a legacy exception migrating as-is.)
- **A hand-written `asgi.py`.** Three of those are why the fleet had three
  different auth stacks and three close-code sets.
- **A second WebSocket implementation.** A human in a browser is this library;
  our own process is an application protocol that owes an answer to "why not a
  Function or a Task".
- **`os.getenv` at import time** for any of the settings above — use
  `realtime_settings`, which resolves at call time.

---

## App-layer override vs upstream contribution

The litmus: *would you need a monkeypatch or an edit inside this package?* →
upstream. *A setting, a subclass or a different authorizer is enough?* →
app-layer.

**App-layer**: a custom authorizer; a consumer subclass with extra frame
handlers (`frame_handlers()` is a dict a subclass extends); a different replay
source; tuned timeouts and window sizes; a bespoke `OriginGuard` allowlist.

**Upstream**: a new envelope field or frame type (versioned contract); a new
close code; a transport other than Channels that other modules would share;
anything that would otherwise be a `stapel_realtime.consumers` monkeypatch.
