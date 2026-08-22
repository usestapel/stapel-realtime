# Changelog

All notable changes to stapel-realtime are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Pre-1.0 semver: **minor = breaking**, patch = compatible.

## [0.1.0] — 2026-08-22

First release: the delivery substrate for the **Signal** primitive
(`tasks/stapel-realtime-design.md` Ф1, `docs/pending/realtime-substrate.md` §1).
The fleet had three independently written browser sockets — chat, video and
studio-dialog — with three JWT paths, three close-code sets and two separate
implementations of the same resume protocol. This is the fourth, and the last:
chat's protocol generalized, with the gates the other two were missing.

### Added

- **Delivery seam** (`delivery.py`). `deliver(stream_key, frame)` implements
  the core 0.33 transport contract verbatim and registers itself as
  `"channels"` from `AppConfig.ready()`; a host opts in with
  `STAPEL_COMM["SIGNAL_TRANSPORT"] = "channels"` and the core's default stays
  `"none"`. Plus `deliver_frame()` for journal frames carrying a persisted
  `seq` and `revoke()` for ending a subscription now. All best-effort and
  non-raising: no Channels, no layer or a dead redis means the frame is
  dropped, which is the contract. The `transaction.on_commit` guarantee is the
  core's — this side is only asked to fan out and return.
- **`EphemeralStreamConsumer`** — at-most-once Signal fan-out. No `seq`, no
  history, read-only for the client.
- **`ResumableStreamConsumer`** — `hello{last_seq}` → `welcome` → replay →
  live, deduplicated by `seq`, bounded by `MAX_REPLAY` with an
  `error{code=resync}` verdict beyond it. Two module hooks
  (`get_server_seq`, `get_replay_rows`); everything else is the base class's.
- **Wire envelope v1** — `{v, type, stream, payload, seq?}`, published as
  `schemas/wire/envelope.v1.json` (the deliberate exception to "an L1 library
  ships no schemas"). It is half a contract whose other half the core wrote:
  `comm.signal()` builds this shape and the substrate forwards it verbatim, so
  a signal reaches the client under its own type. Frame kind is structural —
  `seq` present means journal, absent means ephemeral, with no mode flag to
  get wrong. The ten protocol type names are checked against the core's
  `RESERVED_FRAME_TYPES` by a test rather than agreed by comment. The `stream`
  field is populated but redundant under socket-per-stream: adding a field to
  a live envelope later would be breaking, reading one already there is not.
- **Stream keys** — parsing, and the Channels group name a key maps to. The
  canon itself belongs to the core (`comm.stream_key`) and is re-exported here
  rather than re-implemented: a second regex would be a second answer to "what
  is a legal key", and a module that only emits must be able to build one
  without this library. Long keys fold into a digest instead of truncating,
  because truncation would make two streams share one group.
- **Fail-closed `authorize()`** — a consumer that does not implement it
  subscribes nobody. Run on connect *and* on every `hello` (subscription and
  re-subscription), with the verdict cached for `AUTHORIZE_CACHE_S` so a
  reconnect does not pay a capability round-trip. `WorkspaceCapability` is the canonical implementation: one
  `require_capability` call, the same predicate and the same 30s cache HTTP
  uses, refusing on every non-answer including an unreachable peer.
- **Revoke → kick** — a `kick` frame and close 4410 the moment membership
  ends, instead of leaking until the client reconnects.
- **Heartbeat with token-expiry re-check** — each tick re-reads
  `stapel_claims["exp"]` and closes 4401 if it has passed. A socket that
  outlives its token is a session with a revoked credential; the substrate
  design did not cover this and the spec added it (§6.4).
- **Backpressure** — a bounded per-socket send queue; overflow closes 4413
  rather than buffering without bound or making the producer wait.
- **Close-code canon** (`close_codes.py`) — 4400/4401/4403/4404/4408/4410/
  4413/4503 with machine names, plus `TERMINAL_CLOSE_CODES`.
- **`build_websocket_application()`** — origin guard over core's G14 stack over
  a `URLRouter`, with `collect_websocket_urlpatterns()` discovering every
  installed app's `routing.websocket_urlpatterns`. The origin guard compares
  **with the port** — an allowlist entry of `studio.localhost` never matching
  `http://studio.localhost:8600` is the incident it exists for — and a
  configured-but-all-malformed allowlist refuses rather than falls open.
- **System checks** — `realtime.E001` (in-memory layer with >1 worker),
  `E002` (redis `socket_timeout` below the `BZPOPMIN` floor; redis-py 8
  defaults it to 5s), `E003` (an origin entry that can never match),
  `W001` (no layer), `W002` (guard off), `W003` (`HEARTBEAT_S = 0` disables
  liveness *and* the `exp` re-check), `W004` (a route outside `/ws/<mod>/…`).
- **`stapel_realtime.testing`** — `open_stream()` / `StreamClient`, an
  envelope-aware Channels client so a module testing its consumer does not wire
  the fourth `WebsocketCommunicator` by hand.
- **`e2e/`** — a two-worker ASGI host over a real redis, proving what a unit
  test cannot: on_commit delivery (and non-delivery on rollback), cross-worker
  fan-out in both directions, no cross-workspace leakage, and the bulk shape.
  It emits through the real `comm.signal()` seam, and the host imports nothing
  from this library — the split the design rests on, exercised rather than
  claimed. Measured on 2026-08-22: same-worker 6 ms, B→A 45 ms, one emit
  reaching both workers' sockets 5 ms, 100 signals in one transaction →
  100/100 frames on both sockets, 154 ms emit-to-last-frame, arrivals spread
  over well under 1 ms. The
  bulk shape is the answer to the refetch-storm risk: the burst arrives
  effectively instantaneously, so a debounce in the client's
  `useSignalInvalidate` is a requirement rather than a nicety — undebounced,
  those 100 signals are 100 REST refetches per connected browser.

### Notes

- Pinned to `stapel-core>=0.33,<0.34` — the minor that shipped the Signal
  primitive, published as 0.33.0/0.33.1. Older cores have no seam to register
  into, so the floor is not a preference.
- Known gap, reported upstream: `replay_done` is emitted here and is absent
  from the core's `RESERVED_FRAME_TYPES`, so a module could legally name a
  signal `replay_done` and a resuming client would read it as the end of its
  catch-up. `tests/test_envelope.py` pins the difference exactly, so it can
  shrink but not widen unnoticed.
- Migrating chat, video-lobby and studio-dialog onto these classes is Ф2/Ф3,
  deliberately not part of this release.
