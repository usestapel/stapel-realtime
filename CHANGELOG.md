# Changelog

All notable changes to stapel-realtime are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Pre-1.0 semver: **minor = breaking**, patch = compatible.

## [0.1.3] — 2026-09-02

### Fixed — the site registry drives the origin guard

A multibrand deployment (stapel-core 0.51's `stapel_core.sites`: one image, N
brand hosts) declares its hosts once, and everything host-shaped derives from
that declaration — `ALLOWED_HOSTS`, `CSRF_TRUSTED_ORIGINS`, core's own
WebSocket allowlist (`stapel_core.django.jwt.ws_origin` unions `STAPEL_SITES`
in). Everything except this package's `OriginGuard`, which read only
`STAPEL_REALTIME["ALLOWED_ORIGINS"]` — so a fleet whose chat socket listed the
first brand's origin served the second brand a 403 on every handshake, and the
product silently degraded to polling. The same deployment was guarded
differently per socket, which is the exact two-lists-that-can-disagree failure
the 0.44.1 core note warns about.

`OriginGuard` now unions the site registry's origins (every host and alias,
`https://`-schemed) into the configured allowlist, exactly as core does:

- The setting still answers "which *extra* origins may open a socket" (a Vite
  dev server, a native shell); the registry answers "which hosts do we serve".
  A deployment that declares both means both.
- Never a widening: an empty or broken registry contributes nothing (the
  breakage stays reported by `stapel_core.sites.E001`), a core older than the
  registry contributes nothing, and an explicit `allowed_origins=` argument
  remains a full override — the test seam stays deterministic.
- `realtime.W002` now stays silent when the registry supplies the allowlist:
  a fleet that declared its hosts has a live guard, and warning it otherwise
  trains operators to keep the second hand-list the registry exists to retire.
- New public helper `site_registry_origins()` for anyone assembling an
  allowlist by hand.

## [0.1.2] — 2026-08-24

### Fixed — the substrate pinned one minor of the core and froze everyone on it

`stapel-core>=0.33.2,<0.34` was a one-minor window, the standard discipline for
a library that depends on a moving sibling. It is the wrong discipline for a
**substrate**: this package is meant to be built on by every module that serves
a socket, so its ceiling becomes theirs. The first module to try hit it head-on
— stapel-chat 0.3.0 moved its own floor to core 0.41 to pick up the canonical
serializer seam, and `pip` could not resolve the pair at all:

```
stapel-chat 0.3.0 depends on stapel-core<1.0 and >=0.41.0
stapel-realtime 0.1.1 depends on stapel-core<0.34 and >=0.33.2
ERROR: ResolutionImpossible
```

The ceiling is now `<1.0`. Nothing else changed, and nothing in the code needed
to: the 193-test suite passes unmodified against core 0.43.

**Why dropping the cap is not a loosening.** A one-minor window is right where
the compatibility guarantee *is* the version number. Here it is not.
`tests/test_envelope.py` asserts, in both directions, that this package's frame
types and the core's `RESERVED_FRAME_TYPES` are equal sets — the very check
0.1.1 was released to add. That test fails the moment either half grows a type
the other does not know, which is precisely the break the pin was standing in
for, and it fails in CI against whatever core is actually installed rather than
against a number written months earlier. The range only ever had to name the
floor.

## [0.1.1] — 2026-08-22

The wire contract's two halves are now equal sets, not a pinned difference.

### Changed

- Floor raised to `stapel-core>=0.33.2,<0.34`. Core 0.33.2 added `replay_done`
  — and only it — to `RESERVED_FRAME_TYPES`, so the names this substrate emits
  and the names the core refuses to let a signal claim are the same eleven.
  This is a floor rather than a preference: on an older core a module can
  still name a signal `replay_done`, and a resuming client would read that
  courtesy frame as the end of its catch-up.
- `tests/test_envelope.py` asserts the difference is empty in **both**
  directions instead of pinning one known name. Either half growing a frame
  type the other does not know is a wire break, and that test is where it
  surfaces.

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
- Known gap at the time of this release, reported upstream and closed in
  0.1.1: `replay_done` was emitted here and absent from the core's
  `RESERVED_FRAME_TYPES`.
- Migrating chat, video-lobby and studio-dialog onto these classes is Ф2/Ф3,
  deliberately not part of this release.
