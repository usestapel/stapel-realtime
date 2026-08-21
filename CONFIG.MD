# CONFIG.MD — stapel-realtime

Config registry for **stapel-realtime** (`static-scaffold-and-config.md` §2).
One row per configuration key the library reads, its **source** (`env` = the
process environment / the `STAPEL_REALTIME` settings dict), what it is for,
whether it is required, and its default.

stapel-realtime is an **L1 library** — no models, migrations, views, urls or
comm surface of its own. Unlike most L1 libraries it *is* added to
`INSTALLED_APPS` on a host that serves WebSockets, because it carries system
checks and an unregistered check is a comment. A host that only imports the
envelope or the stream-key helpers does not need the app entry.

Every key is read through `realtime_settings`
(`stapel_realtime.conf.AppSettings`, namespace `STAPEL_REALTIME`).
Resolution order per key: `settings.STAPEL_REALTIME` dict → a flat Django
setting of the same name → environment variable → the default below.

The spec (`tasks/stapel-realtime-design.md` §4.1) names two of these axes
`REALTIME_HEARTBEAT_S` and `REALTIME_MAX_REPLAY`; under the fleet's settings
canon a package's keys are unprefixed inside its own namespace, so they are
`HEARTBEAT_S` and `MAX_REPLAY` here. Same axes, canonical spelling.

## stapel-realtime

| Key | Source | Purpose | Required | Default |
|-----|--------|---------|----------|---------|
| HEARTBEAT_S | env | seconds between server `ping` frames; each tick also re-checks the JWT `exp` and closes 4401 if it has passed. 0 disables both (realtime.W003). | no | 25 |
| HEARTBEAT_TIMEOUT_S | env | seconds to wait for the client's `pong` before closing 4408. | no | 10 |
| MAX_REPLAY | env | widest resume gap replayed inline on a resumable stream, and the `limit` passed to the module's `get_replay_rows` hook; a wider gap answers `error{code=resync}`. | no | 500 |
| SEND_QUEUE_SIZE | env | outbound frames buffered per socket before the substrate closes 4413 rather than waiting on a slow reader. | no | 100 |
| AUTHORIZE_CACHE_S | env | seconds an `authorize()` verdict is reused for the same (user, stream) within one socket; the acknowledged ceiling on the residual leak window for non-revoke paths. | no | 30 |
| ALLOWED_ORIGINS | env | exact origins WITH port that may open a socket; empty disables the guard (realtime.W002), a malformed entry is realtime.E003. | no | [] |
| URL_PREFIX | env | edge convention socket routes live under (`/ws/<mod>/…`), asserted by realtime.W004. | no | ws |
| LAYER_SOCKET_TIMEOUT_MIN | env | floor for the redis channel layer's `socket_timeout`; `None` derives it from the layer's `expiry + 10` (realtime.E002). | no | None |

## Settings this library reads but does not own

| Setting | Owner | Why it matters here |
|---------|-------|---------------------|
| `CHANNEL_LAYERS["default"]` | Django Channels | The fan-out backend. Absent → delivery is a silent no-op (realtime.W001). In-memory with more than one worker → realtime.E001. Its `CONFIG.socket_timeout` / `CONFIG.connection_kwargs.socket_timeout` and `CONFIG.expiry` are what realtime.E002 reads. |
| `WEB_CONCURRENCY` / `UVICORN_WORKERS` / `GUNICORN_WORKERS` | the ASGI server | How realtime.E001 learns the worker count. |
| the `stapel-core` JWT settings | stapel-core | `build_websocket_application()` installs core's G14 middleware; the socket authenticates with exactly the HTTP token stack. |

## e2e host (not shipped in the wheel)

`e2e/` is the two-worker live harness (`make e2e`), not part of the library.
Its knobs are listed because a config registry that skips the reads it can see
teaches everyone that the registry is optional.

| Key | Source | Purpose | Required | Default |
|-----|--------|---------|----------|---------|
| REALTIME_E2E_DIR | env | scratch directory the driver wipes and recreates each run. | no | /tmp/stapel-realtime-e2e |
| REALTIME_E2E_DB | env | sqlite path both worker processes share. | no | /tmp/stapel-realtime-e2e/db.sqlite3 |
| REALTIME_E2E_REDIS | env | redis URL for the channel layer both workers fan out through. | no | redis://127.0.0.1:6399/0 |
