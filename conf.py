"""Settings namespace for stapel-realtime.

All configuration is read through ``realtime_settings`` (lazily, at call
time) — never via module-level ``os.getenv`` (values would freeze at import).
Resolution order per key: ``settings.STAPEL_REALTIME`` dict -> flat Django
setting of the same name -> environment variable -> default below.

The spec (tasks/stapel-realtime-design.md §4.1) names two of these axes
``REALTIME_HEARTBEAT_S`` and ``REALTIME_MAX_REPLAY``. Under the fleet's
settings canon a package's keys are unprefixed inside its own namespace, so
they are ``STAPEL_REALTIME["HEARTBEAT_S"]`` and
``STAPEL_REALTIME["MAX_REPLAY"]`` here — same axes, canonical spelling.
"""
from stapel_core.conf import AppSettings

realtime_settings = AppSettings(
    "STAPEL_REALTIME",
    defaults={
        # ── liveness ─────────────────────────────────────────────────────
        # Seconds between server-initiated ``ping`` frames. Each tick also
        # re-checks the JWT's ``exp`` (spec §6.4): a socket that outlives its
        # token is a session with a revoked credential. 0 disables both — a
        # warning-level system check, never silently.
        "HEARTBEAT_S": 25,
        # Seconds to wait for the client's ``pong`` before closing 4408.
        "HEARTBEAT_TIMEOUT_S": 10,
        # ── resumable streams ────────────────────────────────────────────
        # Widest resume gap replayed inline. A wider gap answers ``error``
        # with code ``resync``; the client re-hydrates over HTTP. There is no
        # infinite rewind by design (substrate §1.7).
        "MAX_REPLAY": 500,
        # ── backpressure ─────────────────────────────────────────────────
        # Outbound frames buffered per socket before the substrate gives up
        # on a slow reader and closes 4413. The producer never waits on a
        # slow client (substrate §1.8); the client resyncs on reconnect.
        "SEND_QUEUE_SIZE": 100,
        # ── authorization ────────────────────────────────────────────────
        # Seconds an ``authorize()`` verdict is reused for the same
        # (user, stream) pair on re-subscription within one socket. Matches
        # the 30s capability cache the HTTP path already accepts (spec §6.3)
        # — the acknowledged ceiling on the residual leak window for paths
        # that do not get an explicit revoke().
        "AUTHORIZE_CACHE_S": 30,
        # ── presence ─────────────────────────────────────────────────────
        # Seconds one live session counts for without a refresh. The base
        # consumer writes on connect, on every heartbeat tick and on
        # disconnect, so this is a LEASE: a worker killed mid-socket stops
        # counting one TTL later instead of leaving a user online forever.
        # It must stay above HEARTBEAT_S or a live socket lets its own lease
        # expire between two beats — realtime.W006. 0 disables the registry
        # and `realtime.is_live` then answers `live: false` for everyone,
        # which the same check reports.
        "PRESENCE_TTL_S": 60,
        # ── host assembly ────────────────────────────────────────────────
        # Exact allowed ``Origin`` values, WITH port
        # (``https://studio.example.com``, ``http://studio.localhost:8600``).
        # Empty = no origin guard, which a system check reports as a
        # warning. The port is part of the comparison on purpose: the
        # studio incident this check exists for was an allowlist entry of
        # ``studio.localhost`` never matching ``http://studio.localhost:8600``.
        "ALLOWED_ORIGINS": [],
        # URL prefix every module's websocket_urlpatterns lives under
        # (``/ws/<mod>/...``, substrate §1.5). Used by the routing helpers
        # and asserted by a system check.
        "URL_PREFIX": "ws",
        # ── channel layer ────────────────────────────────────────────────
        # Minimum acceptable redis ``socket_timeout`` for the channel layer,
        # in seconds; ``None`` derives it from the layer's ``expiry``.
        # redis-py >= 8 defaults socket_timeout to 5s, which kills a
        # consumer parked in BZPOPMIN (substrate §1.4).
        "LAYER_SOCKET_TIMEOUT_MIN": None,
    },
    import_strings=(),
)

__all__ = ["realtime_settings"]
