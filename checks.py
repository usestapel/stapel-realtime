"""System checks — today's realtime bruises, turned into machine verdicts.

Every check here exists because a specific configuration silently half-worked
in production. Documentation did not stop any of them; ``manage.py check``
does. The E-level ones describe configurations a realtime host cannot serve
correctly at all; the W-level ones describe degradations a host may genuinely
want (a dev box with no channel layer, an origin guard not yet configured).
"""
from __future__ import annotations

import os

from django.core.checks import Error, Warning as CheckWarning

from .asgi import normalize_origin, site_registry_origins
from .conf import URL_PREFIX_DEFAULT, realtime_settings, url_prefix

#: Sentinel distinct from any real setting value, including ``None``.
_UNSET = object()

#: Environment variables an ASGI server reads its worker count from.
_WORKER_ENV_VARS = ("WEB_CONCURRENCY", "UVICORN_WORKERS", "GUNICORN_WORKERS")

_IN_MEMORY = "channels.layers.InMemoryChannelLayer"


def _worker_count() -> int:
    for name in _WORKER_ENV_VARS:
        raw = os.environ.get(name)
        if raw:
            try:
                return int(raw)
            except ValueError:
                continue
    return 1


def _default_layer() -> dict:
    from django.conf import settings

    return (getattr(settings, "CHANNEL_LAYERS", None) or {}).get("default") or {}


def check_channel_layer(app_configs, **kwargs):
    """E001/W001 — a fan-out that cannot reach the other worker.

    An in-memory layer keeps its groups inside one process. With a single
    worker that is a legitimate (and fast) dev setup. With two, half the
    browsers connected to the service simply never receive anything, and
    nothing in the logs says so — the frame is delivered, to the wrong
    process's empty group. Two workers plus an in-memory layer is therefore
    not a degraded configuration, it is an unserviceable one.
    """
    layer = _default_layer()
    if not layer:
        return [
            CheckWarning(
                "No CHANNEL_LAYERS['default'] is configured — realtime delivery "
                "is a no-op and clients will only ever see REST state.",
                hint="Configure channels_redis.core.RedisChannelLayer, or leave "
                "it unset deliberately for an HTTP-only host.",
                id="realtime.W001",
            )
        ]
    if layer.get("BACKEND") == _IN_MEMORY and _worker_count() > 1:
        return [
            Error(
                f"InMemoryChannelLayer with {_worker_count()} workers: a signal "
                "emitted in one worker never reaches sockets held by another.",
                hint="Use channels_redis.core.RedisChannelLayer for any host "
                "running more than one worker, or run a single worker.",
                id="realtime.E001",
            )
        ]
    return []


def _redis_library_default_timeout():
    """What ``socket_timeout`` means when nobody sets it.

    redis-py 8.0 changed the answer from ``None`` (block forever — what a
    consumer parked in BZPOPMIN needs) to FIVE SECONDS
    (``redis.asyncio.connection.DEFAULT_SOCKET_TIMEOUT``), and channels-redis
    forwards no value of its own, so an unconfigured deployment inherits
    whatever is installed. Read from the library that will actually run,
    never hard-coded: a check that asserts 8.x behaviour against a 7.x
    install (or vice versa) is wrong in the direction that matters.
    ``None`` means "blocks forever" — no redis installed reads the same,
    because a layer that cannot import redis fails long before a timeout.
    """
    try:
        from redis.asyncio.connection import DEFAULT_SOCKET_TIMEOUT
    except ImportError:
        return None
    if DEFAULT_SOCKET_TIMEOUT is None:
        return None
    try:
        return float(DEFAULT_SOCKET_TIMEOUT)
    except (TypeError, ValueError):  # a sentinel object — treat as unknown
        return None


def check_layer_socket_timeout(app_configs, **kwargs):
    """E002 — the redis timeout that kills a parked consumer.

    ``channels_redis`` waits for messages in a blocking ``BZPOPMIN``. A
    finite ``socket_timeout`` below the layer's own rhythm tears the
    connection down mid-wait and the consumer dies on an idle stream — which
    looks exactly like "realtime is flaky" and never like a config value.

    Three places the value can live, and the check reads all three: the
    CONFIG dict itself, ``connection_kwargs``, and each ``hosts`` entry
    written as a dict — the shape channels-redis actually forwards to
    ``ConnectionPool.from_url``, and the one this check could not see while
    a live fleet's every idle consumer died 4.9 s into its wait. And one
    place it can live invisibly: UNSET. redis-py 8 defaults it to five
    seconds (:func:`_redis_library_default_timeout` asks the installed
    library), so "nobody set it" stopped being the safe answer the day that
    library landed — state ``socket_timeout: None`` explicitly to restore
    blocking, or a number above the layer's expiry.
    """
    layer = _default_layer()
    backend = layer.get("BACKEND") or ""
    if "channels_redis" not in backend:
        return []
    config = layer.get("CONFIG") or {}
    expiry = float(config.get("expiry", 60))
    floor = realtime_settings.LAYER_SOCKET_TIMEOUT_MIN
    floor = float(floor) if floor is not None else expiry + 10

    sources = [config, config.get("connection_kwargs") or {}]
    hosts = config.get("hosts")
    if isinstance(hosts, (list, tuple)):
        sources.extend(h for h in hosts if isinstance(h, dict))

    problems = []
    stated_anywhere = False
    for source in sources:
        if "socket_timeout" in source:
            stated_anywhere = True
        timeout = source.get("socket_timeout")
        if timeout is None:  # explicit None = blocking restored; absent = see below
            continue
        if float(timeout) < floor:
            problems.append(
                Error(
                    f"Channel layer socket_timeout={timeout}s is below the "
                    f"{floor}s floor: a consumer blocked in BZPOPMIN will be "
                    "disconnected while simply waiting for a message.",
                    hint="Set socket_timeout: None on the hosts entry to "
                    "restore blocking reads, or raise the value above the "
                    "layer's expiry.",
                    id="realtime.E002",
                )
            )

    if not problems and not stated_anywhere:
        inherited = _redis_library_default_timeout()
        if inherited is not None and inherited < floor:
            problems.append(
                Error(
                    f"Channel layer sets no socket_timeout, and the installed "
                    f"redis-py defaults it to {inherited:g}s — below the "
                    f"{floor}s floor. Every consumer parked in BZPOPMIN is "
                    "disconnected while simply waiting for a message; the "
                    "browser sees a websocket that quietly reconnects "
                    "forever. redis-py 8.0 changed this default; nothing in "
                    "this deployment chose it.",
                    hint='Write the hosts entry as a dict and state it: '
                    '{"address": "<url>", "socket_timeout": None} to restore '
                    "blocking reads, or a number above the layer's expiry.",
                    id="realtime.E002",
                )
            )
    return problems


def check_allowed_origins(app_configs, **kwargs):
    """E003/W002 — an allowlist entry that can never match anything.

    ``studio.localhost`` is not an origin; ``http://studio.localhost:8600``
    is. An entry in the first shape is not a stricter allowlist, it is a
    silently closed door — this is the exact incident the check is named for.
    """
    raw = realtime_settings.ALLOWED_ORIGINS or []
    if not raw and not site_registry_origins():
        # The site registry counts as an allowlist: OriginGuard unions it in,
        # so a fleet that declares its hosts in STAPEL_SITES has a live guard
        # even with the setting empty — warning it otherwise trains operators
        # to keep a second hand-list the registry exists to retire.
        return [
            CheckWarning(
                "STAPEL_REALTIME['ALLOWED_ORIGINS'] is empty and no site "
                "registry (STAPEL_SITES) is declared — the WebSocket origin "
                "guard is disabled and any page may open a socket "
                "(the JWT still gates who it belongs to).",
                hint="List the exact origins WITH port, e.g. "
                "['https://app.example.com', 'http://localhost:5173'], or "
                "declare the deployment's hosts in the site registry.",
                id="realtime.W002",
            )
        ]
    problems = []
    for entry in raw:
        try:
            normalize_origin(entry)
        except (ValueError, AttributeError):
            problems.append(
                Error(
                    f"Allowed origin {entry!r} is not a scheme://host[:port] "
                    "origin, so it can never match an incoming Origin header.",
                    hint="Write the full origin including scheme and, when it "
                    "is not the scheme's default, the port.",
                    id="realtime.E003",
                )
            )
    return problems


def check_heartbeat(app_configs, **kwargs):
    """W003 — no heartbeat means no liveness *and* no token-expiry re-check."""
    if float(realtime_settings.HEARTBEAT_S or 0) > 0:
        return []
    return [
        CheckWarning(
            "STAPEL_REALTIME['HEARTBEAT_S'] is 0: dead sockets are never "
            "reaped and a socket outliving its JWT 'exp' is never closed.",
            hint="Set a positive interval (default 25s) unless a proxy in "
            "front of the service already enforces both.",
            id="realtime.W003",
        )
    ]


def check_route_prefix(app_configs, **kwargs):
    """W004 — a socket route outside the ``/ws/<mod>/…`` edge convention.

    Self-similar routing is what lets the generated route table and the
    gateway config be projections of the manifest rather than hand-kept
    lists (substrate §1.5).
    """
    from .asgi import collect_websocket_urlpatterns

    prefix = url_prefix().strip("/")
    problems = []
    for pattern in collect_websocket_urlpatterns():
        route = str(getattr(getattr(pattern, "pattern", None), "_route", "") or "")
        if route and not route.lstrip("/").startswith(f"{prefix}/"):
            problems.append(
                CheckWarning(
                    f"WebSocket route {route!r} is outside the canonical "
                    f"'{prefix}/<module>/…' prefix.",
                    hint="Mount module sockets under the canonical prefix so "
                    "the edge route table stays a projection of the manifest.",
                    id="realtime.W004",
                )
            )
    return problems


def check_bare_url_prefix(app_configs, **kwargs):
    """W007 — a deployment still relying on the retired bare-setting fallback.

    One release's worth of pointer, named for both settings, then nothing:
    ``STAPEL_REALTIME["URL_PREFIX"]`` is the only value :func:`check_route_prefix`
    reads now (:func:`stapel_realtime.conf.url_prefix`). Fires only when the
    bare ``URL_PREFIX`` Django setting is present AND the namespaced key is
    absent — a deployment that already migrated, or that never set either
    (and gets the "ws" default), is silent.
    """
    from django.conf import settings

    overrides = getattr(settings, realtime_settings.namespace, None) or {}
    if "URL_PREFIX" in overrides:
        return []
    bare = getattr(settings, "URL_PREFIX", _UNSET)
    if bare is _UNSET:
        return []
    return [
        CheckWarning(
            f"STAPEL_REALTIME['URL_PREFIX'] is unset, and the bare Django "
            f"setting URL_PREFIX={bare!r} is present. This module used to "
            "read that bare setting as the websocket prefix — which is "
            "actually every stapel service's HTTP mount — so realtime.W004 "
            "silently judged socket routes against the wrong value. The "
            f"fallback is now removed and the default is {URL_PREFIX_DEFAULT!r}.",
            hint="Set STAPEL_REALTIME['URL_PREFIX'] explicitly if this "
            "deployment needs something other than 'ws'; otherwise this "
            "warning is safe to ignore once removed in a later release.",
            id="realtime.W007",
        )
    ]


def _presence_cache_backend() -> str:
    from django.conf import settings

    caches = getattr(settings, "CACHES", None) or {}
    entry = caches.get("default") or {}
    return str(entry.get("BACKEND") or "")


def check_presence_cache(app_configs, **kwargs):
    """W005 — a cache backend the presence registry cannot be shared through.

    Presence is a lease in the fleet-shared cache, and the whole point is that
    the service holding the socket and the service asking "is this user live"
    are different processes. On a locmem cache each of them has its own dict:
    every peer answers *no* while the socket is wide open, and the one process
    that would answer yes is usually not the one asked. On a dummy cache
    nothing is stored at all, so the oracle is a constant ``false``.

    A warning and not an error, because both are legitimate for a single
    process (a dev box, a test run) — where the answer is in fact correct.
    """
    from .conf import realtime_settings

    if int(realtime_settings.PRESENCE_TTL_S or 0) <= 0:
        return []  # the registry is off; W006 is the one that says so
    backend = _presence_cache_backend()
    if "locmem" in backend:
        return [
            CheckWarning(
                "The default cache is LocMemCache: the presence registry lives "
                "inside one process, so realtime.is_live answers 'no' from "
                "every other worker and every peer service while the socket is "
                "open.",
                hint="Point CACHES['default'] at a shared backend (redis) on "
                "any deployment where presence is read, or set "
                "STAPEL_REALTIME['PRESENCE_TTL_S'] = 0 to say the registry is "
                "deliberately not in use.",
                id="realtime.W005",
            )
        ]
    if "dummy" in backend:
        return [
            CheckWarning(
                "The default cache is DummyCache: the presence registry stores "
                "nothing, so realtime.is_live answers 'no' for everyone, "
                "always.",
                hint="Point CACHES['default'] at a shared backend (redis), or "
                "set STAPEL_REALTIME['PRESENCE_TTL_S'] = 0 to say the registry "
                "is deliberately not in use.",
                id="realtime.W005",
            )
        ]
    return []


def check_presence_ttl(app_configs, **kwargs):
    """W006 — a presence lease that cannot outlive the beat that renews it.

    The heartbeat tick is what renews a session's lease. With
    ``HEARTBEAT_S >= PRESENCE_TTL_S`` a perfectly healthy socket lets its own
    lease expire between two beats: the user blinks offline and back, and
    whoever gated a push on the oracle sends it to someone who is looking
    right at the screen. Nothing logs it — the lease simply is not there when
    it is read.
    """
    from .conf import realtime_settings

    ttl = int(realtime_settings.PRESENCE_TTL_S or 0)
    if ttl <= 0:
        return [
            CheckWarning(
                "STAPEL_REALTIME['PRESENCE_TTL_S'] is 0: the presence registry "
                "is disabled and the realtime.is_live / realtime.live_batch "
                "Functions answer 'not live' for every user.",
                hint="Set a TTL above HEARTBEAT_S (default 60s) on any host "
                "whose peers gate notifications on liveness, or keep it at 0 "
                "deliberately and let those peers send unconditionally.",
                id="realtime.W006",
            )
        ]
    heartbeat = float(realtime_settings.HEARTBEAT_S or 0)
    if heartbeat <= 0:
        return []  # no beat at all is realtime.W003's verdict, not a second one
    if heartbeat >= ttl:
        return [
            CheckWarning(
                f"STAPEL_REALTIME['HEARTBEAT_S'] ({heartbeat:g}s) is not below "
                f"['PRESENCE_TTL_S'] ({ttl}s): a live socket lets its own "
                "presence lease expire between two beats, so a watching user "
                "reads as offline.",
                hint="Keep the TTL comfortably above the heartbeat — the "
                "defaults are 25s and 60s.",
                id="realtime.W006",
            )
        ]
    return []


ALL_CHECKS = (
    check_channel_layer,
    check_layer_socket_timeout,
    check_allowed_origins,
    check_heartbeat,
    check_route_prefix,
    check_bare_url_prefix,
    check_presence_cache,
    check_presence_ttl,
)


def register_checks() -> None:
    """Register every realtime check. Called from ``AppConfig.ready()``."""
    from django.core.checks import register

    for check in ALL_CHECKS:
        register(check)


__all__ = [
    "ALL_CHECKS",
    "check_allowed_origins",
    "check_bare_url_prefix",
    "check_channel_layer",
    "check_heartbeat",
    "check_layer_socket_timeout",
    "check_presence_cache",
    "check_presence_ttl",
    "check_route_prefix",
    "register_checks",
]
