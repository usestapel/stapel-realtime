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

from .asgi import normalize_origin
from .conf import realtime_settings

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


def check_layer_socket_timeout(app_configs, **kwargs):
    """E002 — the redis timeout that kills a parked consumer.

    ``channels_redis`` waits for messages in a blocking ``BZPOPMIN``. redis-py
    8 defaults ``socket_timeout`` to five seconds, so the client tears the
    connection down mid-wait and the consumer dies on an idle stream — which
    looks exactly like "realtime is flaky" and never like a config value.
    Require it unset, or comfortably above the layer's own expiry.
    """
    layer = _default_layer()
    backend = layer.get("BACKEND") or ""
    if "channels_redis" not in backend:
        return []
    config = layer.get("CONFIG") or {}
    expiry = float(config.get("expiry", 60))
    floor = realtime_settings.LAYER_SOCKET_TIMEOUT_MIN
    floor = float(floor) if floor is not None else expiry + 10

    problems = []
    for source in (config, config.get("connection_kwargs") or {}):
        timeout = source.get("socket_timeout")
        if timeout is None:
            continue
        if float(timeout) < floor:
            problems.append(
                Error(
                    f"Channel layer socket_timeout={timeout}s is below the "
                    f"{floor}s floor: a consumer blocked in BZPOPMIN will be "
                    "disconnected while simply waiting for a message.",
                    hint="Drop socket_timeout (recommended) or raise it above "
                    "the layer's expiry; redis-py >= 8 defaults it to 5s.",
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
    if not raw:
        return [
            CheckWarning(
                "STAPEL_REALTIME['ALLOWED_ORIGINS'] is empty — the WebSocket "
                "origin guard is disabled and any page may open a socket "
                "(the JWT still gates who it belongs to).",
                hint="List the exact origins WITH port, e.g. "
                "['https://app.example.com', 'http://localhost:5173'].",
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

    prefix = str(realtime_settings.URL_PREFIX or "ws").strip("/")
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


ALL_CHECKS = (
    check_channel_layer,
    check_layer_socket_timeout,
    check_allowed_origins,
    check_heartbeat,
    check_route_prefix,
)


def register_checks() -> None:
    """Register every realtime check. Called from ``AppConfig.ready()``."""
    from django.core.checks import register

    for check in ALL_CHECKS:
        register(check)


__all__ = [
    "ALL_CHECKS",
    "check_allowed_origins",
    "check_channel_layer",
    "check_heartbeat",
    "check_layer_socket_timeout",
    "check_route_prefix",
    "register_checks",
]
