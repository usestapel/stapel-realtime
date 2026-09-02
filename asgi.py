"""Host assembly — one call builds the WebSocket half of an ASGI app.

Today the fleet has exactly one hand-written ASGI host, and the two modules
that ship consumers are mounted by nobody: the seam defect in its pure form
(canon §69 — CI is green because libraries are tested in isolation). The fix
is not a paragraph in a README, it is a function every host calls::

    # asgi.py
    from django.core.asgi import get_asgi_application
    from stapel_realtime.asgi import build_websocket_application

    application = build_websocket_application(http_application=get_asgi_application())

With no patterns given it **discovers** them: every installed app that
exports ``<package>.routing.websocket_urlpatterns`` contributes its routes.
Libraries carry the manifest, the assembly reads it — the same idea as the
scripted navigation scaffold, and what lets ``stapel-tools`` emit an ASGI file
statically later without knowing any module by name.

The stack it builds, outside in:

1. :class:`OriginGuard` — exact-origin allowlist, **compared with the port**.
   The bug this exists for is a real one: an allowlist entry of
   ``studio.localhost`` that never matched ``http://studio.localhost:8600``.
   A guard that silently never matches is worse than no guard, so the shape
   of every entry is also machine-checked (``realtime.E003``).
2. ``JWTAuthMiddlewareStack`` — core's G14. Same tokens, same blacklists, same
   user sync as HTTP; unauthenticated sockets are closed 4401 before accept.
3. ``URLRouter`` over the collected patterns.
"""
from __future__ import annotations

import logging
from urllib.parse import urlsplit

from .close_codes import CLOSE_FORBIDDEN
from .conf import realtime_settings

logger = logging.getLogger(__name__)

#: Ports that are implicit in an origin of the matching scheme.
_DEFAULT_PORTS = {"http": "80", "ws": "80", "https": "443", "wss": "443"}


def normalize_origin(origin: str) -> str:
    """``HTTP://Studio.Localhost:80/x`` -> ``http://studio.localhost``.

    Case and a default port are noise; a non-default port is identity. This is
    the whole difference between a working allowlist and the studio incident.
    """
    parts = urlsplit(origin.strip())
    scheme = (parts.scheme or "").lower()
    host = (parts.hostname or "").lower()
    port = parts.port
    if not scheme or not host:
        raise ValueError(f"{origin!r} is not a scheme://host[:port] origin")
    if port is None or str(port) == _DEFAULT_PORTS.get(scheme):
        return f"{scheme}://{host}"
    return f"{scheme}://{host}:{port}"


def site_registry_origins() -> list:
    """``https://<host>`` for every host and alias in the site registry.

    A registered site IS an origin this deployment serves: one image answers
    for N brand hosts (``stapel_core.sites``), and a guard that lists only the
    first brand's origin locks every other brand's browser out of its own
    socket — the failure reads as "chat works on host A, 403 on host B", and
    the product silently degrades to polling. Core's socket stack already
    unions the registry into its allowlist
    (``stapel_core.django.jwt.ws_origin``); this keeps the realtime guard in
    agreement with it, so one deployment cannot be guarded differently per
    socket.

    Never a widening: an empty or broken registry contributes nothing (the
    breakage is reported by ``stapel_core.sites.E001``, not here), and a core
    too old to have a registry contributes nothing either.
    """
    try:
        from stapel_core.sites import SitesConfigError, registry_from_settings
    except ImportError:  # stapel-core < 0.51 — no registry to read
        return []
    try:
        return list(registry_from_settings().origins())
    except SitesConfigError:
        return []


class OriginGuard:
    """ASGI middleware refusing WebSocket handshakes from unlisted origins.

    An empty allowlist disables the guard (with a warning-level system check):
    a host that has not named its origins yet must not be silently locked out
    of its own dev socket. Requests with no ``Origin`` header pass — browsers
    always send one, and non-browser clients are gated by the JWT below.
    """

    def __init__(self, inner, allowed_origins=None):
        self.inner = inner
        self._explicit = allowed_origins

    @property
    def allowed(self) -> set[str]:
        """The parseable entries. A malformed one is dropped, not honoured."""
        allowed = set()
        for entry in self._configured:
            try:
                allowed.add(normalize_origin(entry))
            except (ValueError, AttributeError, TypeError):
                # Reported by realtime.E003; dropping it here keeps a typo
                # from turning into "allow everything" (see __call__).
                logger.warning("realtime: ignoring malformed allowed origin %r", entry)
        return allowed

    @property
    def _configured(self) -> list:
        # An explicit list is a full override (the test seam) — the settings
        # path unions the site registry in, because the registry and the
        # setting answer different questions ("which hosts do we serve" vs
        # "which extra origins may open a socket": a Vite dev server, a native
        # shell) and a deployment that declares both means both.
        if self._explicit is not None:
            return list(self._explicit)
        entries = list(realtime_settings.ALLOWED_ORIGINS or [])
        for origin in site_registry_origins():
            if origin not in entries:
                entries.append(origin)
        return entries

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "websocket":
            return await self.inner(scope, receive, send)
        configured = self._configured
        if configured:
            # Configured-but-all-malformed refuses everything rather than
            # falling open: the host asked for a guard, and a typo must not
            # be the thing that decides it does not get one.
            allowed = self.allowed
            origin = _origin_header(scope)
            if origin is not None:
                try:
                    normalized = normalize_origin(origin)
                except ValueError:
                    normalized = None
                if normalized not in allowed:
                    logger.info("realtime: refused websocket origin %r", origin)
                    try:
                        await receive()
                    except Exception:  # pragma: no cover - transport already gone
                        pass
                    await send({"type": "websocket.close", "code": CLOSE_FORBIDDEN})
                    return
        return await self.inner(scope, receive, send)


def _origin_header(scope) -> str | None:
    for name, value in scope.get("headers") or ():
        if name == b"origin":
            return value.decode("latin-1")
    return None


def collect_websocket_urlpatterns(app_configs=None) -> list:
    """Every installed app's ``routing.websocket_urlpatterns``, in app order.

    An app without a ``routing`` module contributes nothing; an app whose
    ``routing`` module fails to import is a real error and is re-raised — a
    silently missing socket route is the defect this whole function exists to
    close.
    """
    from importlib import import_module
    from importlib.util import find_spec

    from django.apps import apps

    patterns: list = []
    for config in app_configs if app_configs is not None else apps.get_app_configs():
        module_name = f"{config.name}.routing"
        try:
            if find_spec(module_name) is None:
                continue
        except (ImportError, ValueError):  # namespace weirdness, not our business
            continue
        routing = import_module(module_name)
        found = getattr(routing, "websocket_urlpatterns", None)
        if found:
            patterns.extend(found)
            logger.debug("realtime: %s contributed %d route(s)", module_name, len(found))
    return patterns


def build_websocket_application(
    patterns=None,
    *,
    http_application=None,
    allowed_origins=None,
    authenticate: bool = True,
):
    """Assemble the host's ``ProtocolTypeRouter``.

    :param patterns: websocket url patterns; discovered from installed apps
        when omitted.
    :param http_application: the Django HTTP ASGI app. Omit it and the router
        carries WebSocket only — useful when the host composes HTTP itself.
    :param allowed_origins: overrides ``STAPEL_REALTIME["ALLOWED_ORIGINS"]``
        (mostly for tests).
    :param authenticate: keep the G14 JWT stack in the chain. Turning it off
        is a test affordance; a host that does it in production has an
        unauthenticated socket, and the consumers will still refuse to
        subscribe anyone (``scope["user"]`` is anonymous -> close 4401).
    """
    from channels.routing import ProtocolTypeRouter, URLRouter

    if patterns is None:
        patterns = collect_websocket_urlpatterns()

    inner = URLRouter(patterns)
    if authenticate:
        from stapel_core.django.jwt.channels import JWTAuthMiddlewareStack

        inner = JWTAuthMiddlewareStack(inner)
    websocket = OriginGuard(inner, allowed_origins=allowed_origins)

    routes = {"websocket": websocket}
    if http_application is not None:
        routes["http"] = http_application
    return ProtocolTypeRouter(routes)


__all__ = [
    "OriginGuard",
    "build_websocket_application",
    "collect_websocket_urlpatterns",
    "normalize_origin",
    "site_registry_origins",
]
