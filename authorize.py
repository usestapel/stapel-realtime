"""The authorize seam — fail-closed, per stream, on every subscription.

A valid JWT says *who* is connecting. It says nothing about *what they may
watch*: the fleet's token carries identity and staff roles only (workspace
roles are explicitly another domain — ``stapel_core.django.jwt.utils``), so
scope always arrives in the URL and is always checked live. That is why
authentication and authorization are two separate gates here, and why the
second one has no default implementation that lets anyone through.

**Forgetting is not permission.** :func:`deny` is the base consumer's
``authorize``: a subclass that does not override it subscribes nobody
(substrate §1.6). The alternative — an open default — is a class of leak that
looks exactly like working code in every test a module writes for itself.

Two authorizers ship:

* :func:`deny` — the default.
* :class:`WorkspaceCapability` — the canonical one for
  ``<mod>:ws:<workspace_id>`` streams: a single ``require_capability`` call,
  the same predicate the HTTP path uses, with the same 30-second cache. The
  cache is the acknowledged ceiling on the residual leak window (spec §6.3);
  the way to shorten it for a specific stream is :func:`~stapel_realtime.delivery.revoke`,
  not a shorter TTL.
"""
from __future__ import annotations

import logging
from typing import Any

from .streams import WORKSPACE_SCOPE, InvalidStreamKey, parse_stream_key

logger = logging.getLogger(__name__)


def _sync_to_async(func):
    """``database_sync_to_async`` when Channels is present, else asgiref's.

    Both run the callable in a thread; Channels' variant additionally closes
    connections Django left open in that thread. The fallback keeps this
    module importable (and unit-testable) without the optional extra.
    """
    try:
        from channels.db import database_sync_to_async

        return database_sync_to_async(func)
    except ImportError:  # pragma: no cover - extra-less path
        from asgiref.sync import sync_to_async

        return sync_to_async(func, thread_sensitive=True)


async def deny(scope: dict, stream_key: str) -> bool:
    """The default authorizer: refuse. See the module docstring."""
    logger.warning(
        "realtime: refusing %s — the consumer did not implement authorize()",
        stream_key,
    )
    return False


def user_id_from_scope(scope: dict) -> Any | None:
    """The authenticated user's pk, or ``None`` for an anonymous scope."""
    user = scope.get("user")
    if user is None or not getattr(user, "is_authenticated", False):
        return None
    return getattr(user, "pk", None) or getattr(user, "id", None)


class WorkspaceCapability:
    """Authorizer for workspace-scoped streams.

    ``WorkspaceCapability("recordings.read")`` allows a subscription to
    ``recordings:ws:<id>`` iff the connecting user holds ``recordings.read``
    in workspace ``<id>``. Anything that is not a ``ws``-scoped key is
    refused — an authorizer that quietly passes a key shape it was not built
    for is the same hole as no authorizer.

    Failure is closed in both senses: no capability, and no verdict (the
    workspaces peer is unreachable) both return ``False``.
    """

    def __init__(self, capability: str):
        self.capability = capability

    async def __call__(self, scope: dict, stream_key: str) -> bool:
        user_id = user_id_from_scope(scope)
        if user_id is None:
            return False
        try:
            key = parse_stream_key(stream_key)
        except InvalidStreamKey:
            logger.warning("realtime: unparseable stream key %r", stream_key)
            return False
        if key.scope_type != WORKSPACE_SCOPE:
            logger.warning(
                "realtime: %s is not workspace-scoped; WorkspaceCapability refuses it",
                stream_key,
            )
            return False
        return await _sync_to_async(_has_capability)(
            key.scope_id, user_id, self.capability
        )


def _has_capability(workspace_id: str, user_id, capability: str) -> bool:
    from stapel_core.django.workspaces import require_capability

    try:
        return require_capability(workspace_id, user_id, capability) is not None
    except Exception:
        # A lookup that reached no verdict is a denial, never an allowance.
        logger.warning(
            "realtime: capability check failed for %s/%s", workspace_id, capability,
            exc_info=True,
        )
        return False


__all__ = ["WorkspaceCapability", "deny", "user_id_from_scope"]
