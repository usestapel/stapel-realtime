"""The delivery half of the Signal primitive — Channels/Redis, v1 transport.

``stapel_core.comm.signal()`` is the emitter: stdlib, free for all 26
libraries, a silent no-op with no backend configured. This module is the
backend — the transport the core's ``STAPEL_COMM["SIGNAL_TRANSPORT"] =
"channels"`` axis resolves to, registered into the core's seam from
:class:`~stapel_realtime.apps.RealtimeConfig`.

The seam's contract, which :func:`deliver` implements verbatim:

* called as ``transport(stream_key, frame)`` — the routing key and the
  complete wire envelope the core already built;
* called **after** the surrounding transaction commits, in the committing
  thread, so it must fan out and return rather than wait on any client;
* allowed to fail — the core logs and drops, which is a legal outcome for a
  signal. This module never raises anyway.

Three send paths, and they are not interchangeable:

* :func:`deliver` — an **ephemeral** signal. No ``seq``, never persisted,
  at-most-once, and a frame lost because nobody was listening is correct
  behaviour, not an incident.
* :func:`deliver_frame` — a **journal** frame for a resumable stream. The
  module has already committed the row that owns the ``seq``; this only
  notifies live subscribers of a fact the database already holds. Durability
  belongs to the module's model, never to this transport.
* :func:`revoke` — the kick. Rights were withdrawn while a socket was open,
  so the substrate stops leaking now instead of waiting for a reconnect.

Every path is **best-effort and never raises**. Channels missing, no channel
layer configured, redis down — delivery is skipped and the caller's
transaction is unaffected. The pattern is proven by ``stapel_video.realtime``:
HTTP-only hosts and the entire test fleet keep working, clients just refetch.
"""
from __future__ import annotations

import logging
from typing import Any

from . import envelope as wire
from .streams import group_name

logger = logging.getLogger(__name__)

#: The name this transport registers under for ``SIGNAL_TRANSPORT``.
TRANSPORT_NAME = "channels"

# Channels group-message types. Channels maps dots to underscores to find the
# consumer method, so ``realtime.signal`` dispatches to ``realtime_signal``.
GROUP_TYPE_SIGNAL = "realtime.signal"
GROUP_TYPE_FRAME = "realtime.frame"
GROUP_TYPE_REVOKE = "realtime.revoke"


def _channel_layer():
    """The configured channel layer, or ``None`` if realtime is not wired."""
    try:
        from channels.layers import get_channel_layer
    except ImportError:
        return None
    try:
        return get_channel_layer()
    except Exception:  # pragma: no cover - misconfigured layer
        logger.debug("realtime: channel layer unavailable", exc_info=True)
        return None


def _group_send(stream_key: str, message: dict[str, Any]) -> bool:
    """Fan ``message`` out to a stream's group. Returns whether it was sent."""
    layer = _channel_layer()
    if layer is None:
        return False
    try:
        from asgiref.sync import async_to_sync

        async_to_sync(layer.group_send)(group_name(stream_key), message)
        return True
    except Exception:
        # Best-effort by contract: a lost signal is recoverable by refetch,
        # a raised exception here would take the caller's request with it.
        logger.debug("realtime: fan-out skipped for %s", stream_key, exc_info=True)
        return False


def deliver(stream_key: str, frame: dict[str, Any]) -> bool:
    """Deliver one signal envelope to everyone watching ``stream_key`` now.

    This is the callable the core's transport axis names. ``frame`` is the
    complete wire envelope the core built and is forwarded **verbatim**: the
    signal's own type travels in ``type``, which is precisely why the core
    refuses to let a signal claim a protocol frame name.

    Returns ``True`` if the frame reached the channel layer — which is not a
    delivery receipt, only the absence of a local no-op. At-most-once is the
    contract.
    """
    if not isinstance(frame, dict):
        logger.warning("realtime: refusing a non-dict signal frame for %s", stream_key)
        return False
    if frame.get("type") in wire.PROTOCOL_FRAME_TYPES:
        # The core rejects this at emit time; a direct caller might not have.
        # A courtesy frame wearing a protocol name would be read as protocol.
        logger.warning(
            "realtime: refusing signal frame with reserved type %r on %s",
            frame.get("type"), stream_key,
        )
        return False
    return _group_send(
        stream_key, {"type": GROUP_TYPE_SIGNAL, "stream": stream_key, "frame": frame}
    )


def deliver_frame(stream_key: str, payload: dict[str, Any], *, seq: int) -> bool:
    """Deliver one journal frame (carrying its persisted ``seq``).

    Call it **after** the row is written, from the same ``on_commit`` the
    module already uses — store-first, transport-thin. A subscriber that
    misses it replays the row by ``seq``; that is why losing this frame is
    survivable and why the transport is allowed to be best-effort.
    """
    return _group_send(
        stream_key,
        {
            "type": GROUP_TYPE_FRAME,
            "stream": stream_key,
            "seq": int(seq),
            "payload": dict(payload),
        },
    )


def revoke(stream_key: str, user_id, *, reason: str = "access_revoked") -> bool:
    """Kick one user off a stream immediately.

    The module calls this from its own ``@on_action`` subscriber when
    membership ends (workspaces already emits those Actions). Subscribers
    whose ``scope["user"]`` matches get a ``kick`` frame and close 4410;
    everyone else on the stream is untouched. ``user_id=None`` revokes the
    whole stream — the conversation/room itself is gone.
    """
    return _group_send(
        stream_key,
        {
            "type": GROUP_TYPE_REVOKE,
            "stream": stream_key,
            "user_id": None if user_id is None else str(user_id),
            "reason": reason,
        },
    )


def register_transport() -> None:
    """Register :func:`deliver` as ``"channels"`` in the core's signal seam.

    Called from ``AppConfig.ready()``. Registration is not activation: a host
    still chooses the transport with ``STAPEL_COMM["SIGNAL_TRANSPORT"]``, and
    the default stays ``"none"``.
    """
    from stapel_core.comm.signals import register_signal_transport

    register_signal_transport(TRANSPORT_NAME, deliver)


__all__ = [
    "TRANSPORT_NAME",
    "GROUP_TYPE_SIGNAL",
    "GROUP_TYPE_FRAME",
    "GROUP_TYPE_REVOKE",
    "deliver",
    "deliver_frame",
    "register_transport",
    "revoke",
]
