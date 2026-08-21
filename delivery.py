"""The delivery half of the Signal primitive — Channels/Redis, v1 transport.

``stapel_core.comm.signal()`` is the emitter: sixty lines of stdlib in the
core, free for all 26 libraries, a silent no-op with no backend configured.
This module is the backend — the transport the core's ``SIGNAL_TRANSPORT``
axis names when it is set to ``"channels"``.

Three send paths, and they are not interchangeable:

* :func:`deliver` — an **ephemeral** signal. No ``seq``, never persisted,
  at-most-once, and a frame lost because nobody was listening is correct
  behaviour, not an incident (spec §3.2). This is what ``comm.signal()``
  reaches.
* :func:`deliver_frame` — a **journal** frame for a resumable stream. The
  module has already committed the row that owns the ``seq``; this only
  notifies live subscribers of a fact the database already holds. Durability
  belongs to the module's model, never to this transport.
* :func:`revoke` — the kick. Rights were withdrawn while a socket was open,
  so the substrate stops leaking now instead of waiting for a reconnect
  (spec §6.3).

Every path is **best-effort and never raises**. Channels missing, no channel
layer configured, redis down — delivery is skipped and the caller's
transaction is unaffected. The pattern is proven by ``stapel_video.realtime``:
HTTP-only hosts and the entire test fleet keep working, clients just refetch.

Ordering and commit
-------------------
:func:`signal_on_commit` schedules delivery through ``transaction.on_commit``
so a signal can never describe a row that has not landed — the same first
chance the outbox takes, without an outbox row. Emitting inside
``mutate_and_emit()`` / ``transaction.atomic()`` is therefore safe and is the
intended call site.
"""
from __future__ import annotations

import logging
from typing import Any

from .streams import group_name

logger = logging.getLogger(__name__)

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


def deliver(stream_key: str, signal_type: str, payload: dict[str, Any] | None = None) -> bool:
    """Deliver one ephemeral signal to everyone watching ``stream_key`` now.

    This is the seam ``stapel_core.comm.signal()`` calls once its transport
    axis selects Channels. Returns ``True`` if the frame reached the channel
    layer — which is not a delivery receipt, only the absence of a local
    no-op. At-most-once is the contract.
    """
    return _group_send(
        stream_key,
        {
            "type": GROUP_TYPE_SIGNAL,
            "stream": stream_key,
            "signal_type": signal_type,
            "payload": dict(payload or {}),
        },
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
    whose ``scope["user"]`` matches get a ``revoked`` frame and close 4410;
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


def signal_on_commit(
    stream_key: str, signal_type: str, payload: dict[str, Any] | None = None
) -> None:
    """Schedule :func:`deliver` for after the current transaction commits.

    A signal must never outrun the commit it describes (spec §3.2). Outside a
    transaction Django runs the callback immediately, so this is also correct
    on a read path.

    Modules should prefer ``stapel_core.comm.signal()`` — it is free of this
    library. This function is the same guarantee for hosts on a core that does
    not ship the emitter yet, and it is what the substrate's own tests use.
    """
    from django.db import transaction

    transaction.on_commit(lambda: deliver(stream_key, signal_type, payload))


class ChannelsSignalTransport:
    """The signal-delivery backend, as an object.

    Registered as ``STAPEL_COMM["SIGNAL_TRANSPORT"] = "channels"`` (or by
    dotted path to this class). It is deliberately callable *and* exposes
    ``send``: the core resolves a transport either way, and a transport that
    only fits one calling convention is a version-skew trap between two
    packages released by different hands.
    """

    def send(
        self, stream_key: str, signal_type: str, payload: dict[str, Any] | None = None
    ) -> bool:
        return deliver(stream_key, signal_type, payload)

    __call__ = send


#: Module-level instance, for a transport axis that wants an object.
channels_transport = ChannelsSignalTransport()


__all__ = [
    "GROUP_TYPE_SIGNAL",
    "GROUP_TYPE_FRAME",
    "GROUP_TYPE_REVOKE",
    "ChannelsSignalTransport",
    "channels_transport",
    "deliver",
    "deliver_frame",
    "revoke",
    "signal_on_commit",
]
