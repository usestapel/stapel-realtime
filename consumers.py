"""The two consumers every browser socket in the fleet is built from.

Before this module the fleet had three independent implementations of the same
socket — chat, video and studio-dialog each with their own auth, their own
close codes, their own resume protocol. Two of them (chat, dialog) solved the
*same* problem twice. This is the fourth implementation, and it is the last
one: everything below is a generalization of chat's protocol, which is the one
that was actually proven by tests in production shape.

* :class:`EphemeralStreamConsumer` — Signal delivery. Frames carry no ``seq``
  and are never persisted; a client that was not connected gets nothing and
  refetches over REST. This is the degenerate case of the resumable protocol,
  not a different animal.
* :class:`ResumableStreamConsumer` — a journal with catch-up:
  ``hello{last_seq}`` -> ``welcome`` -> replay -> live, deduplicated by
  ``seq``, with a bounded replay window. The module supplies two hooks (the
  current sequence, and the rows after a sequence) and nothing else.

Both inherit the same substrate: G14 authentication, the fail-closed
:mod:`~stapel_realtime.authorize` seam, the v1 envelope, heartbeat with
token-expiry re-check, disconnect-on-overflow backpressure, and
revoke-to-kick.

Channels is an optional extra. Importing this module without it raises a clear
ImportError; nothing in the package imports it at app-ready time, so an
HTTP-only host pays nothing.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Sequence

try:
    from channels.generic.websocket import AsyncJsonWebsocketConsumer
except ImportError as exc:  # pragma: no cover - exercised via optional-dep test
    raise ImportError(
        "stapel_realtime.consumers requires the optional 'channels' dependency. "
        "Install it with:\n    pip install 'stapel-realtime[channels]'"
    ) from exc

from . import envelope as wire
from .authorize import deny
from .close_codes import (
    CLOSE_FORBIDDEN,
    CLOSE_HEARTBEAT_TIMEOUT,
    CLOSE_OVERFLOW,
    CLOSE_PROTOCOL_ERROR,
    CLOSE_REVOKED,
    CLOSE_STREAM_UNKNOWN,
    CLOSE_UNAUTHENTICATED,
)
from .conf import realtime_settings
from .delivery import GROUP_TYPE_FRAME, GROUP_TYPE_REVOKE, GROUP_TYPE_SIGNAL
from .streams import InvalidStreamKey, build_stream_key, group_name

logger = logging.getLogger(__name__)

#: Malformed frames tolerated before the socket is closed 4400. One is a
#: client bug worth reporting; a stream of them is a client that will not stop.
_PROTOCOL_STRIKES = 3


@dataclass(frozen=True)
class JournalRow:
    """One replayable row: its persisted sequence and the frame payload."""

    seq: int
    payload: dict[str, Any]


class BaseStreamConsumer(AsyncJsonWebsocketConsumer):
    """Shared substrate. Not used directly — pick one of the two subclasses.

    Declarative wiring: set ``module``, ``scope_type`` and
    ``stream_key_kwarg`` and the canonical stream key is derived from the URL
    route, or override :meth:`get_stream_key` for anything else::

        class RecordingsConsumer(EphemeralStreamConsumer):
            module = "recordings"
            scope_type = "ws"
            stream_key_kwarg = "workspace_id"
            authorizer = WorkspaceCapability("recordings.read")
    """

    #: Segments of the canonical stream key (see :mod:`stapel_realtime.streams`).
    module: str | None = None
    scope_type: str | None = None
    topic: str | None = None
    #: URL kwarg carrying the scope id.
    stream_key_kwarg: str | None = None

    #: ``async (scope, stream_key) -> bool``. ``None`` means :func:`deny`.
    authorizer: Callable[[dict, str], Awaitable[bool]] | None = None

    # ── lifecycle ────────────────────────────────────────────────────────

    async def connect(self):
        self.stream_key: str | None = None
        self.group: str | None = None
        self._max_seq_sent = 0
        self._protocol_strikes = 0
        self._pong_pending = False
        self._closing = False
        self._writer_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._outbox: asyncio.Queue = asyncio.Queue(
            maxsize=max(1, int(realtime_settings.SEND_QUEUE_SIZE))
        )

        # 1. Authentication. G14 normally rejects before we are reached; this
        #    is the belt for a host that mounted the router without it.
        user = self.scope.get("user")
        if user is None or not getattr(user, "is_authenticated", False):
            await self.close(code=CLOSE_UNAUTHENTICATED)
            return

        # 2. Which stream is this socket?
        try:
            self.stream_key = await self.get_stream_key()
        except (InvalidStreamKey, KeyError, ValueError) as exc:
            logger.warning("realtime: cannot resolve a stream key: %s", exc)
            await self.close(code=CLOSE_STREAM_UNKNOWN)
            return

        # 3. Authorization — fail-closed, before accept, before group_add.
        if not await self._authorize_cached(force=True):
            await self.close(code=CLOSE_FORBIDDEN)
            return

        self.group = group_name(self.stream_key)
        await self.channel_layer.group_add(self.group, self.channel_name)
        await self.accept()
        self._writer_task = asyncio.create_task(self._writer_loop())
        interval = float(realtime_settings.HEARTBEAT_S or 0)
        if interval > 0:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(interval))

    async def disconnect(self, code):
        self._closing = True
        for task in (self._heartbeat_task, self._writer_task):
            if task is not None:
                task.cancel()
        if getattr(self, "group", None):
            await self.channel_layer.group_discard(self.group, self.channel_name)

    async def get_stream_key(self) -> str:
        """The canonical stream key this socket serves.

        The declarative default builds it from ``module``/``scope_type``/
        ``stream_key_kwarg``; override for a key the URL does not spell out.
        """
        if not (self.module and self.scope_type and self.stream_key_kwarg):
            raise NotImplementedError(
                f"{type(self).__name__} must set module/scope_type/"
                "stream_key_kwarg or override get_stream_key()"
            )
        kwargs = (self.scope.get("url_route") or {}).get("kwargs") or {}
        scope_id = kwargs[self.stream_key_kwarg]
        return build_stream_key(self.module, self.scope_type, str(scope_id), self.topic)

    async def authorize(self, scope: dict, stream_key: str) -> bool:
        """May this connection watch this stream? Default: no."""
        authorizer = self.authorizer
        if authorizer is None:
            return await deny(scope, stream_key)
        return await authorizer(scope, stream_key)

    async def _authorize_cached(self, *, force: bool = False) -> bool:
        """``authorize()`` on connect and again on every ``hello``.

        The substrate requires the check on subscription *and* re-subscription
        — a socket that was authorized once and then re-hellos must not be
        taken on trust. Re-asking on every hello without a cache would put a
        capability round-trip on the reconnect path, so a verdict is reused
        for ``AUTHORIZE_CACHE_S`` seconds, the same window the HTTP capability
        cache already accepts. Shortening a specific stream's window is what
        :func:`~stapel_realtime.delivery.revoke` is for.
        """
        now = time.monotonic()
        if not force and now < getattr(self, "_authorized_until", 0.0):
            return True
        allowed = await self.authorize(self.scope, self.stream_key)
        if allowed:
            ttl = float(realtime_settings.AUTHORIZE_CACHE_S or 0)
            self._authorized_until = now + max(0.0, ttl)
        else:
            self._authorized_until = 0.0
        return allowed

    # ── inbound ──────────────────────────────────────────────────────────

    async def receive_json(self, content, **kwargs):
        try:
            frame = wire.parse_frame(content)
        except wire.InvalidEnvelope as exc:
            self._protocol_strikes += 1
            await self._error(wire.ERROR_BAD_ENVELOPE, str(exc))
            if self._protocol_strikes >= _PROTOCOL_STRIKES:
                # Drain first: the client deserves to read why it was dropped.
                await self._drain_then_close(CLOSE_PROTOCOL_ERROR)
            return
        handler = self.frame_handlers().get(frame.type)
        if handler is None:
            await self._error(
                wire.ERROR_BAD_TYPE, f"unknown or unaccepted frame type {frame.type!r}"
            )
            return
        if frame.type == wire.HELLO and not await self._authorize_cached():
            await self._error(
                wire.ERROR_UNAUTHORIZED, "no longer authorized for this stream"
            )
            await self._drain_then_close(CLOSE_FORBIDDEN)
            return
        await handler(frame)

    def frame_handlers(self) -> dict[str, Callable[[wire.Frame], Awaitable[None]]]:
        """client frame type -> handler. Subclasses extend this mapping."""
        return {
            wire.HELLO: self.on_hello,
            wire.PING: self.on_ping,
            wire.PONG: self.on_pong,
        }

    async def on_hello(self, frame: wire.Frame) -> None:
        """Ephemeral default: acknowledge, nothing to replay."""
        await self.send_frame(wire.WELCOME, {"server_seq": 0})

    async def on_ping(self, frame: wire.Frame) -> None:
        await self.send_frame(wire.PONG)

    async def on_pong(self, frame: wire.Frame) -> None:
        self._pong_pending = False

    # ── group events (server -> socket) ──────────────────────────────────

    async def realtime_signal(self, event):
        """``realtime.signal`` — an ephemeral Signal frame."""
        await self.send_frame(
            wire.EPHEMERAL,
            {"signal": event.get("signal_type"), **(event.get("payload") or {})},
        )

    async def realtime_frame(self, event):
        """``realtime.frame`` — a journal frame, deduplicated by ``seq``."""
        await self.send_frame(
            wire.LIVE, event.get("payload") or {}, seq=event.get("seq")
        )

    async def realtime_revoke(self, event):
        """``realtime.revoke`` — rights withdrawn; say so, then close 4410."""
        target = event.get("user_id")
        if target is not None and str(target) != str(self._user_id()):
            return
        await self.send_frame(
            wire.REVOKED, {"reason": event.get("reason") or "access_revoked"}
        )
        await self._drain_then_close(CLOSE_REVOKED)

    # ── outbound ─────────────────────────────────────────────────────────

    async def send_frame(
        self, frame_type: str, payload: dict[str, Any] | None = None, *, seq=None
    ) -> None:
        """Queue one envelope. Deduplicates journal frames by ``seq``."""
        if seq is not None:
            seq = int(seq)
            if seq <= self._max_seq_sent:
                return  # already sent — the replay/live overlap after a resume
            self._max_seq_sent = seq
        await self._enqueue(
            wire.frame(frame_type, payload, seq=seq, stream=self.stream_key)
        )

    async def _error(self, code: str, message: str) -> None:
        await self._enqueue(wire.error_frame(code, message, stream=self.stream_key))

    async def _enqueue(self, envelope: dict) -> None:
        """Backpressure lives here: the producer never waits on a slow client.

        A full queue means this socket's reader has fallen behind by
        ``SEND_QUEUE_SIZE`` frames. The substrate drops the socket (4413)
        instead of buffering without bound or blocking the fan-out — the
        client reconnects and resyncs by its own mechanics (substrate §1.8).
        """
        if self._closing:
            return
        try:
            self._outbox.put_nowait(envelope)
        except asyncio.QueueFull:
            logger.info(
                "realtime: %s overflowed its send queue; closing 4413", self.stream_key
            )
            self._closing = True
            await self.close(code=CLOSE_OVERFLOW)

    async def _writer_loop(self):
        while True:
            envelope = await self._outbox.get()
            try:
                # self.send_json, not super()'s: a subclass that decorates the
                # wire (metrics, a test that stalls it) must see every frame.
                # Nothing in this package calls send_json directly, so there
                # is no path back into the queue.
                await self.send_json(envelope)
            except Exception:  # pragma: no cover - socket died under us
                logger.debug("realtime: send failed", exc_info=True)
                self._outbox.task_done()
                return
            self._outbox.task_done()

    async def _drain_then_close(self, code: int) -> None:
        """Let the queued frames reach the wire, then close with ``code``.

        Bounded: a reader that is not draining must not keep the socket (and
        the revoke that is trying to end it) alive.
        """
        self._closing = True
        try:
            await asyncio.wait_for(self._outbox.join(), timeout=1.0)
        except Exception:  # pragma: no cover - slow reader (TimeoutError included)
            pass
        await self.close(code=code)

    # ── heartbeat + token expiry ─────────────────────────────────────────

    async def _heartbeat_loop(self, interval: float):
        timeout = max(1.0, float(realtime_settings.HEARTBEAT_TIMEOUT_S))
        while True:
            await asyncio.sleep(interval)
            if self._closing:
                return
            if self._token_expired():
                # A socket that outlives its token is a session with a
                # revoked credential (spec §6.4). Close 4401; the client
                # reconnects with a fresh token — transparent in cookie mode.
                logger.info("realtime: token expired on %s; closing 4401", self.stream_key)
                self._closing = True
                await self.close(code=CLOSE_UNAUTHENTICATED)
                return
            self._pong_pending = True
            await self.send_frame(wire.PING)
            await asyncio.sleep(timeout)
            if self._pong_pending and not self._closing:
                self._closing = True
                await self.close(code=CLOSE_HEARTBEAT_TIMEOUT)
                return

    def _token_expired(self) -> bool:
        claims = self.scope.get("stapel_claims") or {}
        exp = claims.get("exp")
        if exp is None:
            return False
        try:
            return float(exp) <= time.time()
        except (TypeError, ValueError):
            return False

    def _user_id(self):
        user = self.scope.get("user")
        return getattr(user, "pk", None) or getattr(user, "id", None)


class EphemeralStreamConsumer(BaseStreamConsumer):
    """Signal delivery: at-most-once fan-out, no history, no ``seq``.

    A frame this socket misses is gone, and that is the contract — the truth
    is in the database behind REST and the client refetches. The consumer is
    read-only for the client: writes go through REST/Function, never through
    the socket (spec §10, "not building now").
    """

    async def on_hello(self, frame: wire.Frame) -> None:
        await self.send_frame(wire.WELCOME, {"ephemeral": True})


class ResumableStreamConsumer(BaseStreamConsumer):
    """A journal with catch-up, generalized from ``stapel_chat.ChatConsumer``.

    Two hooks, both async, both called with the socket's ``stream_key``
    already resolved:

    * :meth:`get_server_seq` — the highest sequence the module has persisted.
    * :meth:`get_replay_rows` — rows with ``seq > after_seq``, ascending,
      at most ``limit`` of them.

    Everything else — the welcome, the bounded replay, the resync verdict, the
    dedup between replay and live — is here, once.

    Store-first is a rule for the *module*, not something this class can
    enforce: write the row, then fan out
    :func:`~stapel_realtime.delivery.deliver_frame` on commit. Then a dropped
    socket costs nothing, because the journal, not the transport, is the
    durable thing.
    """

    async def get_server_seq(self) -> int:
        raise NotImplementedError(
            f"{type(self).__name__} must implement get_server_seq()"
        )

    async def get_replay_rows(self, after_seq: int, limit: int) -> Sequence[JournalRow]:
        raise NotImplementedError(
            f"{type(self).__name__} must implement get_replay_rows()"
        )

    async def on_hello(self, frame: wire.Frame) -> None:
        last_seq = frame.payload.get("last_seq") or 0
        try:
            last_seq = max(0, int(last_seq))
        except (TypeError, ValueError):
            await self._error(wire.ERROR_BAD_ENVELOPE, "'last_seq' must be an integer")
            return

        server_seq = int(await self.get_server_seq())
        await self.send_frame(wire.WELCOME, {"server_seq": server_seq})
        # Advance the dedup cursor to what the client already holds, so a live
        # frame arriving mid-replay at or below last_seq is not re-sent.
        self._max_seq_sent = max(self._max_seq_sent, last_seq)

        limit = int(realtime_settings.MAX_REPLAY)
        if server_seq - last_seq > limit:
            # No infinite rewind: the client re-hydrates over HTTP pagination.
            await self._error(
                wire.ERROR_RESYNC,
                f"resume gap {server_seq - last_seq} exceeds window {limit}",
            )
            return

        for row in await self.get_replay_rows(last_seq, limit):
            await self.send_frame(wire.REPLAY, row.payload, seq=row.seq)
        await self.send_frame(wire.REPLAY_DONE, {"up_to_seq": server_seq})


__all__ = [
    "BaseStreamConsumer",
    "EphemeralStreamConsumer",
    "ResumableStreamConsumer",
    "JournalRow",
    "GROUP_TYPE_FRAME",
    "GROUP_TYPE_REVOKE",
    "GROUP_TYPE_SIGNAL",
]
