"""Test harness for realtime consumers — the part every module would rewrite.

Chat wrote a good one and kept it to itself; video wrote a smaller one; studio
wrote a third. Wiring a ``WebsocketCommunicator`` with an authenticated scope,
speaking the v1 envelope, and waiting for a frame *of a given type* while the
heartbeat interleaves pings is substrate work, not module work.

Import it from a module's tests::

    from stapel_realtime.testing import open_stream

    async def test_it_delivers():
        socket = await open_stream(MyConsumer, "/ws/mine/42", user=user,
                                   url_kwargs={"workspace_id": "42"})
        assert await socket.hello() == ...

Requires the ``channels`` extra (it is a test dependency of the consumer, and
the consumer needs Channels anyway).
"""
from __future__ import annotations

from typing import Any

try:
    from channels.testing import WebsocketCommunicator
except ImportError as exc:  # pragma: no cover - exercised via optional-dep test
    raise ImportError(
        "stapel_realtime.testing requires the optional 'channels' dependency. "
        "Install it with:\n    pip install 'stapel-realtime[channels]'"
    ) from exc

from . import envelope as wire

#: Frames the harness swallows while waiting for something interesting, so a
#: heartbeat tick never breaks an assertion about protocol order.
_NOISE = frozenset({wire.PING, wire.PONG})


class StreamClient:
    """A connected test socket that speaks envelopes instead of dicts."""

    def __init__(self, communicator: WebsocketCommunicator, *, connected=True, close_code=None):
        self.communicator = communicator
        #: Whether the handshake was accepted.
        self.connected = connected
        #: Close code when it was refused (``None`` when accepted).
        self.close_code = close_code

    async def send(self, frame_type: str, payload: dict[str, Any] | None = None) -> None:
        await self.communicator.send_json_to(wire.frame(frame_type, payload))

    async def receive(self, timeout: float = 2) -> wire.Frame:
        """Next frame, skipping heartbeat noise."""
        while True:
            raw = await self.communicator.receive_json_from(timeout=timeout)
            frame = wire.parse_frame(raw)
            if frame.type not in _NOISE:
                return frame

    async def receive_raw(self, timeout: float = 2) -> wire.Frame:
        """Next frame, heartbeat included."""
        return wire.parse_frame(
            await self.communicator.receive_json_from(timeout=timeout)
        )

    async def expect(self, frame_type: str, timeout: float = 2) -> wire.Frame:
        frame = await self.receive(timeout=timeout)
        assert frame.type == frame_type, f"expected {frame_type!r}, got {frame!r}"
        return frame

    async def hello(self, last_seq: int | None = None, timeout: float = 2) -> wire.Frame:
        """Send ``hello`` and return the ``welcome``."""
        payload = {} if last_seq is None else {"last_seq": last_seq}
        await self.send(wire.HELLO, payload)
        return await self.expect(wire.WELCOME, timeout=timeout)

    async def drain(self, timeout: float = 0.2) -> list[wire.Frame]:
        """Everything currently queued, heartbeat noise removed."""
        frames = []
        while True:
            if await self.communicator.receive_nothing(timeout=timeout):
                return frames
            frame = wire.parse_frame(
                await self.communicator.receive_json_from(timeout=timeout)
            )
            if frame.type not in _NOISE:
                frames.append(frame)

    async def receive_nothing(self, timeout: float = 0.2) -> bool:
        return await self.communicator.receive_nothing(timeout=timeout)

    async def wait_closed(self, timeout: float = 2) -> int:
        """Close code the server sent."""
        message = await self.communicator.receive_output(timeout=timeout)
        assert message["type"] == "websocket.close", message
        return message.get("code")

    async def close(self) -> None:
        await self.communicator.disconnect()


async def open_stream(
    consumer,
    path: str = "/ws/test/1",
    *,
    user=None,
    claims: dict | None = None,
    url_kwargs: dict | None = None,
    headers: list | None = None,
    expect_accept: bool = True,
) -> StreamClient:
    """Connect to ``consumer`` with an already-authenticated scope.

    The G14 middleware is deliberately *not* in the path: these tests exercise
    the consumer, and core already tests the handshake. ``user`` and ``claims``
    are placed in the scope exactly as the middleware would.

    With ``expect_accept=False`` the connection is expected to be refused and
    the returned client can be asked for the close code — that is how a
    fail-closed authorize is asserted.
    """
    application = consumer.as_asgi() if hasattr(consumer, "as_asgi") else consumer
    communicator = WebsocketCommunicator(application, path, headers=headers)
    communicator.scope["user"] = user
    communicator.scope["stapel_claims"] = claims or {}
    communicator.scope["url_route"] = {"args": (), "kwargs": url_kwargs or {}}
    connected, detail = await communicator.connect()
    if expect_accept:
        assert connected, f"the consumer refused the connection (close {detail})"
    return StreamClient(
        communicator,
        connected=connected,
        close_code=None if connected else detail,
    )


__all__ = ["StreamClient", "open_stream"]
