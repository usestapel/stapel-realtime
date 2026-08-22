"""Wire envelope v1 — the versioned contract on the socket.

Every frame in both directions is::

    {"v": 1, "type": "<frame type>", "stream": "<key>", "payload": {...},
     "seq": <int>}   # journal frames only

The envelope is **shared with the core**: ``stapel_core.comm.signal()`` builds
exactly this shape and hands it to the transport, and the transport forwards it
to the socket verbatim. Two halves of one contract, written in two packages —
which is why the type set below is machine-checked against the core's
``RESERVED_FRAME_TYPES`` in the tests rather than agreed by comment.

Frame kind is **structural, not a flag** (the core's wording, and the studio
lesson behind it): a journal frame carries ``seq`` from the module's persisted
model; an ephemeral one physically cannot, because nothing persisted it. So a
client tells the two apart by asking whether ``seq`` is there — no mode field
to get wrong, and a courtesy frame can never be mistaken for journal state.

A **signal frame carries the signal's own type** (``recording.status``), not a
generic wrapper. That is why the core refuses to let a signal claim one of the
protocol type names: with the two sharing one `type` field, a reserved list is
what keeps a courtesy frame from being read as protocol.

``stream`` is populated on every frame. Under the v1 socket-per-stream topology
a client can ignore it; it is there because adding a field to a live envelope
later is a breaking change and reading one that is already present is not.

Nothing here imports Django or Channels — the envelope is plain data, so a
client library, a test, or a schema tool can use it without a running host.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: Envelope version carried by every frame. Mirrors
#: ``stapel_core.comm.signals.SIGNAL_ENVELOPE_VERSION``.
WIRE_VERSION = 1

# ── protocol frame types ────────────────────────────────────────────────
# These ten names are reserved fleet-wide by the core, which refuses to let a
# signal type claim one. Keep this block and the core's RESERVED_FRAME_TYPES
# in step — tests/test_envelope.py fails if they drift.
# client → server
HELLO = "hello"
PING = "ping"
PONG = "pong"
# server → client
WELCOME = "welcome"
REPLAY = "replay"
REPLAY_DONE = "replay_done"
LIVE = "live"
EPHEMERAL = "ephemeral"
RESYNC = "resync"
KICK = "kick"
ERROR = "error"

#: Frames a client may send. Anything else answers ``error{code=bad_type}``.
CLIENT_FRAME_TYPES = frozenset({HELLO, PING, PONG})

#: Frames the server may send as PROTOCOL. Everything else a socket emits is a
#: signal carrying its own type.
SERVER_FRAME_TYPES = frozenset(
    {WELCOME, REPLAY, REPLAY_DONE, LIVE, RESYNC, KICK, ERROR, PING, PONG}
)

#: Every name the protocol owns, including the two it does not currently emit:
#: ``ephemeral`` (kept reserved so no signal may be named it) and the client
#: half. A frame whose type is NOT in here is a signal.
PROTOCOL_FRAME_TYPES = CLIENT_FRAME_TYPES | SERVER_FRAME_TYPES | {EPHEMERAL}

#: ``error`` codes the substrate itself emits (a module may add its own).
#: ``resync`` is deliberately absent: it is a frame type, not an error — a
#: resume gap wider than the window is a normal instruction to re-hydrate.
ERROR_BAD_ENVELOPE = "bad_envelope"
ERROR_BAD_TYPE = "bad_type"
ERROR_UNAUTHORIZED = "unauthorized"


class InvalidEnvelope(ValueError):
    """A received frame is not a v1 envelope."""


@dataclass(frozen=True)
class Frame:
    """A parsed inbound envelope."""

    type: str
    payload: dict[str, Any]
    seq: int | None = None
    stream: str | None = None

    @property
    def is_journal(self) -> bool:
        """``seq`` is the structural difference between the two frame kinds."""
        return self.seq is not None

    @property
    def is_signal(self) -> bool:
        """A frame whose type the protocol does not own is a signal."""
        return self.type not in PROTOCOL_FRAME_TYPES


def frame(
    frame_type: str,
    payload: dict[str, Any] | None = None,
    *,
    seq: int | None = None,
    stream: str | None = None,
) -> dict[str, Any]:
    """Build an outbound envelope. ``seq``/``stream`` are omitted when unset."""
    out: dict[str, Any] = {
        "v": WIRE_VERSION,
        "type": frame_type,
        "payload": dict(payload or {}),
    }
    if seq is not None:
        out["seq"] = int(seq)
    if stream is not None:
        out["stream"] = stream
    return out


def error_frame(code: str, message: str, *, stream: str | None = None) -> dict[str, Any]:
    """An ``error`` envelope. ``code`` is the machine half, ``message`` the log half."""
    return frame(ERROR, {"code": code, "message": message}, stream=stream)


def parse_frame(raw: Any) -> Frame:
    """Validate an inbound envelope and return it as a :class:`Frame`.

    Raises :class:`InvalidEnvelope` — never returns a partially trusted dict.
    Unknown ``type`` values parse fine (a signal's type is a module's word, and
    a protocol frame the consumer does not accept answers ``bad_type``); an
    unknown ``v`` does not, because the shape underneath is then unknown.
    """
    if not isinstance(raw, dict):
        raise InvalidEnvelope("frame must be a JSON object")
    version = raw.get("v")
    if version != WIRE_VERSION:
        raise InvalidEnvelope(f"unsupported envelope version {version!r}")
    frame_type = raw.get("type")
    if not isinstance(frame_type, str) or not frame_type:
        raise InvalidEnvelope("frame is missing a string 'type'")
    payload = raw.get("payload", {})
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise InvalidEnvelope("'payload' must be a JSON object")
    seq = raw.get("seq")
    if seq is not None:
        try:
            seq = int(seq)
        except (TypeError, ValueError) as exc:
            raise InvalidEnvelope("'seq' must be an integer") from exc
    stream = raw.get("stream")
    if stream is not None and not isinstance(stream, str):
        raise InvalidEnvelope("'stream' must be a string")
    return Frame(type=frame_type, payload=payload, seq=seq, stream=stream)


__all__ = [
    "WIRE_VERSION",
    "HELLO",
    "PING",
    "PONG",
    "WELCOME",
    "REPLAY",
    "REPLAY_DONE",
    "LIVE",
    "EPHEMERAL",
    "RESYNC",
    "KICK",
    "ERROR",
    "CLIENT_FRAME_TYPES",
    "SERVER_FRAME_TYPES",
    "PROTOCOL_FRAME_TYPES",
    "ERROR_BAD_ENVELOPE",
    "ERROR_BAD_TYPE",
    "ERROR_UNAUTHORIZED",
    "Frame",
    "InvalidEnvelope",
    "frame",
    "error_frame",
    "parse_frame",
]
