"""Wire envelope v1 — the versioned contract on the socket.

Every frame in both directions is::

    {"v": 1, "type": "<frame type>", "payload": {...},
     "seq": <int>,        # journal frames only
     "stream": "<key>"}   # optional, see below

``v`` is the envelope version, not the payload version: a module evolves its
payload under its own schema, the substrate evolves the wrapper. A client that
receives an envelope whose ``v`` it does not know must not guess — the field
exists so the break is loud.

``seq`` is present **only** on journal frames (a persisted row's monotonic
per-stream sequence). Ephemeral frames physically cannot carry one: they never
touch a persistent model, which is the whole point of keeping the two sorts
apart (substrate §1.2 — the studio journal-garbage lesson).

``stream`` is optional and reserved. v1 topology is socket-per-stream, so the
field is redundant today and the consumers still stamp it: adding a field to a
live envelope later is a breaking change, reading one that is already there is
not (spec §11.1). A future multiplexed socket routes on it.

Nothing here imports Django or Channels — the envelope is plain data, so a
client library, a test, or a schema tool can use it without a running host.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

#: Envelope version carried by every frame.
WIRE_VERSION = 1

# ── frame types ─────────────────────────────────────────────────────────
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
ERROR = "error"
REVOKED = "revoked"

#: Frames a client may send. Anything else answers ``error{code=bad_type}``.
CLIENT_FRAME_TYPES = frozenset({HELLO, PING, PONG})

#: Frames the server may send.
SERVER_FRAME_TYPES = frozenset(
    {WELCOME, REPLAY, REPLAY_DONE, LIVE, EPHEMERAL, ERROR, PING, PONG, REVOKED}
)

#: ``error`` codes the substrate itself emits (a module may add its own).
ERROR_BAD_ENVELOPE = "bad_envelope"
ERROR_BAD_TYPE = "bad_type"
ERROR_RESYNC = "resync"
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
    Unknown ``type`` values parse fine (the consumer answers ``bad_type``);
    an unknown ``v`` does not, because the shape underneath is then unknown.
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
    "ERROR",
    "REVOKED",
    "CLIENT_FRAME_TYPES",
    "SERVER_FRAME_TYPES",
    "ERROR_BAD_ENVELOPE",
    "ERROR_BAD_TYPE",
    "ERROR_RESYNC",
    "ERROR_UNAUTHORIZED",
    "Frame",
    "InvalidEnvelope",
    "frame",
    "error_frame",
    "parse_frame",
]
