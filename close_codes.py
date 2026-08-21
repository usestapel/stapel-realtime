"""Canonical WebSocket close codes for the Stapel realtime substrate.

One table for the whole fleet. Before this library there were three: chat and
video each minted 4401/4403 locally and studio had its own third set, so a
client could not tell "you were never allowed in" from "you just lost access"
without knowing which module it was talking to.

All codes live in the RFC 6455 private-use range (4000–4999) and mirror the
HTTP status they correspond to, so a close code correlates with a REST response
without a lookup table:

======  ===========================  ==================================
 code    name                         meaning
======  ===========================  ==================================
 4400    PROTOCOL_ERROR               the client sent a frame that is not a
                                      v1 envelope, repeatedly
 4401    UNAUTHENTICATED              no/invalid token at handshake (raised
                                      by core's G14 middleware before accept),
                                      or the token's ``exp`` passed while the
                                      socket was open
 4403    FORBIDDEN                    authenticated, but ``authorize()`` said
                                      no for this stream
 4404    STREAM_UNKNOWN               the URL resolved to a stream key the
                                      consumer cannot serve
 4408    HEARTBEAT_TIMEOUT            no ``pong`` within the heartbeat window
 4410    REVOKED                      rights were withdrawn while connected
                                      (``revoke()``); a ``revoked`` frame is
                                      sent first, then the socket closes
 4413    OVERFLOW                     the client could not keep up and the
                                      per-socket send queue overflowed
 4503    DATA_HOME_UNAVAILABLE        the tenant's data home could not be
                                      resolved (L2+ isolation, spec §6.6) —
                                      never a silent fallback to a shared DB
======  ===========================  ==================================

4401 is imported from core rather than re-declared: the handshake rejection is
core's (``stapel_core.django.jwt.channels``) and two constants for one number
is how they drift apart.
"""
from __future__ import annotations

try:  # pragma: no cover - exercised via the optional-dep test
    from stapel_core.django.jwt.channels import CLOSE_CODE_UNAUTHORIZED as _UNAUTH
except ImportError:  # channels not installed: the number is still the contract
    _UNAUTH = 4401

CLOSE_PROTOCOL_ERROR = 4400
CLOSE_UNAUTHENTICATED = _UNAUTH
CLOSE_FORBIDDEN = 4403
CLOSE_STREAM_UNKNOWN = 4404
CLOSE_HEARTBEAT_TIMEOUT = 4408
CLOSE_REVOKED = 4410
CLOSE_OVERFLOW = 4413
CLOSE_DATA_HOME_UNAVAILABLE = 4503

#: code -> short machine name, for logs and for the client library's switch.
CLOSE_CODE_NAMES = {
    CLOSE_PROTOCOL_ERROR: "protocol_error",
    CLOSE_UNAUTHENTICATED: "unauthenticated",
    CLOSE_FORBIDDEN: "forbidden",
    CLOSE_STREAM_UNKNOWN: "stream_unknown",
    CLOSE_HEARTBEAT_TIMEOUT: "heartbeat_timeout",
    CLOSE_REVOKED: "revoked",
    CLOSE_OVERFLOW: "overflow",
    CLOSE_DATA_HOME_UNAVAILABLE: "data_home_unavailable",
}

#: Closes a client must NOT retry with the same credentials — reconnecting
#: changes nothing until the user's access or token changes.
TERMINAL_CLOSE_CODES = frozenset(
    {CLOSE_FORBIDDEN, CLOSE_STREAM_UNKNOWN, CLOSE_REVOKED}
)


def close_code_name(code: int) -> str:
    """Machine name for a close code, or ``"unknown"``."""
    return CLOSE_CODE_NAMES.get(code, "unknown")


__all__ = [
    "CLOSE_PROTOCOL_ERROR",
    "CLOSE_UNAUTHENTICATED",
    "CLOSE_FORBIDDEN",
    "CLOSE_STREAM_UNKNOWN",
    "CLOSE_HEARTBEAT_TIMEOUT",
    "CLOSE_REVOKED",
    "CLOSE_OVERFLOW",
    "CLOSE_DATA_HOME_UNAVAILABLE",
    "CLOSE_CODE_NAMES",
    "TERMINAL_CLOSE_CODES",
    "close_code_name",
]
