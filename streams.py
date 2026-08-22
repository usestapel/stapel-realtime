"""Stream keys, and the mapping from a stream key to a Channels group name.

The key canon — ``<mod>:<scope_type>:<scope_id>[:<topic>]`` — **belongs to the
core**, next to the emitter that validates it
(``stapel_core.comm.signals.stream_key``). A module that only emits must be
able to build one without this library, and a second regex here would be a
second answer to "what is a legal key". So the builder and the exception are
re-exported, not reimplemented; what lives here is the part that needs a
transport: parsing a key back into its scope, and translating it into a group
name.

    recordings:ws:9f1c…            a workspace-wide recordings stream
    chat:conv:3d2b…                one conversation's journal
    video:lobby:ABC123             one room's lobby
    tasks:ws:9f1c…:board           a topic inside a workspace scope

**The scope is part of the name.** That is the load-bearing property, not a
formatting preference: a group whose name contains the workspace id physically
cannot deliver a frame across workspaces, whatever a consumer gets wrong later.
The name is not a secret — knowing it buys nothing, because ``authorize()``
still runs.

Group names are not stream keys. Channels restricts a group name to
``[A-Za-z0-9_.-]`` under 100 characters, and ``:`` is not in that set, so
:func:`group_name` translates. Long keys (a topic carrying a long identifier)
fold into a digest form rather than being truncated — truncation would make two
different streams share one group, which is a cross-tenant delivery bug.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from stapel_core.comm.exceptions import InvalidStreamKey
from stapel_core.comm.signals import stream_key as build_stream_key

#: Channels' own ceiling is 100; stay under it with room for a prefix.
_MAX_GROUP_NAME = 90

#: Scope type used by workspace-scoped streams — the one the workspace
#: capability authorizer understands (see :mod:`stapel_realtime.authorize`).
WORKSPACE_SCOPE = "ws"


@dataclass(frozen=True)
class StreamKey:
    """A parsed canonical stream key."""

    module: str
    scope_type: str
    scope_id: str
    topic: str | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        return build_stream_key(self.module, self.scope_type, self.scope_id, self.topic)

    @property
    def is_workspace_scoped(self) -> bool:
        return self.scope_type == WORKSPACE_SCOPE


def parse_stream_key(key: str) -> StreamKey:
    """Parse ``<mod>:<scope_type>:<scope_id>[:<topic>]``.

    Validation is the core's: the parts are re-assembled through
    :func:`build_stream_key`, so exactly one regex in the fleet decides what a
    legal key is. Raises :class:`InvalidStreamKey` on anything else — a
    consumer that cannot parse its own stream key must close the socket, not
    guess a scope.
    """
    if not isinstance(key, str) or not key:
        raise InvalidStreamKey("stream key must be a non-empty string")
    parts = key.split(":")
    if len(parts) not in (3, 4):
        raise InvalidStreamKey(
            f"{key!r} must have 3 or 4 colon-separated segments: "
            "'<mod>:<scope_type>:<scope_id>[:<topic>]'"
        )
    module, scope_type, scope_id = parts[0], parts[1], parts[2]
    topic = parts[3] if len(parts) == 4 else None
    build_stream_key(module, scope_type, scope_id, topic)  # raises on a bad segment
    return StreamKey(module=module, scope_type=scope_type, scope_id=scope_id, topic=topic)


def group_name(key: str | StreamKey) -> str:
    """Channels group name for a stream key.

    ``recordings:ws:9f1c`` -> ``recordings.ws.9f1c``. A key whose translated
    name would exceed the length ceiling becomes ``<mod>.h.<sha256[:40]>`` —
    collision-free where truncation would not be.
    """
    if isinstance(key, StreamKey):
        parsed = key
        raw = str(key)
    else:
        parsed = parse_stream_key(key)
        raw = key
    name = raw.replace(":", ".")
    if len(name) <= _MAX_GROUP_NAME:
        return name
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]
    return f"{parsed.module}.h.{digest}"


def workspace_stream(module: str, workspace_id, topic: str | None = None) -> str:
    """``<module>:ws:<workspace_id>[:<topic>]`` — the common case."""
    return build_stream_key(module, WORKSPACE_SCOPE, str(workspace_id), topic)


__all__ = [
    "WORKSPACE_SCOPE",
    "InvalidStreamKey",
    "StreamKey",
    "build_stream_key",
    "parse_stream_key",
    "group_name",
    "workspace_stream",
]
