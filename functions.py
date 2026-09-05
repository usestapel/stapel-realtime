"""comm surface of stapel-realtime — the fleet's liveness oracle.

Two Functions, both reads, both over :mod:`stapel_realtime.presence`:

- ``realtime.is_live`` — does this user have a live realtime session right now?
- ``realtime.live_batch`` — the same question for up to 100 users at once.

They are Functions and not a Python import on purpose. The caller is a peer
service deciding whether to send a push, and the only process that knows the
answer is the one holding the socket; an import would answer from whichever
service happened to ask. The pair is the door a calls product filed as missing:
with nothing to ask, an incoming-call push went out unconditionally and every
client carried the workaround of suppressing a banner for a call it was already
ringing.

**There is no HTTP surface here.** This library has no views, no urls and no
gate registry — it is an L1 substrate installed for its checks — so the read is
reachable exactly the way every other comm Function is (in-process, over NATS,
or over the core's ``api/_functions/<name>/`` transport when a deployment
mounts it), and not through a route this package invents for itself.

Deliberately unguarded, like the rest of the fleet's read family: the bus is a
trusted boundary and *who may ask* is the caller's deployment policy. What the
answer never carries is where the person is: a stream family is a module name,
never a stream id, so knowing someone is live on ``chat`` reveals no
conversation.
"""
from __future__ import annotations

import json
from pathlib import Path

from stapel_core.comm import function

from . import presence

_SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas" / "functions"


def _schema(name: str) -> dict:
    return json.loads((_SCHEMAS_DIR / f"{name}.json").read_text(encoding="utf-8"))


@function("realtime.is_live", schema=_schema("realtime.is_live"))
def is_live(payload: dict) -> dict:
    """Is this user watching right now?

    Input: ``{"user_id": str, "family": str?}``.
    Output: ``{"live": bool, "sessions": int, "last_seen": iso|null}``.

    Degrades to *not live* rather than failing: a caller gating a push on this
    falls back to sending it, which is the behaviour that existed before the
    oracle did.
    """
    return presence.is_live(payload["user_id"], family=payload.get("family") or None)


@function("realtime.live_batch", schema=_schema("realtime.live_batch"))
def live_batch(payload: dict) -> dict:
    """:func:`is_live` for many users in one round trip.

    Input: ``{"user_ids": [str, …], "family": str?}`` — at most 100.
    Output: ``{"users": {user_id: {"live", "sessions", "last_seen"}}}``.

    Every id supplied comes back, including one nobody has ever seen. Over the
    cap raises ``ValueError``, which the caller sees as a ``FunctionCallError``
    — a batch read is for a notifier fanning out to a group, not a way to
    enumerate the fleet.
    """
    return {
        "users": presence.live_batch(
            payload.get("user_ids") or [], family=payload.get("family") or None
        )
    }


__all__ = ["is_live", "live_batch"]
