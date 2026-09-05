"""Presence — the fleet's answer to "is this person watching *right now*?"

The Signal primitive is addressed to a human looking at a screen, and every
sender of one eventually needs the question this module answers. The first was
an incoming call: the ring is pushed to the callee's phone *and* rung in the
tab they already have open, because nothing in the fleet could say whether
that tab existed. So the push went out unconditionally and the client
suppressed the banner for a call it was already ringing — a workaround living
on the wrong side of the wire, in every client, forever.

The oracle belongs here because this library is the only place that *knows*.
``stapel-chat`` had presence first and it is the shape of the gap, not the
answer: it counts chat sockets, it is a Postgres row, and it is module-private,
so a person on a video or notifications socket with no chat tab open reads as
offline to everyone who asks. Anything built on the realtime substrate is a
live session, and the substrate is what writes here.

What it is
----------
A TTL lease in the **fleet-shared** cache, keyed by user, written by the base
consumer on connect, on every heartbeat tick, and on disconnect. No model, no
migration, no table: presence is worth exactly as long as it is fresh, and a
row that outlives the process holding the socket is the "online forever" bug
every counter-only implementation has.

``fleet_cache`` and not ``django.core.cache`` is the load-bearing choice. Every
service in a split deployment sets its own ``KEY_PREFIX``; the service holding
the socket and the service asking about it are not the same one. Written
through the ordinary connection, "is this user live" would answer *no* from
every peer — the exact per-service illusion ``stapel_core.core.fleet_cache``
was extracted to end (core 0.45.0, revocation and step-up before us).

The document, under one key per user::

    {"sessions": {"<channel name>": {"seen": <epoch>, "family": "chat"}, ...},
     "last_seen": <epoch>}

``family`` is the stream key's module segment — the socket's *stream family* —
so a caller can ask "live anywhere" or "live on chat" without a second
registry. The session id is the Channels channel name: unique per socket, so
two tabs are two sessions and one person.

Honest properties
-----------------
* **Read-modify-write, not atomic.** Django's cache API has no atomic map
  update that works on every backend. Two sockets connecting in the same
  millisecond can lose one entry — and the losing socket writes itself back on
  its next heartbeat, so the error is bounded by ``HEARTBEAT_S`` and always in
  the direction of under-counting. That is the honest direction: an extra push
  is a duplicate banner, a missed push is a missed call.
* **Failure degrades to offline.** No cache, a dead redis, a malformed
  document — all answer ``live: false`` rather than raising. Fail-closed here
  means "send the push", which is precisely today's unconditional behaviour,
  so nothing gets worse when the oracle is down.
* **Expiry is enforced twice.** The cache entry carries ``PRESENCE_TTL_S`` as
  its timeout *and* every session is re-checked against its own ``seen`` on
  read. A worker killed mid-socket therefore stops counting after one TTL even
  though nothing ran its ``disconnect``.
* **Not a last-seen history.** ``last_seen`` survives only as long as the
  entry — one TTL past the last disconnect. A durable "last online" belongs to
  a profile row, not to a liveness lease.
"""
from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .conf import realtime_settings

logger = logging.getLogger(__name__)

#: Cache namespace shared by every service of the fleet. A namespace is a wire
#: format between peers (``stapel_core.core.fleet_cache``): deliberately not
#: derived from SERVICE_NAME or anything else that differs between them.
PRESENCE_NAMESPACE = "stapel_realtime_presence"

#: Key template inside that namespace. The final store key is
#: ``stapel_realtime_presence:<namespace version>:stapel:realtime:presence:<id>``.
PRESENCE_KEY = "stapel:realtime:presence:{user_id}"

#: Ceiling on :func:`live_batch`. A batch read is a convenience for a notifier
#: fanning out to a group, not a way to enumerate the fleet.
MAX_BATCH = 100

#: Ids that can travel in a cache key as themselves. Anything else (a space, a
#: control character, something very long) is digested rather than truncated —
#: the same discipline as ``streams.group_name``, and for the same reason:
#: truncation would make two users share one key.
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.:@+-]{1,128}$")

_OFFLINE: dict[str, Any] = {"live": False, "sessions": 0, "last_seen": None}


def _ttl() -> int:
    try:
        return int(realtime_settings.PRESENCE_TTL_S or 0)
    except (TypeError, ValueError):
        return 0


def _cache():
    """The fleet-shared connection, or ``None`` if it cannot be opened.

    Not ``django.core.cache.cache``: that is namespaced per service, and a
    presence record only one service can see answers "no" everywhere else.
    """
    try:
        from stapel_core.core.fleet_cache import fleet_cache

        return fleet_cache(
            namespace=PRESENCE_NAMESPACE, alias="default", what="realtime presence"
        )
    except Exception:  # pragma: no cover - no cache configured at all
        logger.warning("realtime: presence cache unavailable", exc_info=True)
        return None


def _key(user_id) -> str:
    ident = str(user_id)
    if not _SAFE_ID.match(ident):
        ident = "h." + hashlib.sha256(ident.encode("utf-8")).hexdigest()[:40]
    return PRESENCE_KEY.format(user_id=ident)


def _iso(epoch) -> str | None:
    if not epoch:
        return None
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _sessions(doc: Any) -> dict[str, dict]:
    sessions = (doc or {}).get("sessions") if isinstance(doc, Mapping) else None
    if not isinstance(sessions, Mapping):
        return {}
    return {
        str(sid): dict(entry)
        for sid, entry in sessions.items()
        if isinstance(entry, Mapping)
    }


def _prune(sessions: dict[str, dict], *, now: float, ttl: int) -> dict[str, dict]:
    """Drop sessions whose lease has run out.

    Kept on every write, not only on read: a worker killed mid-socket never
    runs its ``disconnect``, and without this the document grows one dead
    entry per crash for as long as the user keeps reconnecting.
    """
    return {
        sid: entry
        for sid, entry in sessions.items()
        if now - float(entry.get("seen") or 0) <= ttl
    }


def _snapshot(doc: Any, *, family: str | None, now: float, ttl: int) -> dict[str, Any]:
    """``{live, sessions, last_seen}`` for one user's document."""
    matching = [
        entry
        for entry in _sessions(doc).values()
        if family is None or entry.get("family") == family
    ]
    seen = [float(entry.get("seen") or 0) for entry in matching]
    live = [value for value in seen if now - value <= ttl]
    candidates = list(seen)
    if family is None and isinstance(doc, Mapping) and doc.get("last_seen"):
        candidates.append(float(doc["last_seen"]))
    return {
        "live": bool(live),
        "sessions": len(live),
        "last_seen": _iso(max(candidates)) if candidates else None,
    }


# ── writes (the consumer's side) ─────────────────────────────────────────


def record_connect(user_id, session_id, *, family: str | None = None) -> bool:
    """Count one live session for ``user_id``. Called after ``accept()``.

    ``session_id`` is the socket's Channels channel name — unique per socket,
    so two tabs count as two sessions and one person. Returns whether the
    registry was written (``False`` when presence is disabled or the cache is
    unreachable); a caller must never let that answer close a socket.
    """
    return _touch(user_id, session_id, family=family)


def record_heartbeat(user_id, session_id, *, family: str | None = None) -> bool:
    """Renew this session's lease. Called on every heartbeat tick.

    The lease is what makes a crashed worker stop counting: nothing runs
    :func:`record_disconnect` for it, and one ``PRESENCE_TTL_S`` later its
    session is simply not live any more. Which is why ``HEARTBEAT_S`` must
    stay below that TTL — realtime.W006 says so at check time.
    """
    return _touch(user_id, session_id, family=family)


def _touch(user_id, session_id, *, family: str | None) -> bool:
    ttl = _ttl()
    if ttl <= 0 or user_id is None or not session_id:
        return False
    cache = _cache()
    if cache is None:
        return False
    now = time.time()
    key = _key(user_id)
    try:
        sessions = _prune(_sessions(cache.get(key)), now=now, ttl=ttl)
        sessions[str(session_id)] = {"seen": now, "family": family or None}
        cache.set(key, {"sessions": sessions, "last_seen": now}, timeout=ttl)
    except Exception:
        logger.warning("realtime: presence write failed", exc_info=True)
        return False
    return True


def record_disconnect(user_id, session_id) -> bool:
    """Stop counting one session. Called from the consumer's ``disconnect``.

    The document survives with an empty session map so ``last_seen`` stays
    answerable for one more TTL; after that the entry expires and the user is
    simply unknown. This is a liveness lease, not a history.
    """
    ttl = _ttl()
    if ttl <= 0 or user_id is None or not session_id:
        return False
    cache = _cache()
    if cache is None:
        return False
    now = time.time()
    key = _key(user_id)
    try:
        doc = cache.get(key)
        if not isinstance(doc, Mapping):
            return False
        sessions = _sessions(doc)
        sessions.pop(str(session_id), None)
        cache.set(
            key,
            {"sessions": _prune(sessions, now=now, ttl=ttl), "last_seen": now},
            timeout=ttl,
        )
    except Exception:
        logger.warning("realtime: presence write failed", exc_info=True)
        return False
    return True


# ── reads (the oracle) ───────────────────────────────────────────────────


def is_live(user_id, *, family: str | None = None) -> dict[str, Any]:
    """``{"live": bool, "sessions": int, "last_seen": iso|null}`` for one user.

    ``family`` narrows the question to one stream family (the module segment
    of the stream key: ``"chat"``, ``"video"``, …); omit it to ask "watching
    anything at all", which is what a push suppressor wants.

    Never raises. A disabled registry, an unreachable cache or a corrupt
    document all answer *not live* — the direction in which an extra push is
    the worst outcome.
    """
    ttl = _ttl()
    cache = _cache()
    if ttl <= 0 or user_id is None or cache is None:
        return dict(_OFFLINE)
    try:
        doc = cache.get(_key(user_id))
    except Exception:
        logger.warning("realtime: presence read failed", exc_info=True)
        return dict(_OFFLINE)
    return _snapshot(doc, family=family, now=time.time(), ttl=ttl)


def live_batch(
    user_ids: Iterable, *, family: str | None = None
) -> dict[str, dict[str, Any]]:
    """:func:`is_live` for up to :data:`MAX_BATCH` users, in one cache round trip.

    **Every id asked about comes back**, including one nobody has ever seen
    (as the offline snapshot) — the caller is holding that id for a reason and
    must be told the answer, not left to infer it from an absence.

    Raises ``ValueError`` above the cap. A batch read is for a notifier
    deciding whom to push to, not a way to enumerate the fleet.
    """
    ids = [str(value) for value in user_ids if value is not None]
    if len(ids) > MAX_BATCH:
        raise ValueError(
            f"live_batch accepts at most {MAX_BATCH} user ids, got {len(ids)}"
        )
    ttl = _ttl()
    cache = _cache()
    if not ids:
        return {}
    if ttl <= 0 or cache is None:
        return {ident: dict(_OFFLINE) for ident in ids}
    keys = {ident: _key(ident) for ident in ids}
    try:
        docs = cache.get_many(sorted(set(keys.values())))
    except Exception:
        logger.warning("realtime: presence batch read failed", exc_info=True)
        return {ident: dict(_OFFLINE) for ident in ids}
    now = time.time()
    return {
        ident: _snapshot(docs.get(key), family=family, now=now, ttl=ttl)
        for ident, key in keys.items()
    }


__all__ = [
    "MAX_BATCH",
    "PRESENCE_KEY",
    "PRESENCE_NAMESPACE",
    "is_live",
    "live_batch",
    "record_connect",
    "record_disconnect",
    "record_heartbeat",
]
