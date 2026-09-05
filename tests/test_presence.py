"""The presence registry and the two Functions that read it.

Every assertion here is about the property a caller gates a push on: connect
makes a person live, disconnect makes them not, a lease that is not renewed
expires, two tabs are two sessions and one person, and a batch answers for
every id it was handed.

The registry runs on the test host's locmem cache — which is exactly the
configuration `realtime.W005` warns about for a real deployment, and exactly
the correct one for a single-process test run. `reset_fleet_caches()` between
tests because the fleet connection is memoized per (alias, namespace).
"""
import json
import time
from pathlib import Path

import pytest

from stapel_realtime import presence
from stapel_realtime.consumers import EphemeralStreamConsumer
from stapel_realtime.testing import open_stream

REPO = Path(__file__).resolve().parent.parent


class Watchers(EphemeralStreamConsumer):
    module = "fake"
    scope_type = "ws"
    stream_key_kwarg = "workspace_id"

    async def authorize(self, scope, stream_key):
        return True


class OtherModuleWatchers(Watchers):
    module = "chat"


@pytest.fixture(autouse=True)
def _clean_presence():
    """A fresh fleet connection and an empty registry for every test."""
    from stapel_core.core.fleet_cache import reset_fleet_caches

    reset_fleet_caches()
    presence._cache().clear()
    yield
    reset_fleet_caches()


async def _open(consumer, user, workspace_id="42"):
    """Open a socket and wait until its ``connect()`` has finished.

    The handshake is accepted from *inside* ``connect()``, so a test that
    asserts straight after ``open_stream`` races the presence write that comes
    after the accept. One ``hello`` round trip is the synchronisation point:
    Channels does not dispatch a received frame until the connect handler has
    returned.
    """
    socket = await open_stream(
        consumer,
        f"/ws/{consumer.module}/{workspace_id}",
        user=user,
        url_kwargs={"workspace_id": workspace_id},
    )
    await socket.hello()
    return socket


class TestTheSocketWritesPresence:
    async def test_connect_makes_the_user_live(self, user):
        socket = await _open(Watchers, user)
        try:
            answer = presence.is_live(user.pk)
            assert answer["live"] is True
            assert answer["sessions"] == 1
            assert answer["last_seen"] is not None
        finally:
            await socket.close()

    async def test_disconnect_makes_the_user_not_live(self, user):
        socket = await _open(Watchers, user)
        await socket.close()
        answer = presence.is_live(user.pk)
        assert answer["live"] is False
        assert answer["sessions"] == 0
        # The lease is gone; when they were last here is not.
        assert answer["last_seen"] is not None

    async def test_two_sockets_are_two_sessions_and_one_person(self, user):
        first = await _open(Watchers, user)
        second = await _open(Watchers, user, workspace_id="43")
        try:
            assert presence.is_live(user.pk)["sessions"] == 2
        finally:
            await first.close()
            await second.close()
        assert presence.is_live(user.pk)["live"] is False

    async def test_one_socket_closing_leaves_the_other_live(self, user):
        first = await _open(Watchers, user)
        second = await _open(Watchers, user, workspace_id="43")
        await first.close()
        try:
            answer = presence.is_live(user.pk)
            assert answer["live"] is True
            assert answer["sessions"] == 1
        finally:
            await second.close()

    async def test_a_refused_socket_is_never_counted(self, user):
        class Closed(Watchers):
            async def authorize(self, scope, stream_key):
                return False

        socket = await open_stream(
            Closed,
            "/ws/fake/42",
            user=user,
            url_kwargs={"workspace_id": "42"},
            expect_accept=False,
        )
        assert socket.connected is False
        assert presence.is_live(user.pk)["live"] is False

    async def test_presence_is_scoped_per_stream_family(self, user):
        socket = await _open(OtherModuleWatchers, user)
        try:
            assert presence.is_live(user.pk, family="chat")["live"] is True
            assert presence.is_live(user.pk, family="video")["live"] is False
            assert presence.is_live(user.pk)["live"] is True
        finally:
            await socket.close()


class TestTheLease:
    def test_a_session_that_is_not_renewed_expires(self, settings, monkeypatch):
        settings.STAPEL_REALTIME = {"PRESENCE_TTL_S": 30}
        presence.record_connect("u-1", "session-a", family="chat")
        assert presence.is_live("u-1")["live"] is True

        # No disconnect ever runs for a worker that was killed; the lease is
        # what stops it counting.
        real_time = time.time
        monkeypatch.setattr(presence.time, "time", lambda: real_time() + 31)
        answer = presence.is_live("u-1")
        assert answer["live"] is False
        assert answer["sessions"] == 0

    def test_a_heartbeat_renews_it(self, settings, monkeypatch):
        settings.STAPEL_REALTIME = {"PRESENCE_TTL_S": 30}
        presence.record_connect("u-1", "session-a")
        real_time = time.time
        monkeypatch.setattr(presence.time, "time", lambda: real_time() + 20)
        presence.record_heartbeat("u-1", "session-a")
        monkeypatch.setattr(presence.time, "time", lambda: real_time() + 45)
        assert presence.is_live("u-1")["live"] is True

    def test_a_dead_session_is_pruned_rather_than_accumulated(self, settings, monkeypatch):
        settings.STAPEL_REALTIME = {"PRESENCE_TTL_S": 30}
        presence.record_connect("u-1", "crashed-worker")
        real_time = time.time
        monkeypatch.setattr(presence.time, "time", lambda: real_time() + 31)
        presence.record_connect("u-1", "fresh")
        doc = presence._cache().get(presence._key("u-1"))
        assert list(doc["sessions"]) == ["fresh"]

    def test_zero_ttl_disables_the_registry(self, settings):
        settings.STAPEL_REALTIME = {"PRESENCE_TTL_S": 0}
        assert presence.record_connect("u-1", "session-a") is False
        assert presence.is_live("u-1") == {
            "live": False,
            "sessions": 0,
            "last_seen": None,
        }


class TestReads:
    def test_an_unknown_user_is_offline_not_an_error(self):
        assert presence.is_live("nobody") == {
            "live": False,
            "sessions": 0,
            "last_seen": None,
        }

    def test_batch_answers_for_every_id_asked(self):
        presence.record_connect("u-1", "a")
        presence.record_connect("u-2", "b")
        answer = presence.live_batch(["u-1", "u-2", "u-3"])
        assert set(answer) == {"u-1", "u-2", "u-3"}
        assert answer["u-1"]["live"] is True
        assert answer["u-2"]["live"] is True
        # Not omitted: the caller holds that id for a reason.
        assert answer["u-3"] == {"live": False, "sessions": 0, "last_seen": None}

    def test_batch_is_capped(self):
        with pytest.raises(ValueError, match="at most 100"):
            presence.live_batch([f"u-{n}" for n in range(101)])

    def test_batch_of_nothing_is_nothing(self):
        assert presence.live_batch([]) == {}

    def test_an_id_that_cannot_travel_in_a_cache_key_is_digested(self):
        weird = "user id with spaces\nand a newline"
        presence.record_connect(weird, "a")
        assert presence.is_live(weird)["live"] is True
        assert " " not in presence._key(weird)

    def test_a_corrupt_document_reads_as_offline(self):
        presence._cache().set(presence._key("u-1"), "not a document", timeout=60)
        assert presence.is_live("u-1")["live"] is False

    def test_a_read_never_raises_when_the_cache_is_down(self, monkeypatch):
        class Broken:
            def get(self, key):
                raise RuntimeError("redis is gone")

            def get_many(self, keys):
                raise RuntimeError("redis is gone")

        monkeypatch.setattr(presence, "_cache", lambda: Broken())
        assert presence.is_live("u-1")["live"] is False
        assert presence.live_batch(["u-1"])["u-1"]["live"] is False

    def test_a_write_never_raises_when_the_cache_is_down(self, monkeypatch):
        class Broken:
            def get(self, key):
                raise RuntimeError("redis is gone")

        monkeypatch.setattr(presence, "_cache", lambda: Broken())
        assert presence.record_connect("u-1", "a") is False
        assert presence.record_disconnect("u-1", "a") is False


class TestTheCommSurface:
    def test_both_functions_are_registered(self):
        from stapel_core.comm import function_registry

        assert function_registry.get("realtime.is_live") is not None
        assert function_registry.get("realtime.live_batch") is not None

    def test_every_function_has_a_committed_schema(self):
        committed = {
            path.stem
            for path in (REPO / "schemas" / "functions").glob("*.json")
        }
        assert committed == {"realtime.is_live", "realtime.live_batch"}
        for name in committed:
            schema = json.loads(
                (REPO / "schemas" / "functions" / f"{name}.json").read_text()
            )
            assert schema["title"] == name
            assert schema["additionalProperties"] is False

    def test_is_live_answers_over_comm(self):
        from stapel_core.comm import call

        presence.record_connect("u-1", "a", family="chat")
        answer = call("realtime.is_live", {"user_id": "u-1"})
        assert answer["live"] is True
        assert answer["sessions"] == 1
        assert call("realtime.is_live", {"user_id": "u-1", "family": "video"})[
            "live"
        ] is False

    def test_live_batch_answers_over_comm(self):
        from stapel_core.comm import call

        presence.record_connect("u-1", "a")
        answer = call("realtime.live_batch", {"user_ids": ["u-1", "u-2"]})
        assert answer["users"]["u-1"]["live"] is True
        assert answer["users"]["u-2"]["live"] is False

    def test_the_batch_schema_refuses_more_than_a_hundred(self):
        from stapel_core.comm import call
        from stapel_core.comm.exceptions import FunctionCallError, SchemaValidationError

        # The schema refuses it before the handler runs; the handler's own
        # ValueError (surfacing as FunctionCallError) is the belt for a
        # deployment that turned schema validation off.
        with pytest.raises((SchemaValidationError, FunctionCallError)):
            call(
                "realtime.live_batch",
                {"user_ids": [f"u-{n}" for n in range(101)]},
            )
