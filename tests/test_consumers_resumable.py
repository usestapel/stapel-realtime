"""The resumable consumer — chat's protocol, generalized and owned by one class.

The fixture below is a stand-in for a module's journal model (chat's
``Message``, dialog's turn log, a future document's op journal): a list of rows
with a monotonic ``seq``. That is the whole contract the substrate needs.
"""
import pytest
from asgiref.sync import sync_to_async

from stapel_realtime import delivery
from stapel_realtime import envelope as wire
from stapel_realtime.consumers import JournalRow, ResumableStreamConsumer
from stapel_realtime.testing import open_stream

# pytest is in asyncio auto mode (pyproject): every async test is a socket test.

STREAM = "chat:conv:7"
CONV_KWARGS = {"conversation_id": "7"}

#: The module's "database". Reset per test by the fixture below.
JOURNAL: list[JournalRow] = []


async def _allow(scope, stream_key):
    return True


class JournalConsumer(ResumableStreamConsumer):
    module = "chat"
    scope_type = "conv"
    stream_key_kwarg = "conversation_id"
    authorizer = staticmethod(_allow)

    async def get_server_seq(self) -> int:
        return JOURNAL[-1].seq if JOURNAL else 0

    async def get_replay_rows(self, after_seq, limit):
        return [row for row in JOURNAL if row.seq > after_seq][:limit]


@pytest.fixture(autouse=True)
def journal():
    JOURNAL.clear()
    yield JOURNAL
    JOURNAL.clear()


def fill(count, start=1):
    JOURNAL.extend(
        JournalRow(seq=n, payload={"body": f"m{n}"}) for n in range(start, start + count)
    )


class TestHelloReplayLive:
    async def test_a_fresh_client_replays_everything(self, user):
        fill(3)
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        welcome = await socket.hello(last_seq=0)
        assert welcome.payload["server_seq"] == 3
        for n in (1, 2, 3):
            frame = await socket.expect(wire.REPLAY)
            assert frame.seq == n
            assert frame.payload == {"body": f"m{n}"}
        done = await socket.expect(wire.REPLAY_DONE)
        assert done.payload["up_to_seq"] == 3
        await socket.close()

    async def test_a_resuming_client_only_gets_the_gap(self, user):
        fill(5)
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        await socket.hello(last_seq=3)
        assert [f.seq for f in await socket.drain()] == [4, 5, None]
        await socket.close()

    async def test_live_frames_follow_the_replay(self, user):
        fill(2)
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        await socket.hello(last_seq=2)
        await socket.expect(wire.REPLAY_DONE)
        await sync_to_async(delivery.deliver_frame)(STREAM, {"body": "m3"}, seq=3)
        frame = await socket.expect(wire.LIVE)
        assert frame.seq == 3 and frame.payload == {"body": "m3"}
        await socket.close()

    async def test_frames_carry_the_stream_for_a_future_multiplex(self, user):
        fill(1)
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        await socket.hello(last_seq=0)
        assert (await socket.expect(wire.REPLAY)).stream == STREAM
        await socket.close()


class TestDedup:
    async def test_a_live_frame_racing_the_replay_is_not_sent_twice(self, user):
        """The replay/live overlap after a resume must not double-deliver."""
        fill(3)
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        await socket.hello(last_seq=0)
        assert [f.seq for f in await socket.drain()] == [1, 2, 3, None]
        # The fan-out for row 2 arrives late — the client already has it.
        await sync_to_async(delivery.deliver_frame)(STREAM, {"body": "m2"}, seq=2)
        assert await socket.receive_nothing()
        await sync_to_async(delivery.deliver_frame)(STREAM, {"body": "m4"}, seq=4)
        assert (await socket.expect(wire.LIVE)).seq == 4
        await socket.close()

    async def test_hello_advances_the_cursor_to_what_the_client_holds(self, user):
        fill(5)
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        await socket.hello(last_seq=5)
        await socket.expect(wire.REPLAY_DONE)
        await sync_to_async(delivery.deliver_frame)(STREAM, {"body": "m5"}, seq=5)
        assert await socket.receive_nothing()
        await socket.close()


class TestReplayWindow:
    async def test_a_gap_wider_than_the_window_answers_resync(self, user, settings):
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 3600, "MAX_REPLAY": 10}
        fill(50)
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        welcome = await socket.hello(last_seq=0)
        assert welcome.payload["server_seq"] == 50
        resync = await socket.expect(wire.RESYNC)
        assert resync.payload == {"gap": 50, "window": 10, "server_seq": 50}
        # No infinite rewind: nothing is replayed at all, and the socket lives.
        assert await socket.receive_nothing()
        await socket.send(wire.PING)
        assert (await socket.receive_raw()).type == wire.PONG
        await socket.close()

    async def test_a_gap_inside_the_window_replays_normally(self, user, settings):
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 3600, "MAX_REPLAY": 10}
        fill(50)
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        await socket.hello(last_seq=45)
        assert [f.seq for f in await socket.drain()] == [46, 47, 48, 49, 50, None]
        await socket.close()

    async def test_the_window_bounds_the_query_too(self, user, settings):
        """`limit` reaches the module's hook, so a wide stream cannot be dredged."""
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 3600, "MAX_REPLAY": 3}
        fill(3)
        seen = {}

        class Recording(JournalConsumer):
            async def get_replay_rows(self, after_seq, limit):
                seen["limit"] = limit
                return await super().get_replay_rows(after_seq, limit)

        socket = await open_stream(Recording, user=user, url_kwargs=CONV_KWARGS)
        await socket.hello(last_seq=0)
        await socket.drain()
        assert seen["limit"] == 3
        await socket.close()


class TestBadInput:
    async def test_a_non_integer_last_seq_is_an_error_not_a_crash(self, user):
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        await socket.send(wire.HELLO, {"last_seq": "yesterday"})
        assert (await socket.expect(wire.ERROR)).payload["code"] == wire.ERROR_BAD_ENVELOPE
        await socket.close()

    async def test_a_missing_last_seq_means_from_the_beginning(self, user):
        fill(2)
        socket = await open_stream(JournalConsumer, user=user, url_kwargs=CONV_KWARGS)
        await socket.hello()
        assert [f.seq for f in await socket.drain()] == [1, 2, None]
        await socket.close()


class TestUnimplementedHooks:
    async def test_the_base_class_refuses_to_guess(self, user):
        class Naked(ResumableStreamConsumer):
            module = "chat"
            scope_type = "conv"
            stream_key_kwarg = "conversation_id"
            authorizer = staticmethod(_allow)

        socket = await open_stream(Naked, user=user, url_kwargs=CONV_KWARGS)
        await socket.send(wire.HELLO, {})
        with pytest.raises(Exception) as caught:
            await socket.receive(timeout=1)
        assert "get_server_seq" in str(caught.value)
