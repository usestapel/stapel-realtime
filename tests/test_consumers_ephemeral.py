"""The ephemeral consumer — Signal delivery, and the gates around it."""
import asyncio
import time

from asgiref.sync import sync_to_async

from stapel_realtime import delivery
from stapel_realtime import envelope as wire
from stapel_realtime.close_codes import (
    CLOSE_FORBIDDEN,
    CLOSE_HEARTBEAT_TIMEOUT,
    CLOSE_OVERFLOW,
    CLOSE_PROTOCOL_ERROR,
    CLOSE_REVOKED,
    CLOSE_STREAM_UNKNOWN,
    CLOSE_UNAUTHENTICATED,
)
from stapel_realtime.consumers import EphemeralStreamConsumer
from stapel_realtime.testing import open_stream

# pytest is in asyncio auto mode (pyproject): every async test is a socket test.


async def _allow(scope, stream_key):
    return True


class OpenConsumer(EphemeralStreamConsumer):
    module = "recordings"
    scope_type = "ws"
    stream_key_kwarg = "workspace_id"
    authorizer = staticmethod(_allow)


class ForgetfulConsumer(EphemeralStreamConsumer):
    """A module author who wired a consumer and forgot the authorize hook."""

    module = "recordings"
    scope_type = "ws"
    stream_key_kwarg = "workspace_id"


WS_KWARGS = {"workspace_id": "42"}
STREAM = "recordings:ws:42"


class TestSubscriptionGates:
    async def test_forgetting_authorize_subscribes_nobody(self, user):
        """Fail-closed: an unimplemented hook is a refusal, not an opening."""
        socket = await open_stream(
            ForgetfulConsumer, user=user, url_kwargs=WS_KWARGS, expect_accept=False
        )
        assert socket.close_code == CLOSE_FORBIDDEN

    async def test_anonymous_scope_is_refused(self):
        socket = await open_stream(
            OpenConsumer, user=None, url_kwargs=WS_KWARGS, expect_accept=False
        )
        assert socket.close_code == CLOSE_UNAUTHENTICATED

    async def test_an_unresolvable_stream_key_closes_4404(self, user):
        socket = await open_stream(
            OpenConsumer, user=user, url_kwargs={}, expect_accept=False
        )
        assert socket.close_code == CLOSE_STREAM_UNKNOWN

    async def test_a_denied_authorizer_never_joins_the_group(self, user):
        async def refuse(scope, stream_key):
            return False

        class Refusing(OpenConsumer):
            authorizer = staticmethod(refuse)

        socket = await open_stream(
            Refusing, user=user, url_kwargs=WS_KWARGS, expect_accept=False
        )
        assert socket.close_code == CLOSE_FORBIDDEN

    async def test_the_authorizer_sees_the_resolved_stream_key(self, user):
        seen = []

        async def record(scope, stream_key):
            seen.append(stream_key)
            return True

        class Recording(OpenConsumer):
            authorizer = staticmethod(record)

        socket = await open_stream(Recording, user=user, url_kwargs=WS_KWARGS)
        assert seen == [STREAM]
        await socket.close()

    async def test_hello_re_asks_once_the_cached_verdict_expires(
        self, user, settings
    ):
        """Subscription AND re-subscription are checked (substrate 1.6)."""
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 3600, "AUTHORIZE_CACHE_S": 0}
        verdicts = [True, True, False]
        asked = []

        async def fading(scope, stream_key):
            asked.append(stream_key)
            return verdicts.pop(0)

        class Fading(OpenConsumer):
            authorizer = staticmethod(fading)

        socket = await open_stream(Fading, user=user, url_kwargs=WS_KWARGS)
        await socket.hello()               # second verdict: still allowed
        await socket.send(wire.HELLO, {})  # third verdict: access is gone
        frame = await socket.expect(wire.ERROR)
        assert frame.payload["code"] == wire.ERROR_UNAUTHORIZED
        assert await socket.wait_closed() == CLOSE_FORBIDDEN
        assert len(asked) == 3

    async def test_the_cache_keeps_hello_off_the_capability_path(
        self, user, settings
    ):
        """Re-asking on every hello without a cache is a round-trip per reconnect."""
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 3600, "AUTHORIZE_CACHE_S": 300}
        asked = []

        async def counting(scope, stream_key):
            asked.append(stream_key)
            return True

        class Counting(OpenConsumer):
            authorizer = staticmethod(counting)

        socket = await open_stream(Counting, user=user, url_kwargs=WS_KWARGS)
        for _ in range(3):
            await socket.hello()
        assert len(asked) == 1, "the connect verdict should still be warm"
        await socket.close()


class TestDelivery:
    async def test_a_signal_reaches_a_subscriber(self, user):
        socket = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        await socket.hello()
        await sync_to_async(delivery.deliver)(
            STREAM, "recording.status", {"recording_id": "7", "status": "ready"}
        )
        frame = await socket.expect(wire.EPHEMERAL)
        assert frame.payload == {
            "signal": "recording.status",
            "recording_id": "7",
            "status": "ready",
        }
        assert frame.seq is None, "an ephemeral frame must never carry a seq"
        assert frame.stream == STREAM
        await socket.close()

    async def test_a_signal_on_another_stream_is_not_delivered(self, user):
        socket = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        await socket.hello()
        await sync_to_async(delivery.deliver)("recordings:ws:99", "x", {})
        assert await socket.receive_nothing()
        await socket.close()

    async def test_two_subscribers_both_get_it(self, user, other_user):
        a = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        b = await open_stream(OpenConsumer, user=other_user, url_kwargs=WS_KWARGS)
        await sync_to_async(delivery.deliver)(STREAM, "recording.status", {"id": "7"})
        assert (await a.expect(wire.EPHEMERAL)).payload["id"] == "7"
        assert (await b.expect(wire.EPHEMERAL)).payload["id"] == "7"
        await a.close()
        await b.close()

    async def test_welcome_says_the_stream_has_no_journal(self, user):
        socket = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        assert (await socket.hello()).payload == {"ephemeral": True}
        await socket.close()


class TestProtocol:
    async def test_ping_gets_a_pong(self, user):
        socket = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        await socket.send(wire.PING)
        assert (await socket.receive_raw()).type == wire.PONG
        await socket.close()

    async def test_an_unknown_frame_type_is_answered_not_fatal(self, user):
        socket = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        await socket.send("send")  # the chat frame this consumer does not accept
        frame = await socket.expect(wire.ERROR)
        assert frame.payload["code"] == wire.ERROR_BAD_TYPE
        # still alive
        await socket.send(wire.PING)
        assert (await socket.receive_raw()).type == wire.PONG
        await socket.close()

    async def test_persistent_garbage_closes_4400(self, user):
        socket = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        for _ in range(3):
            await socket.communicator.send_json_to({"nope": True})
            frame = await socket.expect(wire.ERROR)
            assert frame.payload["code"] == wire.ERROR_BAD_ENVELOPE
        assert await socket.wait_closed() == CLOSE_PROTOCOL_ERROR


class TestRevoke:
    async def test_the_named_user_is_kicked_immediately(self, user, other_user):
        victim = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        bystander = await open_stream(
            OpenConsumer, user=other_user, url_kwargs=WS_KWARGS
        )
        await sync_to_async(delivery.revoke)(STREAM, user.pk, reason="left_workspace")

        frame = await victim.expect(wire.REVOKED)
        assert frame.payload["reason"] == "left_workspace"
        assert await victim.wait_closed() == CLOSE_REVOKED
        # The other subscriber is untouched and still receiving.
        assert await bystander.receive_nothing()
        await sync_to_async(delivery.deliver)(STREAM, "recording.status", {"id": "9"})
        assert (await bystander.expect(wire.EPHEMERAL)).payload["id"] == "9"
        await bystander.close()

    async def test_a_stream_wide_revoke_kicks_everyone(self, user, other_user):
        a = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        b = await open_stream(OpenConsumer, user=other_user, url_kwargs=WS_KWARGS)
        await sync_to_async(delivery.revoke)(STREAM, None)
        assert (await a.expect(wire.REVOKED)).payload["reason"] == "access_revoked"
        assert (await b.expect(wire.REVOKED)).payload["reason"] == "access_revoked"
        assert await a.wait_closed() == CLOSE_REVOKED
        assert await b.wait_closed() == CLOSE_REVOKED


class StalledConsumer(OpenConsumer):
    """A socket whose wire has stopped moving — a genuinely slow reader."""

    gate: asyncio.Event

    async def send_json(self, content, close=False):
        await type(self).gate.wait()
        await super().send_json(content, close=close)


class TestBackpressure:
    async def test_a_stalled_wire_drops_the_socket_instead_of_buffering(
        self, user, settings
    ):
        """The producer never waits on a slow client (substrate 1.8)."""
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 3600, "SEND_QUEUE_SIZE": 4}
        StalledConsumer.gate = asyncio.Event()
        socket = await open_stream(StalledConsumer, user=user, url_kwargs=WS_KWARGS)
        for i in range(50):
            await sync_to_async(delivery.deliver)(STREAM, "flood", {"i": i})
            await asyncio.sleep(0)
        message = await socket.communicator.receive_output(timeout=2)
        assert message["type"] == "websocket.close"
        assert message["code"] == CLOSE_OVERFLOW
        StalledConsumer.gate.set()

    async def test_a_draining_reader_receives_the_whole_burst(self, user, settings):
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 3600, "SEND_QUEUE_SIZE": 4}
        socket = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        for i in range(20):
            await sync_to_async(delivery.deliver)(STREAM, "burst", {"i": i})
            await asyncio.sleep(0)
        received = [(await socket.expect(wire.EPHEMERAL)).payload["i"] for _ in range(20)]
        assert received == list(range(20))
        await socket.close()


class TestHeartbeat:
    async def test_no_pong_closes_4408(self, user, settings):
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 0.05, "HEARTBEAT_TIMEOUT_S": 0.05}
        socket = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        assert (await socket.receive_raw(timeout=2)).type == wire.PING
        assert await socket.wait_closed(timeout=2) == CLOSE_HEARTBEAT_TIMEOUT

    async def test_a_client_that_pongs_stays_connected(self, user, settings):
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 0.05, "HEARTBEAT_TIMEOUT_S": 0.5}
        socket = await open_stream(OpenConsumer, user=user, url_kwargs=WS_KWARGS)
        assert (await socket.receive_raw(timeout=2)).type == wire.PING
        await socket.send(wire.PONG)
        await asyncio.sleep(0.3)
        await sync_to_async(delivery.deliver)(STREAM, "still.here", {})
        assert (await socket.expect(wire.EPHEMERAL)).payload["signal"] == "still.here"
        await socket.close()

    async def test_a_socket_outliving_its_token_is_closed_4401(self, user, settings):
        """A socket that survives `exp` is a session with a revoked credential."""
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 0.05, "HEARTBEAT_TIMEOUT_S": 0.5}
        socket = await open_stream(
            OpenConsumer,
            user=user,
            url_kwargs=WS_KWARGS,
            claims={"exp": time.time() - 1},
        )
        assert await socket.wait_closed(timeout=2) == CLOSE_UNAUTHENTICATED

    async def test_a_live_token_is_not_disturbed(self, user, settings):
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 0.05, "HEARTBEAT_TIMEOUT_S": 0.5}
        socket = await open_stream(
            OpenConsumer,
            user=user,
            url_kwargs=WS_KWARGS,
            claims={"exp": time.time() + 3600},
        )
        assert (await socket.receive_raw(timeout=2)).type == wire.PING
        await socket.close()
