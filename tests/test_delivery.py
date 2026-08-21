"""The delivery seam: best-effort by contract, on_commit by construction."""
import pytest
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from stapel_realtime import delivery
from stapel_realtime.streams import group_name


@pytest.fixture
def listener():
    """A raw channel joined to a stream's group — the layer's-eye view."""
    layer = get_channel_layer()

    def _join(stream_key, channel="test-channel"):
        async_to_sync(layer.group_add)(group_name(stream_key), channel)
        return channel

    return _join


def _drain(channel="test-channel"):
    layer = get_channel_layer()
    return async_to_sync(layer.receive)(channel)


class TestDeliver:
    def test_ephemeral_signal_reaches_the_group(self, listener):
        listener("recordings:ws:42")
        assert delivery.deliver("recordings:ws:42", "recording.status", {"id": "7"})
        message = _drain()
        assert message["type"] == delivery.GROUP_TYPE_SIGNAL
        assert message["signal_type"] == "recording.status"
        assert message["payload"] == {"id": "7"}
        assert message["stream"] == "recordings:ws:42"

    def test_a_signal_carries_no_seq(self):
        """Ephemeral frames physically cannot become journal frames."""
        listener_channel = "no-seq"
        layer = get_channel_layer()
        async_to_sync(layer.group_add)(group_name("recordings:ws:42"), listener_channel)
        delivery.deliver("recordings:ws:42", "x", {})
        assert "seq" not in _drain(listener_channel)

    def test_journal_frame_carries_its_seq(self, listener):
        listener("chat:conv:1")
        assert delivery.deliver_frame("chat:conv:1", {"body": "hi"}, seq=12)
        message = _drain()
        assert message["type"] == delivery.GROUP_TYPE_FRAME
        assert message["seq"] == 12

    def test_revoke_names_the_user(self, listener):
        listener("recordings:ws:42")
        assert delivery.revoke("recordings:ws:42", 7, reason="left_workspace")
        message = _drain()
        assert message["type"] == delivery.GROUP_TYPE_REVOKE
        assert message["user_id"] == "7"
        assert message["reason"] == "left_workspace"

    def test_revoke_without_a_user_targets_the_whole_stream(self, listener):
        listener("chat:conv:1")
        delivery.revoke("chat:conv:1", None)
        assert _drain()["user_id"] is None


class TestBestEffort:
    def test_no_channel_layer_is_a_silent_no_op(self, settings):
        """HTTP-only hosts and most tests: correctness must not depend on this."""
        settings.CHANNEL_LAYERS = {}
        assert delivery.deliver("recordings:ws:42", "x", {}) is False
        assert delivery.deliver_frame("chat:conv:1", {}, seq=1) is False
        assert delivery.revoke("chat:conv:1", 1) is False

    def test_a_failing_layer_never_raises_into_the_caller(self, monkeypatch):
        class Exploding:
            async def group_send(self, *a, **k):
                raise RuntimeError("redis is down")

        monkeypatch.setattr(delivery, "_channel_layer", lambda: Exploding())
        assert delivery.deliver("recordings:ws:42", "x", {}) is False

    def test_an_invalid_stream_key_does_not_escape_either(self):
        assert delivery.deliver("not a key", "x", {}) is False


class TestOnCommit:
    def test_a_signal_never_outruns_its_transaction(
        self, db, monkeypatch, django_capture_on_commit_callbacks
    ):
        from django.db import transaction

        sent = []
        monkeypatch.setattr(
            delivery, "deliver", lambda *args: sent.append(args) or True
        )
        with django_capture_on_commit_callbacks(execute=True):
            with transaction.atomic():
                delivery.signal_on_commit(
                    "recordings:ws:42", "recording.status", {"id": "7"}
                )
                # Nothing on the wire yet — the row this describes is uncommitted.
                assert sent == []
        assert sent == [("recordings:ws:42", "recording.status", {"id": "7"})]

    def test_outside_a_transaction_it_still_delivers(self, transactional_db, listener):
        listener("recordings:ws:42")
        delivery.signal_on_commit("recordings:ws:42", "recording.status", {"id": "7"})
        assert _drain()["signal_type"] == "recording.status"


class TestTransportObject:
    def test_it_is_callable_and_has_send(self, listener):
        """Core resolves a transport either way; fitting one shape is a skew trap."""
        listener("recordings:ws:42")
        assert delivery.channels_transport("recordings:ws:42", "a", {}) is True
        assert _drain()["signal_type"] == "a"
        assert delivery.channels_transport.send("recordings:ws:42", "b", {}) is True
        assert _drain()["signal_type"] == "b"
