"""The delivery seam: the core's contract, best-effort by design."""
import pytest
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from stapel_realtime import delivery
from stapel_realtime.streams import group_name

SIGNAL_FRAME = {
    "v": 1,
    "type": "recording.status",
    "stream": "recordings:ws:42",
    "payload": {"recording_id": "7", "status": "ready"},
}


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


class TestCoreSeam:
    """`transport(stream_key, frame)` — the signature the core calls."""

    def test_it_is_registered_under_the_name_the_axis_uses(self):
        from stapel_core.comm.signals import signal_transport

        # AppConfig.ready() registered it; selecting it is still the host's call.
        assert delivery.TRANSPORT_NAME == "channels"
        from django.test import override_settings

        with override_settings(STAPEL_COMM={"SIGNAL_TRANSPORT": "channels"}):
            assert signal_transport() is delivery.deliver

    def test_the_default_axis_stays_no_op(self):
        from stapel_core.comm.signals import signal_transport

        assert signal_transport() is None

    def test_a_signal_emitted_through_the_core_reaches_the_group(
        self, transactional_db, listener, settings
    ):
        """End to end across the seam: comm.signal() -> layer, after commit."""
        from stapel_core.comm import signal

        settings.STAPEL_COMM = {
            "SIGNAL_TRANSPORT": "channels",
            "OUTBOX_ENABLED": False,
            "ACTION_TRANSPORT": "inprocess",
        }
        listener("recordings:ws:42")
        signal("recordings:ws:42", "recording.status", {"status": "ready"})
        message = _drain()
        assert message["type"] == delivery.GROUP_TYPE_SIGNAL
        assert message["frame"]["type"] == "recording.status"
        assert message["frame"]["payload"] == {"status": "ready"}

    def test_a_signal_never_outruns_its_transaction(
        self, db, listener, settings, django_capture_on_commit_callbacks
    ):
        from django.db import transaction
        from stapel_core.comm import signal

        settings.STAPEL_COMM = {
            "SIGNAL_TRANSPORT": "channels",
            "OUTBOX_ENABLED": False,
            "ACTION_TRANSPORT": "inprocess",
        }
        listener("recordings:ws:42")
        sent = []
        with django_capture_on_commit_callbacks() as callbacks:
            with transaction.atomic():
                signal("recordings:ws:42", "recording.status", {"status": "ready"})
        assert sent == [] and len(callbacks) == 1, "delivery must wait for the commit"


class TestDeliver:
    def test_a_signal_frame_is_forwarded_verbatim(self, listener):
        """Re-wrapping would put a second envelope between two agreed halves."""
        listener("recordings:ws:42")
        assert delivery.deliver("recordings:ws:42", SIGNAL_FRAME)
        message = _drain()
        assert message["type"] == delivery.GROUP_TYPE_SIGNAL
        assert message["frame"] == SIGNAL_FRAME
        assert "seq" not in message["frame"], "an ephemeral frame cannot carry a seq"

    def test_a_frame_claiming_a_protocol_type_is_refused(self, listener):
        """The core rejects this at emit; a direct caller might not have."""
        listener("recordings:ws:42")
        refused = {**SIGNAL_FRAME, "type": "welcome"}
        assert delivery.deliver("recordings:ws:42", refused) is False

    def test_a_non_dict_frame_is_refused(self):
        assert delivery.deliver("recordings:ws:42", "not a frame") is False

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
        assert delivery.deliver("recordings:ws:42", SIGNAL_FRAME) is False
        assert delivery.deliver_frame("chat:conv:1", {}, seq=1) is False
        assert delivery.revoke("chat:conv:1", 1) is False

    def test_a_failing_layer_never_raises_into_the_caller(self, monkeypatch):
        class Exploding:
            async def group_send(self, *a, **k):
                raise RuntimeError("redis is down")

        monkeypatch.setattr(delivery, "_channel_layer", lambda: Exploding())
        assert delivery.deliver("recordings:ws:42", SIGNAL_FRAME) is False

    def test_an_invalid_stream_key_does_not_escape_either(self):
        assert delivery.deliver("not a key", SIGNAL_FRAME) is False
