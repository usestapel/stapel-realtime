"""Stream-key canon: the scope is in the name, and the name maps to a group."""
import pytest

from stapel_realtime import streams


class TestBuildAndParse:
    def test_three_segment_key(self):
        assert streams.build_stream_key("recordings", "ws", "42") == "recordings:ws:42"

    def test_topic_is_the_optional_fourth(self):
        key = streams.build_stream_key("tasks", "ws", "42", "board")
        assert key == "tasks:ws:42:board"
        assert streams.parse_stream_key(key).topic == "board"

    def test_workspace_helper(self):
        assert streams.workspace_stream("recordings", "42") == "recordings:ws:42"

    def test_parsed_fields(self):
        key = streams.parse_stream_key("chat:conv:3d2b")
        assert (key.module, key.scope_type, key.scope_id, key.topic) == (
            "chat",
            "conv",
            "3d2b",
            None,
        )
        assert not key.is_workspace_scoped
        assert streams.parse_stream_key("recordings:ws:1").is_workspace_scoped

    def test_str_round_trips(self):
        assert str(streams.parse_stream_key("tasks:ws:42:board")) == "tasks:ws:42:board"

    @pytest.mark.parametrize(
        "bad",
        [
            "",
            "recordings",
            "recordings:ws",
            "a:b:c:d:e",
            "recordings:ws:has space",
            "recordings:ws:has/slash",
            "recordings::42",
        ],
    )
    def test_rejects_non_canonical(self, bad):
        with pytest.raises(streams.InvalidStreamKey):
            streams.parse_stream_key(bad)

    def test_build_validates_too(self):
        with pytest.raises(streams.InvalidStreamKey):
            streams.build_stream_key("recordings", "ws", "has space")


class TestGroupName:
    def test_colons_become_dots(self):
        assert streams.group_name("recordings:ws:42") == "recordings.ws.42"

    def test_uuid_scope_survives(self):
        key = "recordings:ws:9f1c2b3d-4e5f-6071-8293-a4b5c6d7e8f9"
        assert streams.group_name(key) == key.replace(":", ".")

    def test_accepts_a_parsed_key(self):
        parsed = streams.parse_stream_key("chat:conv:7")
        assert streams.group_name(parsed) == "chat.conv.7"

    def test_group_names_stay_within_the_channels_ceiling(self):
        key = streams.build_stream_key("recordings", "ws", "x" * 200)
        name = streams.group_name(key)
        assert len(name) < 100
        assert set(name) <= set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
        )

    def test_long_keys_hash_instead_of_truncating(self):
        """Truncation would make two streams share one group — a tenancy bug."""
        a = streams.build_stream_key("recordings", "ws", "x" * 200 + "a")
        b = streams.build_stream_key("recordings", "ws", "x" * 200 + "b")
        assert streams.group_name(a) != streams.group_name(b)
        assert streams.group_name(a).startswith("recordings.h.")

    def test_group_name_is_stable(self):
        key = streams.build_stream_key("recordings", "ws", "y" * 200)
        assert streams.group_name(key) == streams.group_name(key)

    def test_two_workspaces_never_share_a_group(self):
        """The load-bearing property: the scope is physically in the name."""
        assert streams.group_name("recordings:ws:1") != streams.group_name(
            "recordings:ws:2"
        )
