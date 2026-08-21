"""The wire envelope v1 — the contract two independently written halves share."""
import json
import pathlib

import pytest

from stapel_realtime import envelope as wire

SCHEMA_PATH = (
    pathlib.Path(__file__).resolve().parent.parent / "schemas" / "wire" / "envelope.v1.json"
)


class TestBuild:
    def test_minimal_frame(self):
        assert wire.frame("ping") == {"v": 1, "type": "ping", "payload": {}}

    def test_seq_and_stream_are_omitted_when_unset(self):
        built = wire.frame("ephemeral", {"a": 1})
        assert "seq" not in built and "stream" not in built

    def test_journal_frame_carries_seq_and_stream(self):
        built = wire.frame("live", {"a": 1}, seq=7, stream="chat:conv:1")
        assert built["seq"] == 7
        assert built["stream"] == "chat:conv:1"

    def test_payload_is_copied_not_aliased(self):
        payload = {"a": 1}
        built = wire.frame("ephemeral", payload)
        payload["a"] = 2
        assert built["payload"] == {"a": 1}

    def test_error_frame_shape(self):
        built = wire.error_frame("resync", "gap too wide")
        assert built["type"] == "error"
        assert built["payload"] == {"code": "resync", "message": "gap too wide"}


class TestParse:
    def test_round_trip(self):
        parsed = wire.parse_frame(wire.frame("hello", {"last_seq": 3}))
        assert parsed.type == "hello"
        assert parsed.payload == {"last_seq": 3}

    @pytest.mark.parametrize(
        "raw",
        [
            "not a dict",
            {"type": "ping", "payload": {}},                 # no version
            {"v": 2, "type": "ping", "payload": {}},         # unknown version
            {"v": 1, "payload": {}},                         # no type
            {"v": 1, "type": "", "payload": {}},             # empty type
            {"v": 1, "type": "ping", "payload": []},         # payload not an object
            {"v": 1, "type": "live", "payload": {}, "seq": "x"},
            {"v": 1, "type": "live", "payload": {}, "stream": 7},
        ],
    )
    def test_rejects_malformed(self, raw):
        with pytest.raises(wire.InvalidEnvelope):
            wire.parse_frame(raw)

    def test_unknown_type_parses_so_the_consumer_can_answer_bad_type(self):
        """An unknown *type* is a protocol answer; an unknown *version* is not."""
        parsed = wire.parse_frame({"v": 1, "type": "whatever", "payload": {}})
        assert parsed.type == "whatever"

    def test_missing_payload_defaults_to_empty(self):
        assert wire.parse_frame({"v": 1, "type": "ping"}).payload == {}


class TestPublishedSchema:
    """The envelope ships as a JSON schema — the L1 exception spec 4.1 grants."""

    @pytest.fixture(scope="class")
    @classmethod
    def schema(cls):
        return json.loads(SCHEMA_PATH.read_text())

    def test_schema_frame_types_match_the_code(self, schema):
        declared = set(schema["properties"]["type"]["enum"])
        assert declared == wire.CLIENT_FRAME_TYPES | wire.SERVER_FRAME_TYPES

    def test_schema_error_codes_match_the_code(self, schema):
        declared = set(schema["$defs"]["error"]["properties"]["code"]["enum"])
        assert declared == {
            wire.ERROR_BAD_ENVELOPE,
            wire.ERROR_BAD_TYPE,
            wire.ERROR_RESYNC,
            wire.ERROR_UNAUTHORIZED,
        }

    @pytest.mark.parametrize(
        "built",
        [
            wire.frame("ping"),
            wire.frame("welcome", {"server_seq": 4}),
            wire.frame("live", {"x": 1}, seq=9, stream="chat:conv:1"),
            wire.error_frame("resync", "too wide"),
        ],
    )
    def test_emitted_frames_validate(self, schema, built):
        jsonschema = pytest.importorskip("jsonschema")
        jsonschema.validate(built, schema)

    def test_a_frame_with_an_extra_top_level_key_is_rejected(self, schema):
        jsonschema = pytest.importorskip("jsonschema")
        bad = {**wire.frame("ping"), "sneaky": 1}
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bad, schema)
