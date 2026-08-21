"""The authorize seam. Every path through it that is not an explicit yes is a no."""
import pytest

from stapel_realtime import authorize

# pytest is in asyncio auto mode (pyproject): every async test is a socket test.


class FakeUser:
    is_authenticated = True

    def __init__(self, pk=7):
        self.pk = pk


class TestDefaultDeny:
    async def test_deny_refuses_and_says_why(self, caplog):
        assert await authorize.deny({}, "recordings:ws:42") is False
        assert "did not implement authorize()" in caplog.text


class TestUserIdFromScope:
    def test_authenticated_user(self):
        assert authorize.user_id_from_scope({"user": FakeUser(3)}) == 3

    def test_no_user(self):
        assert authorize.user_id_from_scope({}) is None

    def test_anonymous_user(self):
        class Anon:
            is_authenticated = False
            pk = None

        assert authorize.user_id_from_scope({"user": Anon()}) is None


class TestWorkspaceCapability:
    @pytest.fixture
    def granted(self, monkeypatch):
        calls = []

        def fake(workspace_id, user_id, capability):
            calls.append((workspace_id, user_id, capability))
            return object()  # a Membership stand-in

        monkeypatch.setattr(
            "stapel_core.django.workspaces.require_capability", fake
        )
        return calls

    @pytest.fixture
    def refused(self, monkeypatch):
        monkeypatch.setattr(
            "stapel_core.django.workspaces.require_capability",
            lambda *a: None,
        )

    async def test_it_asks_the_same_question_http_asks(self, granted, db):
        gate = authorize.WorkspaceCapability("recordings.read")
        assert await gate({"user": FakeUser(7)}, "recordings:ws:42") is True
        assert granted == [("42", 7, "recordings.read")]

    async def test_no_capability_is_a_refusal(self, refused, db):
        gate = authorize.WorkspaceCapability("recordings.read")
        assert await gate({"user": FakeUser(7)}, "recordings:ws:42") is False

    async def test_an_unreachable_peer_is_a_refusal_not_an_allowance(
        self, monkeypatch, db
    ):
        def explode(*args):
            raise RuntimeError("workspaces is down")

        monkeypatch.setattr(
            "stapel_core.django.workspaces.require_capability", explode
        )
        gate = authorize.WorkspaceCapability("recordings.read")
        assert await gate({"user": FakeUser(7)}, "recordings:ws:42") is False

    async def test_an_anonymous_scope_never_reaches_the_peer(self, granted, db):
        gate = authorize.WorkspaceCapability("recordings.read")
        assert await gate({}, "recordings:ws:42") is False
        assert granted == []

    async def test_a_non_workspace_stream_is_refused(self, granted, db):
        """An authorizer that passes a key shape it was not built for is a hole."""
        gate = authorize.WorkspaceCapability("chat.read")
        assert await gate({"user": FakeUser(7)}, "chat:conv:99") is False
        assert granted == []

    async def test_an_unparseable_key_is_refused(self, granted, db):
        gate = authorize.WorkspaceCapability("recordings.read")
        assert await gate({"user": FakeUser(7)}, "garbage") is False
        assert granted == []

    async def test_the_topic_does_not_change_the_workspace_asked_about(
        self, granted, db
    ):
        gate = authorize.WorkspaceCapability("tasks.read")
        assert await gate({"user": FakeUser(7)}, "tasks:ws:42:board") is True
        assert granted == [("42", 7, "tasks.read")]
