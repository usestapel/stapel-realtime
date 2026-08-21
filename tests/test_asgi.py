"""Host assembly: origin guard with the port, and routing discovery."""
from types import SimpleNamespace

import pytest

from stapel_realtime import asgi
from stapel_realtime.close_codes import CLOSE_FORBIDDEN


def fake_app(name):
    return SimpleNamespace(name=name)


class TestNormalizeOrigin:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("http://localhost:8600", "http://localhost:8600"),
            ("HTTP://Studio.Localhost:8600", "http://studio.localhost:8600"),
            ("https://app.example.com", "https://app.example.com"),
            ("https://app.example.com:443", "https://app.example.com"),
            ("http://app.example.com:80", "http://app.example.com"),
            ("https://app.example.com/some/path", "https://app.example.com"),
        ],
    )
    def test_normalization(self, raw, expected):
        assert asgi.normalize_origin(raw) == expected

    @pytest.mark.parametrize("raw", ["studio.localhost", "", "://x", "localhost:8600"])
    def test_a_bare_host_is_not_an_origin(self, raw):
        """The studio incident: 'studio.localhost' can never match anything."""
        with pytest.raises(ValueError):
            asgi.normalize_origin(raw)

    def test_the_port_is_part_of_identity(self):
        assert asgi.normalize_origin("http://x:8600") != asgi.normalize_origin(
            "http://x:8601"
        )


class TestOriginGuard:
    @pytest.fixture
    def spy(self):
        seen = []

        async def inner(scope, receive, send):
            seen.append(scope)

        inner.seen = seen
        return inner

    def _scope(self, origin=None, kind="websocket"):
        headers = [(b"origin", origin.encode())] if origin else []
        return {"type": kind, "headers": headers}

    async def _run(self, guard, scope):
        sent = []

        async def receive():
            return {"type": "websocket.connect"}

        async def send(message):
            sent.append(message)

        await guard(scope, receive, send)
        return sent

    async def test_an_allowed_origin_passes(self, spy):
        guard = asgi.OriginGuard(spy, allowed_origins=["http://localhost:8600"])
        await self._run(guard, self._scope("http://localhost:8600"))
        assert len(spy.seen) == 1

    async def test_the_same_host_on_another_port_is_refused(self, spy):
        guard = asgi.OriginGuard(spy, allowed_origins=["http://localhost:8600"])
        sent = await self._run(guard, self._scope("http://localhost:9999"))
        assert spy.seen == []
        assert sent == [{"type": "websocket.close", "code": CLOSE_FORBIDDEN}]

    async def test_an_unlisted_origin_is_refused(self, spy):
        guard = asgi.OriginGuard(spy, allowed_origins=["https://app.example.com"])
        sent = await self._run(guard, self._scope("https://evil.example.com"))
        assert sent[0]["code"] == CLOSE_FORBIDDEN

    async def test_an_empty_allowlist_disables_the_guard(self, spy):
        guard = asgi.OriginGuard(spy, allowed_origins=[])
        await self._run(guard, self._scope("https://anything.example.com"))
        assert len(spy.seen) == 1

    async def test_a_request_without_an_origin_passes(self, spy):
        """Non-browser clients send no Origin; the JWT still gates them."""
        guard = asgi.OriginGuard(spy, allowed_origins=["https://app.example.com"])
        await self._run(guard, self._scope(None))
        assert len(spy.seen) == 1

    async def test_a_malformed_entry_does_not_open_the_door(self, spy):
        guard = asgi.OriginGuard(spy, allowed_origins=["studio.localhost"])
        sent = await self._run(guard, self._scope("http://studio.localhost:8600"))
        assert sent[0]["code"] == CLOSE_FORBIDDEN

    async def test_non_websocket_scopes_pass_through(self, spy):
        guard = asgi.OriginGuard(spy, allowed_origins=["https://app.example.com"])
        await self._run(guard, self._scope("https://evil.example.com", kind="http"))
        assert len(spy.seen) == 1

    async def test_it_reads_the_setting_when_not_given_one(self, spy, settings):
        settings.STAPEL_REALTIME = {"ALLOWED_ORIGINS": ["https://app.example.com"]}
        guard = asgi.OriginGuard(spy)
        sent = await self._run(guard, self._scope("https://evil.example.com"))
        assert sent[0]["code"] == CLOSE_FORBIDDEN


class TestDiscovery:
    def test_a_module_manifest_is_collected(self):
        patterns = asgi.collect_websocket_urlpatterns(
            [fake_app("stapel_realtime.tests.fake_module")]
        )
        assert len(patterns) == 1
        assert "ws/fake" in str(patterns[0].pattern)

    def test_an_app_without_routing_contributes_nothing(self):
        assert asgi.collect_websocket_urlpatterns([fake_app("stapel_realtime")]) == []

    def test_several_apps_are_concatenated(self):
        patterns = asgi.collect_websocket_urlpatterns(
            [
                fake_app("stapel_realtime.tests.fake_module"),
                fake_app("stapel_realtime.tests.offside_module"),
            ]
        )
        assert len(patterns) == 2

    def test_a_broken_routing_module_is_not_swallowed(self):
        """A silently missing socket route is the defect this whole thing closes."""
        with pytest.raises(RuntimeError, match="routing is broken"):
            asgi.collect_websocket_urlpatterns(
                [fake_app("stapel_realtime.tests.broken_module")]
            )

    def test_an_app_that_does_not_exist_is_skipped(self):
        assert (
            asgi.collect_websocket_urlpatterns(
                [fake_app("stapel_realtime.tests.no_such_package")]
            )
            == []
        )


class TestBuildWebsocketApplication:
    def test_it_carries_both_protocols(self):
        async def http_app(scope, receive, send):  # pragma: no cover - never called
            pass

        application = asgi.build_websocket_application(
            patterns=[], http_application=http_app
        )
        assert set(application.application_mapping) == {"http", "websocket"}

    def test_websocket_only_when_no_http_app_is_given(self):
        application = asgi.build_websocket_application(patterns=[])
        assert set(application.application_mapping) == {"websocket"}

    def test_the_stack_is_origin_guard_over_jwt(self):
        application = asgi.build_websocket_application(patterns=[])
        websocket = application.application_mapping["websocket"]
        assert isinstance(websocket, asgi.OriginGuard)
        from stapel_core.django.jwt.channels import JWTAuthMiddleware

        assert isinstance(websocket.inner, JWTAuthMiddleware)

    def test_authentication_can_be_dropped_for_tests_only(self):
        application = asgi.build_websocket_application(patterns=[], authenticate=False)
        from channels.routing import URLRouter

        assert isinstance(
            application.application_mapping["websocket"].inner, URLRouter
        )
