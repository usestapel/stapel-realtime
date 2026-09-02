"""System checks — each one is a production bruise that no longer needs a human."""
import pytest

from stapel_realtime import checks

REDIS = "channels_redis.core.RedisChannelLayer"
IN_MEMORY = "channels.layers.InMemoryChannelLayer"


def ids(messages):
    return [m.id for m in messages]


class TestChannelLayer:
    def test_no_layer_warns(self, settings):
        settings.CHANNEL_LAYERS = {}
        assert ids(checks.check_channel_layer(None)) == ["realtime.W001"]

    def test_in_memory_with_one_worker_is_fine(self, settings, monkeypatch):
        monkeypatch.delenv("WEB_CONCURRENCY", raising=False)
        settings.CHANNEL_LAYERS = {"default": {"BACKEND": IN_MEMORY}}
        assert checks.check_channel_layer(None) == []

    def test_in_memory_with_two_workers_is_unserviceable(self, settings, monkeypatch):
        """Half the browsers silently receive nothing — an error, not a warning."""
        monkeypatch.setenv("WEB_CONCURRENCY", "2")
        settings.CHANNEL_LAYERS = {"default": {"BACKEND": IN_MEMORY}}
        problems = checks.check_channel_layer(None)
        assert ids(problems) == ["realtime.E001"]
        assert "never reaches sockets held by another" in problems[0].msg

    @pytest.mark.parametrize("var", ["WEB_CONCURRENCY", "UVICORN_WORKERS", "GUNICORN_WORKERS"])
    def test_every_worker_env_var_is_read(self, settings, monkeypatch, var):
        for name in ("WEB_CONCURRENCY", "UVICORN_WORKERS", "GUNICORN_WORKERS"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv(var, "4")
        settings.CHANNEL_LAYERS = {"default": {"BACKEND": IN_MEMORY}}
        assert ids(checks.check_channel_layer(None)) == ["realtime.E001"]

    def test_redis_with_many_workers_is_the_supported_shape(self, settings, monkeypatch):
        monkeypatch.setenv("WEB_CONCURRENCY", "8")
        settings.CHANNEL_LAYERS = {"default": {"BACKEND": REDIS}}
        assert checks.check_channel_layer(None) == []


class TestLayerSocketTimeout:
    def test_a_short_socket_timeout_kills_a_parked_consumer(self, settings):
        settings.CHANNEL_LAYERS = {
            "default": {"BACKEND": REDIS, "CONFIG": {"socket_timeout": 5}}
        }
        problems = checks.check_layer_socket_timeout(None)
        assert ids(problems) == ["realtime.E002"]
        assert "BZPOPMIN" in problems[0].msg

    def test_it_looks_inside_connection_kwargs_too(self, settings):
        settings.CHANNEL_LAYERS = {
            "default": {
                "BACKEND": REDIS,
                "CONFIG": {"connection_kwargs": {"socket_timeout": 5}},
            }
        }
        assert ids(checks.check_layer_socket_timeout(None)) == ["realtime.E002"]

    def test_unset_inherits_the_library_default_and_redis_8_made_it_lethal(
        self, settings, monkeypatch
    ):
        """"Unset" is not a value — it is whatever the installed redis-py
        ships, and 8.0 changed that from "block forever" to FIVE SECONDS
        (redis.asyncio.connection.DEFAULT_SOCKET_TIMEOUT). A check that
        blessed unset was green on a deployment whose every idle consumer
        died mid-BZPOPMIN — the gate was blind to the device it guarded.
        """
        settings.CHANNEL_LAYERS = {"default": {"BACKEND": REDIS, "CONFIG": {}}}
        monkeypatch.setattr(checks, "_redis_library_default_timeout", lambda: 5.0)
        problems = checks.check_layer_socket_timeout(None)
        assert ids(problems) == ["realtime.E002"]
        assert "redis-py" in problems[0].msg

    def test_unset_is_fine_where_the_library_blocks_forever(
        self, settings, monkeypatch
    ):
        """redis-py < 8 (or a future one that reverts): no default timeout,
        nothing to warn about."""
        settings.CHANNEL_LAYERS = {"default": {"BACKEND": REDIS, "CONFIG": {}}}
        monkeypatch.setattr(checks, "_redis_library_default_timeout", lambda: None)
        assert checks.check_layer_socket_timeout(None) == []

    def test_an_explicit_none_disables_the_timeout_and_the_check_agrees(
        self, settings
    ):
        settings.CHANNEL_LAYERS = {
            "default": {"BACKEND": REDIS, "CONFIG": {"socket_timeout": None}}
        }
        assert checks.check_layer_socket_timeout(None) == []

    def test_it_reads_the_hosts_dict_shape_channels_redis_actually_forwards(
        self, settings
    ):
        """`{"hosts": [{"address": …, "socket_timeout": 5}]}` is the form
        channels-redis forwards to ConnectionPool.from_url — the one shape
        an operator setting a per-host timeout actually writes, and the one
        this check could not see."""
        settings.CHANNEL_LAYERS = {
            "default": {
                "BACKEND": REDIS,
                "CONFIG": {
                    "hosts": [{"address": "redis://redis:6379/2", "socket_timeout": 5}]
                },
            }
        }
        assert ids(checks.check_layer_socket_timeout(None)) == ["realtime.E002"]

    def test_a_hosts_dict_explicit_none_is_the_recommended_fix(
        self, settings, monkeypatch
    ):
        """The fleet fix for the redis-8 default: state None per host. The
        check must read that as "blocking restored", not as "unset"."""
        settings.CHANNEL_LAYERS = {
            "default": {
                "BACKEND": REDIS,
                "CONFIG": {
                    "hosts": [
                        {"address": "redis://redis:6379/2", "socket_timeout": None}
                    ]
                },
            }
        }
        monkeypatch.setattr(checks, "_redis_library_default_timeout", lambda: 5.0)
        assert checks.check_layer_socket_timeout(None) == []

    def test_a_timeout_above_the_expiry_floor_passes(self, settings):
        settings.CHANNEL_LAYERS = {
            "default": {"BACKEND": REDIS, "CONFIG": {"expiry": 60, "socket_timeout": 120}}
        }
        assert checks.check_layer_socket_timeout(None) == []

    def test_the_floor_follows_the_layer_expiry(self, settings):
        settings.CHANNEL_LAYERS = {
            "default": {"BACKEND": REDIS, "CONFIG": {"expiry": 300, "socket_timeout": 120}}
        }
        assert ids(checks.check_layer_socket_timeout(None)) == ["realtime.E002"]

    def test_the_floor_is_configurable(self, settings):
        settings.CHANNEL_LAYERS = {
            "default": {"BACKEND": REDIS, "CONFIG": {"socket_timeout": 20}}
        }
        settings.STAPEL_REALTIME = {"LAYER_SOCKET_TIMEOUT_MIN": 10}
        assert checks.check_layer_socket_timeout(None) == []

    def test_a_non_redis_layer_is_none_of_its_business(self, settings):
        settings.CHANNEL_LAYERS = {"default": {"BACKEND": IN_MEMORY}}
        assert checks.check_layer_socket_timeout(None) == []


class TestAllowedOrigins:
    def test_an_empty_allowlist_warns(self, settings):
        settings.STAPEL_REALTIME = {"ALLOWED_ORIGINS": []}
        assert ids(checks.check_allowed_origins(None)) == ["realtime.W002"]

    def test_a_site_registry_is_an_allowlist(self, settings):
        """A fleet that declares its hosts in STAPEL_SITES has a guard —
        W002 must not tell it otherwise."""
        from stapel_core.sites import reset_sites_cache

        settings.STAPEL_REALTIME = {"ALLOWED_ORIGINS": []}
        settings.STAPEL_SITES = {
            "sites": [{"host": "brand-a.example", "primary": True}]
        }
        reset_sites_cache()
        try:
            assert checks.check_allowed_origins(None) == []
        finally:
            reset_sites_cache()

    def test_a_bare_host_is_an_error(self, settings):
        """The studio bug, machine-checked: a guard that never matches."""
        settings.STAPEL_REALTIME = {"ALLOWED_ORIGINS": ["studio.localhost"]}
        problems = checks.check_allowed_origins(None)
        assert ids(problems) == ["realtime.E003"]
        assert "can never match" in problems[0].msg

    def test_full_origins_pass(self, settings):
        settings.STAPEL_REALTIME = {
            "ALLOWED_ORIGINS": ["http://studio.localhost:8600", "https://app.example.com"]
        }
        assert checks.check_allowed_origins(None) == []

    def test_every_bad_entry_is_reported(self, settings):
        settings.STAPEL_REALTIME = {
            "ALLOWED_ORIGINS": ["studio.localhost", "https://ok.example.com", "nope"]
        }
        assert ids(checks.check_allowed_origins(None)) == [
            "realtime.E003",
            "realtime.E003",
        ]


class TestHeartbeat:
    def test_zero_warns_about_both_things_it_disables(self, settings):
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 0}
        problems = checks.check_heartbeat(None)
        assert ids(problems) == ["realtime.W003"]
        assert "exp" in problems[0].msg

    def test_a_positive_interval_is_silent(self, settings):
        settings.STAPEL_REALTIME = {"HEARTBEAT_S": 25}
        assert checks.check_heartbeat(None) == []


class TestRoutePrefix:
    def test_a_canonical_route_is_silent(self, monkeypatch):
        from stapel_realtime.tests.fake_module.routing import websocket_urlpatterns

        monkeypatch.setattr(
            "stapel_realtime.asgi.collect_websocket_urlpatterns",
            lambda: websocket_urlpatterns,
        )
        assert checks.check_route_prefix(None) == []

    def test_an_offside_route_warns(self, monkeypatch):
        from stapel_realtime.tests.offside_module.routing import websocket_urlpatterns

        monkeypatch.setattr(
            "stapel_realtime.asgi.collect_websocket_urlpatterns",
            lambda: websocket_urlpatterns,
        )
        problems = checks.check_route_prefix(None)
        assert ids(problems) == ["realtime.W004"]
        assert "sockets/offside" in problems[0].msg


class TestRegistration:
    def test_every_check_is_registered_by_the_app_config(self):
        """A check that is not registered is a comment."""
        from django.core.checks import registry

        registered = {getattr(c, "__name__", None) for c in registry.registry.get_checks()}
        for check in checks.ALL_CHECKS:
            assert check.__name__ in registered
