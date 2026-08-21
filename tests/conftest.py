def pytest_configure(config):
    from django.conf import settings
    if not settings.configured:
        settings.configure(
            SECRET_KEY="test-secret-key-not-for-production",
            INSTALLED_APPS=[
                "django.contrib.contenttypes",
                "django.contrib.auth",
                "django.contrib.sessions",
                "stapel_core.django.users",
                "stapel_realtime",
            ],
            AUTH_USER_MODEL="users.User",
            DATABASES={
                "default": {
                    "ENGINE": "django.db.backends.sqlite3",
                    "NAME": ":memory:",
                }
            },
            DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
            USE_TZ=True,
            CACHES={
                "default": {
                    "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                }
            },
            # The in-memory layer is correct for a single-process test run and
            # is exactly what realtime.E001 forbids for a multi-worker host.
            CHANNEL_LAYERS={
                "default": {"BACKEND": "channels.layers.InMemoryChannelLayer"},
            },
            STAPEL_REALTIME={
                # Long enough that no test trips over a heartbeat it did not
                # ask for; the heartbeat tests override it explicitly.
                "HEARTBEAT_S": 3600,
                "ALLOWED_ORIGINS": [],
            },
            STAPEL_BUS_BACKEND="stapel_core.bus.backends.memory.MemoryBus",
            STAPEL_COMM={
                "OUTBOX_ENABLED": False,
                "ACTION_TRANSPORT": "inprocess",
                "VALIDATE_SCHEMAS": True,
            },
            MIGRATION_MODULES={"users": None},
        )
        import django
        django.setup()


import pytest  # noqa: E402


@pytest.fixture
def user(db):
    """A saved, authenticated user — what the G14 middleware puts in scope."""
    from django.contrib.auth import get_user_model

    return get_user_model().objects.create(
        email="watcher@example.com", username="watcher"
    )


@pytest.fixture
def other_user(db):
    from django.contrib.auth import get_user_model

    return get_user_model().objects.create(
        email="other@example.com", username="other"
    )


@pytest.fixture(autouse=True)
def _flush_channel_layer():
    """Groups must not leak between tests — the in-memory layer is global."""
    yield
    from channels.layers import get_channel_layer

    layer = get_channel_layer()
    if layer is not None and hasattr(layer, "flush"):
        from asgiref.sync import async_to_sync

        try:
            async_to_sync(layer.flush)()
        except Exception:
            pass
