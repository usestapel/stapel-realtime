"""E2E host settings — a real ASGI host serving real WebSockets over redis.

Not shipped in the wheel (the setuptools packages list is explicit). This is
the host ``e2e/run_e2e.py`` boots TWICE, as two independent worker processes
sharing one redis channel layer, to prove the property no unit test can:
a signal emitted in one worker reaches a socket held by another.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

SECRET_KEY = "e2e-only-not-a-secret"
DEBUG = False
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "stapel_core.django.users",
    "stapel_realtime",
    "e2e",
]

AUTH_USER_MODEL = "users.User"

MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "e2e.urls"
TEMPLATES = []

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": os.environ.get("REALTIME_E2E_DB", "/tmp/stapel-realtime-e2e/db.sqlite3"),
        "OPTIONS": {"timeout": 20},
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True

CACHES = {"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}

REDIS_URL = os.environ.get("REALTIME_E2E_REDIS", "redis://127.0.0.1:6399/0")

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [REDIS_URL],
            # socket_timeout deliberately left unset — realtime.E002 explains
            # why redis-py 8's 5s default kills a consumer parked in BZPOPMIN.
        },
    }
}

# The seam under test: comm.signal() resolves this name to the transport
# stapel_realtime registered from its AppConfig.ready().
STAPEL_COMM = {
    "SIGNAL_TRANSPORT": "channels",
    "OUTBOX_ENABLED": False,
    "ACTION_TRANSPORT": "inprocess",
}

STAPEL_REALTIME = {
    "HEARTBEAT_S": 30,
    "ALLOWED_ORIGINS": ["http://127.0.0.1:8771", "http://127.0.0.1:8772"],
    "SEND_QUEUE_SIZE": 500,
}

# The JWT stack the sockets authenticate with — the same one HTTP uses.
JWT_SECRET_KEY = "e2e-only-jwt-secret"
JWT_ALGORITHM = "HS256"

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": "WARNING"},
}
