"""The whole ASGI host — one call, and the routes come from the manifests."""
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "e2e.settings")

import django  # noqa: E402

django.setup()

from django.core.asgi import get_asgi_application  # noqa: E402

from stapel_realtime.asgi import build_websocket_application  # noqa: E402

application = build_websocket_application(http_application=get_asgi_application())
