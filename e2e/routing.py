"""The manifest `build_websocket_application()` discovers for this host."""
from django.urls import path

from .consumers import E2EConsumer

websocket_urlpatterns = [
    path("ws/e2e/<str:workspace_id>", E2EConsumer.as_asgi()),
]
