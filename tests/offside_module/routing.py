"""A module that mounted its socket outside the canonical /ws/ prefix."""
from django.urls import path

from stapel_realtime.tests.fake_module.routing import FakeConsumer

websocket_urlpatterns = [
    path("sockets/offside/<str:workspace_id>", FakeConsumer.as_asgi()),
]
