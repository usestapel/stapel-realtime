"""A stand-in for a module's routing manifest — what discovery reads."""
from django.urls import path

from stapel_realtime.consumers import EphemeralStreamConsumer


class FakeConsumer(EphemeralStreamConsumer):
    module = "fake"
    scope_type = "ws"
    stream_key_kwarg = "workspace_id"


websocket_urlpatterns = [
    path("ws/fake/<str:workspace_id>", FakeConsumer.as_asgi()),
]
