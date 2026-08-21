"""Django AppConfig — the one reason this L1 library is installable.

By the library standard an L1 package has no models, urls or comm surface and
is simply imported. This one carries system checks, and a check that is not
registered is a comment. So a host that actually serves WebSockets adds
``"stapel_realtime"`` to ``INSTALLED_APPS`` and gets the checks; a host that
only imports the envelope or the stream-key helpers does not have to, and
nothing here is touched on an HTTP-only start (Channels is never imported at
ready time).
"""
from django.apps import AppConfig


class RealtimeConfig(AppConfig):
    name = "stapel_realtime"
    label = "realtime"
    verbose_name = "Stapel realtime"

    def ready(self):
        from .checks import register_checks

        register_checks()
