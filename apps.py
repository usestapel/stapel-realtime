"""Django AppConfig — the one reason this L1 library is installable.

By the library standard an L1 package has no models or urls and is simply
imported. This one carries system checks, and a check that is not registered is
a comment. Since 0.2.0 it also carries a two-Function comm surface — the
presence oracle — and an unregistered Function is a door nobody can open. So a
host that actually serves WebSockets adds ``"stapel_realtime"`` to
``INSTALLED_APPS`` and gets both; a host that only imports the envelope or the
stream-key helpers does not have to, and nothing here is touched on an
HTTP-only start (Channels is never imported at ready time — ``functions.py``
reaches the registry and the cache, never a socket).
"""
from django.apps import AppConfig


class RealtimeConfig(AppConfig):
    name = "stapel_realtime"
    label = "realtime"
    verbose_name = "Stapel realtime"

    def ready(self):
        from . import functions  # noqa: F401  — registers the comm surface
        from .checks import register_checks
        from .delivery import register_transport

        register_checks()
        # Register, do not activate: the host still selects the transport with
        # STAPEL_COMM["SIGNAL_TRANSPORT"], whose default stays "none".
        register_transport()
