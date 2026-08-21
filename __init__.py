"""stapel-realtime — the delivery substrate for the Signal primitive.

Three primitives address code: **Function** ("answer me now"), **Action**
("this happened, the system must know" — outbox, at-least-once), **Task**
("do the long work"). **Signal** is the fourth, and its addressee is a human
looking at a screen: *show this to whoever is watching right now*. There is no
obligation to an observer who is not watching — when they look, they read
current state over REST. Losing a signal is correct behaviour, and that is the
property that lets this library be simple.

``stapel_core.comm.signal()`` is the emitter — free, no-op without a backend,
importable by all 26 libraries. **This package is the delivery half**: the
Channels/Redis transport, the two consumers every browser socket is built
from, the wire envelope, the fail-closed authorize seam, revoke-to-kick, the
close-code canon, the host assembly helper, and the system checks that turn
today's realtime bruises into machine verdicts.

The boundary that keeps a fifth implementation from appearing (spec §2.3):

    If a human in a browser is on the other end of the socket, it is
    stapel-realtime. If it is one of our own processes, it is an
    application-level protocol (stapel-runner-protocol) and it owes an
    answer to "why not a Function or a Task".

Public API (lazily exported, PEP 562 — importing this package never pulls in
Django or Channels):

- ``realtime_settings`` — resolved app settings.
- ``deliver`` / ``deliver_frame`` / ``revoke`` / ``signal_on_commit`` /
  ``ChannelsSignalTransport`` — the delivery seam.
- ``EphemeralStreamConsumer`` / ``ResumableStreamConsumer`` / ``JournalRow``
  — the consumers (need the ``channels`` extra).
- ``WorkspaceCapability`` — the canonical authorizer for ``ws``-scoped streams.
- ``build_websocket_application`` / ``collect_websocket_urlpatterns`` — host
  assembly.
- ``build_stream_key`` / ``parse_stream_key`` / ``workspace_stream`` /
  ``group_name`` — the stream-key canon.
- ``frame`` / ``parse_frame`` / ``WIRE_VERSION`` — the wire envelope.
"""

__all__ = [
    "realtime_settings",
    # delivery
    "ChannelsSignalTransport",
    "deliver",
    "deliver_frame",
    "revoke",
    "signal_on_commit",
    # consumers
    "BaseStreamConsumer",
    "EphemeralStreamConsumer",
    "ResumableStreamConsumer",
    "JournalRow",
    # authorization
    "WorkspaceCapability",
    # host assembly
    "build_websocket_application",
    "collect_websocket_urlpatterns",
    # stream keys
    "StreamKey",
    "InvalidStreamKey",
    "build_stream_key",
    "parse_stream_key",
    "workspace_stream",
    "group_name",
    # wire
    "WIRE_VERSION",
    "Frame",
    "InvalidEnvelope",
    "frame",
    "parse_frame",
]

# name -> submodule that defines it. Resolution is deferred until first
# attribute access so that `import stapel_realtime` stays Django-free — and,
# just as important, Channels-free: a module that only builds stream keys or
# emits signals must not be forced to install the transport.
_LAZY_EXPORTS = {
    "realtime_settings": ".conf",
    "ChannelsSignalTransport": ".delivery",
    "deliver": ".delivery",
    "deliver_frame": ".delivery",
    "revoke": ".delivery",
    "signal_on_commit": ".delivery",
    "BaseStreamConsumer": ".consumers",
    "EphemeralStreamConsumer": ".consumers",
    "ResumableStreamConsumer": ".consumers",
    "JournalRow": ".consumers",
    "WorkspaceCapability": ".authorize",
    "build_websocket_application": ".asgi",
    "collect_websocket_urlpatterns": ".asgi",
    "StreamKey": ".streams",
    "InvalidStreamKey": ".streams",
    "build_stream_key": ".streams",
    "parse_stream_key": ".streams",
    "workspace_stream": ".streams",
    "group_name": ".streams",
    "WIRE_VERSION": ".envelope",
    "Frame": ".envelope",
    "InvalidEnvelope": ".envelope",
    "frame": ".envelope",
    "parse_frame": ".envelope",
}


def __getattr__(name):
    if name in _LAZY_EXPORTS:
        from importlib import import_module

        value = getattr(import_module(_LAZY_EXPORTS[name], __name__), name)
        globals()[name] = value  # cache for subsequent lookups
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(__all__))
