"""The e2e host's one consumer — an ephemeral stream anybody signed in may watch.

The capability authorizer needs a live stapel-workspaces peer, which this host
does not run; the point of the e2e is the transport, not the predicate. The
hook is still explicit — which is the property being demonstrated: nothing is
open by default, a host has to say so.
"""
from stapel_realtime.consumers import EphemeralStreamConsumer


async def any_signed_in_user(scope, stream_key):
    user = scope.get("user")
    return bool(user is not None and getattr(user, "is_authenticated", False))


class E2EConsumer(EphemeralStreamConsumer):
    module = "e2e"
    scope_type = "ws"
    stream_key_kwarg = "workspace_id"
    authorizer = staticmethod(any_signed_in_user)
