"""
Implementation of the lower level of GALP, handles routing.
"""

from typing import TypeAlias, Callable, Iterable

from galp.writer import TransportMessage
from galp.net.routing.load import Routed
from galp.net.routing.dump import ReplyFromSession, ForwardSessions, Writer
from galp.result import Error


# Routing-layer handlers
# ======================

TransportReturn: TypeAlias = Iterable[TransportMessage] | Error
TransportHandler: TypeAlias = Callable[
        [Writer[list[bytes]], list[bytes]], TransportReturn]
"""
Type of handler implemented by this layer and intended to be passed to the
transport
"""

def handle_routing(is_router: bool, session: Writer[list[bytes]], msg: Routed
        ) -> ReplyFromSession | ForwardSessions:
    """
    Creates one or two sessions: one associated with the original recipient
    (forward session), and one associated with the original sender (reply
    session).

    Args:
        is_router: True if sending function should move a routing id in the
            first routing segment.
    """

    reply = ReplyFromSession(session, is_router, msg.incoming)

    if msg.forward:
        forward = ReplyFromSession(session, is_router, msg.forward)
        return ForwardSessions(origin=reply, dest=forward)

    return reply
