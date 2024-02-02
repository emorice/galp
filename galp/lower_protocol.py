"""
Implementation of the lower level of GALP, handles routing.
"""

from typing import TypeAlias, Callable, Iterable, TypeVar

from galp.writer import TransportMessage
from galp.net.routing.load import load_routes, Routes
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

AppSessionT = TypeVar('AppSessionT')
"""
Generic type of the session, or collection thereof, passed to the final app
handler. In practice this type is almost always known, but a few bits of code
benefit from treating it as a generic.
"""

RoutedHandler: TypeAlias = Callable[[AppSessionT, list[bytes]],
        TransportReturn]
"""
Type of the next-layer ("routed" layer, once the "routing" is parsed) handler to
be injected. This is also generic in the type of session passed to the app
handler.
"""

LocalHandler: TypeAlias = RoutedHandler[ReplyFromSession]
"""
More specific type of next-layer handler for the local case.
"""

ForwardHandler: TypeAlias = RoutedHandler[ForwardSessions]
"""
More specific type of next-layer handler for the forward case.
"""

def _handle_routing(is_router: bool, upper: LocalHandler,
    upper_forward: ForwardHandler | None, session: Writer[list[bytes]],
    msg: tuple[Routes, list[bytes]]) -> TransportReturn:
    """
    Calls the upper protocol with the parsed message.

    This creates two sessions: one associated with the original recipient
    (forward session), and one associated with the original sender (reply
    session). Generating the outgoing forwarded message is up to the forward
    handler.

    Args:
        router: True if sending function should move a routing id in the
            first routing segment.
    """
    routes, payload = msg
    is_forward = bool(routes.forward)

    reply = ReplyFromSession(session, is_router, routes.incoming)
    forward = ReplyFromSession(session, is_router, routes.forward)
    both = ForwardSessions(origin=reply, dest=forward)

    if is_forward:
        if upper_forward:
            return upper_forward(both, payload)
        return []

    return upper(reply, payload)

def handle_routing(router: bool,
        upper_local: LocalHandler,
        upper_forward: ForwardHandler | None
        ) -> TransportHandler:
    """Stack the routing-layer handlers"""
    def on_message(session: Writer[list[bytes]], msg_parts: list[bytes]
                   ) -> TransportReturn:
        return load_routes(msg_parts).then(
                lambda r: _handle_routing(router,
                            upper_local, upper_forward,
                            session, r)
                )
    return on_message
