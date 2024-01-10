"""
Implementation of the lower level of GALP, handles routing.
"""

from typing import TypeAlias, Callable, Iterable, TypeVar

import logging

from galp.writer import TransportMessage, Writer
from galp.lower_sessions import Route, ReplyFromSession, ForwardSessions


# Routing-layer handlers
# ======================

TransportHandler: TypeAlias = Callable[
        [Writer, list[bytes]], Iterable[TransportMessage]
        ]
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

RoutedHandler: TypeAlias = Callable[
        [
            # Session used by upper parser to send back parsing error messages
            ReplyFromSession,
            # Sessions used by app handler to send other responses or forward
            AppSessionT,
            # Payload
            list[bytes]
            ],
        # Messages to be sent in reaction
        Iterable[TransportMessage]]
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

class IllegalRequestError(Exception):
    """Base class for all badly formed requests, should trigger sending an ILLEGAL
    message back"""
    def __init__(self, reason: str):
        super().__init__()
        self.reason = reason

def _handle_illegal(upper: TransportHandler) -> TransportHandler:
    """
    Wraps a handler to catch IllegalRequestError, log, then suppress them.

    At this level, we don't send error messages back.
    """
    def on_message(session: Writer, msg: list[bytes]
            ) -> Iterable[TransportMessage]:
        try:
            return upper(session, msg)
        except IllegalRequestError as exc:
            logging.warning('Supressing malformed incoming message: %s', exc.reason)
            return []
    return on_message

def _parse_lower(msg: list[bytes]) -> tuple[list[bytes], tuple[Route, Route]]:
    """
    Parses and returns the routing part of `msg`, and body.

    Can be overloaded to handle different routing strategies.
    """
    route_parts = []
    while msg and msg[0]:
        route_parts.append(msg[0])
        msg = msg[1:]
    # Whatever was found is treated as the route. If it's malformed, we
    # cannot know, and we cannot send answers anywhere else.
    route = route_parts[:1], route_parts[1:]

    # Discard empty frame
    if not msg or  msg[0]:
        raise IllegalRequestError('Missing empty delimiter frame')
    msg = msg[1:]

    return msg, route

def _handle_routing(is_router: bool, upper: LocalHandler,
        upper_forward: ForwardHandler | None) -> TransportHandler:
    """
    Parses the routing part of a GALP message,
    then calls the upper protocol with the parsed message.

    This creates two sessions: one associated with the original recipient
    (forward session), and one associated with the original sender (reply
    session). Generating the outgoing forwarded message is up to the forward
    handler.

    Args:
        router: True if sending function should move a routing id in the
            first routing segment. Default False.
    """
    def on_message(session: Writer, msg_parts: list[bytes]
                   ) -> Iterable[list[bytes]]:
        payload, routes = _parse_lower(msg_parts)

        incoming_route, forward_route = routes
        is_forward = bool(forward_route)

        reply = ReplyFromSession(session, is_router, incoming_route)
        forward = ReplyFromSession(session, is_router, forward_route)
        both = ForwardSessions(origin=reply, dest=forward)

        if is_forward:
            if upper_forward:
                return upper_forward(reply, both, payload)
            return []

        return upper(reply, reply, payload)
    return on_message

def handle_routing(router: bool,
        upper_local: LocalHandler,
        upper_forward: ForwardHandler | None
        ) -> TransportHandler:
    """
    Stack the routing-layer handlers
    """
    return _handle_illegal(
            _handle_routing(router,
                upper_local, upper_forward
                )
            )
