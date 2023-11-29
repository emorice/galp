"""
Implementation of the lower level of GALP, handles routing.
"""

from dataclasses import dataclass

import logging

Route = list[bytes]
"""
This actually ZMQ specific and should be changed to a generic if we ever need
more protocols
"""

@dataclass
class Routes:
    """
    The routing layer of a message
    """
    incoming: Route
    forward: Route

PlainMessage = tuple[tuple[Route, Route], list[bytes]]

class IllegalRequestError(Exception):
    """Base class for all badly formed requests, should trigger sending an ILLEGAL
    message back"""
    def __init__(self, reason: str):
        super().__init__()
        self.reason = reason

@dataclass
class Session:
    """
    Object encapsulating information received in previous messages along with
    the callbacks necessary to generate new messages from this information.

    Typically, this is is used to implement request-reply patterns,
    authentication, or any scheme when a peer has to remember some information
    from the last received message to generate the next one.
    """
    lower_session: 'Session | None'

    def write_message(self) -> list[bytes]:
        """
        Serializes this layer's session's message
        """
        return []

    def write_layer(self, payload: list[bytes]) -> list[bytes]:
        """
        Adds this layer's information to the payload generated by the upper
        layer. Default is to pass the payload unmodified.
        """
        return self.write_message() + payload

    def write(self, payload: list[bytes]) -> list[bytes]:
        """
        Adds this layer and the ones below's information to the payload generated by the upper
        layer.
        """
        current = self.write_layer(payload)
        if self.lower_session:
            return self.lower_session.write(current)
        return current

class InvalidMessageDispatcher:
    """
    Mixin class to provide error handling around `on_message`

    The only effective function is to provide a way to abort handling in
    the message handler to let the invalid handler take the relay.

    The default invalid handler logs the error and suppresses the message
    without attempting to generate any kind of message back.
    """
    def __init__(self, upper):
        self.upper = upper

    def on_message(self, session: Session, msg_parts: list[bytes]
            ) -> list[list[bytes]]:
        """
        Public handler for messages, including invalid message handling.
        """
        try:
            return self.upper.on_message(session, msg_parts)
        except IllegalRequestError as exc:
            return self.upper.on_invalid(session, exc)

@dataclass
class LowerSession(Session):
    """
    Keeps track of routing information of a peer and origin to address messages
    """
    is_router: bool
    routes: Routes

    def write_message(self) -> list[bytes]:
        """
        Dumps route as frames
        """
        if self.is_router:
            # If routing, we need an id, we take it from the forward segment
            # Note: this id is consumed by the zmq router sending stage and does
            # not end up on the wire
            next_hop, *forward_route = self.routes.forward
            return [next_hop, *self.routes.incoming, *forward_route, b'']
        return [*self.routes.incoming, *self.routes.forward, b'']

@dataclass
class ForwardingSession:
    """
    Session encapsulating a destination but letting the user fill in the origin
    part of the message
    """
    lower: Session
    is_router: bool
    forward: Route

    def forward_from(self, origin: 'ForwardingSession | None'):
        """
        Creates a session to send galp messages

        Args:
            origin: where the message originates from and where replies should
            be sent. If None, means the message was locally generated.
        """
        nat_origin = Route() if origin is None else origin.forward
        return LowerSession(self.lower, self.is_router,
                Routes(incoming=nat_origin, forward=self.forward)
                )

    @property
    def uid(self):
        """
        Hashable identifier for this destination
        """
        return tuple(self.forward)

class LowerProtocol:
    """
    Lower half of a galp protocol handler.

    Handles routing and co, exposes only the pre-split message to the upper
    protocol.
    """

    def __init__(self, router: bool, upper):
        """
        Args:
            name: short string to include in log messages.
            router: True if sending function should move a routing id in the
                first routing segment. Default False.
        """
        self.router = router
        self.upper = upper

    # Main public methods
    # ===================

    def on_message(self, session: Session, msg_parts: list[bytes]
                   ) -> list[list[bytes]]:
        """
        Parses the routing part of a GALP message,
        then calls the upper protocol with the parsed message.

        This creates two sessions: one associated with the original recipient
        (forward session), and one associated with the original sender (reply
        session). By default, the forward session is used immediately to forward
        the incoming message, and only the reply session is exposed to the layer
        above. Messages are automatically forwarded without further action from
        the higher handlers ; however, interrupting the handler with, say, a
        validation error would cause the forwarded message to be dropped too.
        """
        payload, routes = self._parse_lower(msg_parts)

        incoming_route, forward_route = routes

        forward = LowerSession(session, self.router,
                               Routes(incoming_route, forward_route)
                               )
        reply = ForwardingSession(session, self.router, incoming_route)
        out = []

        if forward_route:
            out.append(forward.write(payload))

        out.extend(self.upper.on_message(reply, routes, payload))

        return out

    def on_invalid(self, session: Session, exc: IllegalRequestError) -> list[list[bytes]]:
        """
        Handler for invalid messages
        """
        _ = session
        logging.warning('Supressing malformed incoming message: %s', exc.reason)
        return []


    # Internal parsing utilities
    # ==========================

    def _parse_lower(self, msg: list[bytes]) -> tuple[list[bytes], tuple[Route, Route]]:
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

def make_local_session(is_router: bool) -> Session:
    """
    Create a default-addressing session
    """
    return LowerSession(None, is_router, Routes(incoming=Route(), forward=Route()))
