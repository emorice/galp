"""
Implementation of the lower level of GALP, handles routing.
"""

from typing import NoReturn

Route = list[bytes]
"""
This actually ZMQ specific and should be changed to a generic if we ever need
more protocols
"""

PlainMessage = tuple[tuple[Route, Route], list[bytes]]

class IllegalRequestError(Exception):
    """Base class for all badly formed requests, should trigger sending an ILLEGAL
    message back"""
    def __init__(self, route: tuple[Route, Route], reason: str):
        super().__init__()
        self.route = route
        self.reason = reason

class LowerProtocol:
    """
    Lower half of a galp protocol handler.

    Handles routing and co, exposes only the pre-split message to the upper
    protocol.
    """

    def __init__(self, name, router):
        """
        Args:
            name: short string to include in log messages.
            router: True if sending function should move a routing id in the
                first routing segment. Default False.
        """
        self.proto_name = name
        self.router = router

    # Main public methods
    # ===================

    def on_message(self, msg_parts: list[bytes]):
        """
        Parses the lower part of a GALP message,
        then calls the upper protocol with the parsed message.
        """
        msg_body, route = self._parse_lower(msg_parts)

        return self.on_verb(route, msg_body)

    def write_plain_message(self, msg: PlainMessage):
        """
        Concats route and message.
        """
        route, msg_body = msg
        incoming_route, forward_route = route
        if not forward_route:
            # We used to allow these and re-interpret the routes, but that was a
            # hack
            assert not incoming_route, 'Message with an origin but no dest'

        if self.router:
            # If routing, we need an id, we take it from the forward segment
            next_hop, *forward_route = forward_route
            route_parts = [next_hop] + incoming_route + forward_route
        else:
            route_parts = incoming_route + forward_route

        return route_parts + [b''] + msg_body

    # Internal parsing utilities
    # ==========================

    def _validate(self, condition, route, reason):
        """
        Calls invalid message callback. Must always raise and be caught if the
        condition fails.
        """
        if not condition:
            self.on_invalid(route, reason)

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
        self._validate(msg and not msg[0], route, 'Missing empty delimiter frame')
        msg = msg[1:]

        return msg, route

    def on_verb(self, route: tuple[Route, Route], msg_body: list[bytes]) -> list:
        """
        Higher level interface to implement by subclassing.
        """
        raise NotImplementedError

    def on_invalid(self, route, reason: str) -> NoReturn:
        """
        An invalid message was received.
        """
        raise IllegalRequestError(route, reason)
