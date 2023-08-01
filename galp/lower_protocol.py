"""
Implementation of the lower level of GALP, handles routing.
"""

import logging

from typing import NoReturn

Route = list[bytes]
"""
This actually ZMQ specific and should be changed to a generic if we ever need
more protocols
"""

PlainMessage = tuple[tuple[Route, Route], list[bytes]]

class BaseProtocol:
    """
    Abstract class defining the interface expected by the transport
    """
    def write_message(self, msg):
        """
        Takes an application-specific message description, and returns the
        sequence of bytes to send. Return None to suppress the message instead.
        """
        raise NotImplementedError

    def on_message(self, msg_parts):
        """
        Handler called when a message is received.

        Args:
            msg_parts: a list of message parts, each part being a bytes object
        Returns:
            A list of messages that can be iterated and
            given one by one to `write_message`
        """
        raise NotImplementedError

class BaseSplitProtocol(BaseProtocol):
    """
    Abstract class defining how a protocol is split into an Upper and LowerPart
    """
    def on_verb(self, route, msg_body) -> list:
        """
        High-level message handler.

        Args:
            route: a tuple (incoming_route, forwarding route)
            msg_body: all the message parts starting with the galp verb

        Returns:
            A list of messages to send (same as on_message, must be iterable and
            the items compatible with write_message)
        """
        raise NotImplementedError

    def on_invalid(self, route, reason):
        """
        Callback for malformed messages that still contained a well-formed
        route.
        """
        raise NotImplementedError

class LowerProtocol(BaseSplitProtocol):
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


        msg_log_str, meta_log_str = self._log_str(route, msg_body)
        if route[1]:
            # Just forwarding
            logging.debug('<- %s', msg_log_str)
        else:
            logging.info('<- %s', msg_log_str)
        logging.debug('<- %s', meta_log_str)

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
            incoming_route = [next_hop, *incoming_route]
        route_parts = incoming_route + forward_route
        msg_log_str, meta_log_str = self._log_str(
            (incoming_route, forward_route), msg_body)
        if incoming_route:
            # Forwarding only
            logging.debug('-> %s', msg_log_str)
        else:
            logging.info('-> %s', msg_log_str)
        logging.debug('-> %s', meta_log_str)

        return route_parts + [b''] + msg_body

    def default_route(self) -> tuple[Route, Route]:
        """
        Route to pass to send methods to simply send a message a connected
        unnamed peer
        """
        # This is a tuple (incoming, forward) with both routes set to the empty
        # route
        return ([], [])

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

    # Logging utils
    def _log_str(self, route: tuple[Route, Route], msg_body: list[bytes]
            ) -> tuple[str, str]:
        msg_str = (
            f"{self.proto_name +' ' if self.proto_name else ''}"
            f"[{route[0][0].hex() if route[0] else ''}]"
            f" {msg_body[0].decode('ascii') if msg_body else 'PING'}"
            )
        meta_log_str = (
            f"hops {len(route[0] + route[1])}"
            )

        return msg_str, meta_log_str

    def on_verb(self, route, msg_body) -> list:
        """
        Default action, simply log the message
        """
        logging.info("No upper-level handler for verb %s", msg_body[:1])
        return []

    def on_invalid(self, route, reason: str) -> NoReturn:
        """
        Invalid hander. Must be implemented by subclasses to raise a sensible
        exception.
        """
        raise NotImplementedError(f'No handler for invalid message ({reason})')
