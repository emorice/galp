"""
Implementation of the lower level of GALP, handles routing, priority and
counters.
"""

import logging

from typing import NoReturn, Any

Route = list[bytes]
"""
This actually ZMQ specific and should be changed to a generic if we ever need
more protocols
"""

class MessageList(list):
    """
    Litterally just a list of message objects.

    Allows to distinguish messages from list of messages by type
    """
    @classmethod
    def from_any(cls, messages):
        """
        Wraps messages in a MessageList if not already one

        Also recognizes None, False, etc as empty lists of messages.
        """
        if not messages:
            return cls()
        if isinstance(messages, cls):
            return messages
        message = messages
        return cls([message])

    def __repr__(self):
        return f'MessageList({super().__repr__()})'

    def __add__(self, other):
        new_list = MessageList(self)
        new_list.extend(other)
        return new_list

class BaseProtocol:
    """
    Abstract class defining the interface expected by the transport
    """
    def write_message(self, msg: tuple[Any, list[bytes]]):
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
            A MessageList, a collection of messages that can be iterated and
            given one by one to `write_message`
        """
        raise NotImplementedError

class BaseSplitProtocol(BaseProtocol):
    """
    Abstract class defining how a protocol is split into an Upper and LowerPart
    """
    def on_verb(self, route, msg_body):
        """
        High-level message handler.

        Args:
            route: a tuple (incoming_route, forwarding route)
            msg_body: all the message parts starting with the galp verb

        Returns:
            Either a single message, a MessageList, or any object that evaluates
            to False to mean no messages.
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

    def __init__(self, name, router, capacity=1000):
        """
        Args:
            name: short string to include in log messages.
            router: True if sending function should move a routing id in the
                first routing segment. Default False.
            capacity: number of max messages to ask peers to put in queue.
                Advertized in outgoing messages. Actual compliance depends on
                the peer.
        """
        self.proto_name = name
        self.router = router

        # Internal message counters
        self.capacity = capacity
        ## Index of the next message that will be sent
        self._next_send_idx = 0
        ## Index of the first message that we should block
        self._next_block_idx = 0
        ## Index of the first message that we want the peer to block
        self._next_peer_block_idx = self.capacity

    # Internal constants
    _counter_size = 4
    _counter_endianness = 'big'


    # Main public methods
    # ===================

    def on_message(self, msg_parts):
        """
        Parses the lower part of a GALP message,
        then calls the upper protocol with the parsed message.
        """
        msg_body, counters, route = self._parse_lower(msg_parts)

        sent, block = counters
        self._next_block_idx = block

        # Example: if we last received msg #5 and have a capa of 2, we want the
        # peer to block message #5 + 2 + 1 = #8, but allow #6 and #7.
        # We could have used the last allowed instead of first blocked to
        # simplify, but first blocked can be safely initialized to 0.
        self._next_peer_block_idx = sent + self.capacity + 1


        msg_log_str, meta_log_str = self._log_str(
            route, msg_body,
            sent, block)
        if route[1]:
            # Just forwarding
            logging.debug('<- %s', msg_log_str)
        else:
            logging.info('<- %s', msg_log_str)
        logging.debug('<- %s', meta_log_str)

        # Note: we do not call on_verb for ping, even if it could be treated as
        # an empty verb.
        if msg_body:
            return MessageList.from_any(
                self.on_verb(route, msg_body)
                )

        return MessageList()

    def write_message(self, msg: tuple[tuple[Route, Route], list[bytes]]):
        """
        Concats route and message.
        """
        route, msg_body = msg
        incoming_route, forward_route = route
        if not forward_route:
            # Assuming we want to reply
            incoming_route, forward_route = forward_route, incoming_route
        if self.router:
            # If routing, we need an id, we take it from the forward segment
            next_hop, *forward_route = forward_route
            incoming_route = [next_hop, *incoming_route]
        route_parts = incoming_route + forward_route
        msg_log_str, meta_log_str = self._log_str(
            (incoming_route, forward_route), msg_body,
            self._next_send_idx, self._next_peer_block_idx)
        if incoming_route:
            # Forwarding only
            logging.debug('-> %s', msg_log_str)
        else:
            logging.info('-> %s', msg_log_str)
        logging.debug('-> %s', meta_log_str)

        # Note: this part is synchronous, so we can never build two messages
        # with the same index
        msg_parts = self._build_message(route_parts, msg_body)
        self._next_send_idx += 1

        # Sending is not. So on the other hand a message with a given index
        # could never be actually sent, logically this is the same as dropped.
        return msg_parts

    def ping(self, route):
        """
        Send a ping.
        """
        # Really just an empty message
        return route, []

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

    def _validate(self, condition, route, reason) -> NoReturn:
        """
        Calls invalid message callback. Must always raise and be caught
        """
        if not condition:
            self.on_invalid(route, reason)

    def _parse_lower(self, msg):
        """
        Parses and returns the routing part of `msg`, counters, and body.

        Can be overloaded to handle different routing strategies.
        """
        route_parts = []
        while msg and msg[0]:
            route_parts.append(msg[0])
            msg = msg[1:]
        # Whatever was found is treated as the route. If it's malformed, we
        # cannot know, and we cannot send answers anywhere else.
        route = self._split_route(route_parts)

        # Discard empty frame
        self._validate(msg and not msg[0], route, 'Missing empty delimiter frame')
        msg = msg[1:]

        # Counters
        self._validate(len(msg) >= 2, route, 'Missing message counters')
        b_sent = msg[0]
        b_block = msg[1]
        msg = msg[2:]

        self._validate(len(b_sent) == self._counter_size, route, 'Bad send counter size')
        self._validate(len(b_block) == self._counter_size, route, 'Bad block counter size')
        sent = int.from_bytes(b_sent, self._counter_endianness)
        block = int.from_bytes(b_block, self._counter_endianness)

        return msg, (sent, block), route

    def _split_route(self, route_parts):
        """
        With only one part, it is interpreted as incoming with an empty forward
        """
        incoming_route = route_parts[:1]
        forward_route = route_parts[1:]
        return incoming_route, forward_route

    def _build_message(self, route_parts, msg_body):
        """
        Current value of counter is used, so increment after building.
        """
        send = self._next_send_idx.to_bytes(
            self._counter_size,
            self._counter_endianness)
        peer_block = self._next_peer_block_idx.to_bytes(
            self._counter_size,
            self._counter_endianness)

        return route_parts + [b'', send, peer_block] + msg_body

    # Logging utils
    def _log_str(self, route, msg_body, send, block):
        msg_str = (
            f"{self.proto_name +' ' if self.proto_name else ''}"
            f"[{route[0][0].hex() if route[0] else ''}]"
            f" {msg_body[0].decode('ascii') if msg_body else 'PING'}"
            )
        meta_log_str = (
            f"hops {len(route[0] + route[1])}"
            f" | last sent {send}"
            f" | next block {block}"
            )

        return msg_str, meta_log_str

    def on_verb(self, route, msg_body):
        """
        Default action, simply log the message
        """
        logging.info("No upper-level handler for verb %s", msg_body[:1])

    def on_invalid(self, route, reason: str) -> NoReturn:
        """
        Invalid hander. Must be implemented by subclasses to raise a sensible
        exception.
        """
        raise NotImplementedError(f'No handler for invalid message ({reason})')
