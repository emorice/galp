"""
GALP protocol implementation
"""

import logging

from typing import NoReturn, TypeAlias
from dataclasses import dataclass

import galp.messages as gm
from galp.lower_protocol import LowerProtocol, Route, PlainMessage
from galp.serializer import dump_model, load_model

# Errors and exceptions
# =====================
class ProtocolEndException(Exception):
    """
    Exception thrown by a handler to signal that no more messages are expected
    and the transport should be closed
    """

class IllegalRequestError(Exception):
    """Base class for all badly formed requests, triggers sending an ILLEGAL
    message back"""
    def __init__(self, route, reason):
        super().__init__()
        self.route = route
        self.reason = reason

# High-level protocol
# ===================

@dataclass
class RoutedMessage:
    """
    A message with its routes
    """
    incoming: Route
    forward: Route

    body: gm.Message

    def reply(self, new: gm.Message) -> 'RoutedMessage':
        """
        Address a message, extracting and swapping the
        incoming and forward routes from `self`
        """
        return type(self)(incoming=self.forward, forward=self.incoming, body=new)

    @classmethod
    def default(cls, new: gm.Message) -> 'RoutedMessage':
        """
        Address a message by default
        """
        return cls(incoming=Route(), forward=Route(), body=new)


Replies: TypeAlias = gm.Message | RoutedMessage | list[gm.Message | RoutedMessage] | None
"""
Allowed returned type of message handers
"""

class Protocol(LowerProtocol):
    """
    Helper class gathering methods solely concerned with parsing and building
    messages, but not with what to do with them.

    Methods named after a verb (`get`) build and send a message.
    Methods starting with `on_` and a verb (`on_get`) are called when such a
    message is received, and should usually be overriden unless the verb is to
    be ignored (with a warning).
    """

    # Default handlers
    # ================

    def on_unhandled(self, msg: gm.Message):
        """
        A message without an overriden callback was received.
        """
        logging.error("Unhandled GALP verb %s", msg.verb)

    def on_invalid(self, route, reason: str) -> NoReturn:
        """
        An invalid message was received.
        """
        raise IllegalRequestError(route, reason)

    # Send methods
    # ============

    def _dump_message(self, msg: RoutedMessage):
        route = (msg.incoming, msg.forward)
        frames = [
                msg.body.verb.upper().encode('ascii'),
                dump_model(msg.body, exclude={'data'})
                ]
        if hasattr(msg.body, 'data'):
            frames.append(msg.body.data)
        return route, frames

    def write_message(self, msg: RoutedMessage):
        """
        Serialize gm.Message objects, allowing them to be returned directly from
        handlers
        """
        if isinstance(msg, RoutedMessage):
            return super().write_plain_message(self._dump_message(msg))
        if isinstance(msg, gm.Message):
            raise ValueError('Message must be routed (addressed) before being written out')
        raise TypeError(f'Invalid message type {type(msg)}')

    def route_messages(self, orig: RoutedMessage | None, news: Replies
            ) -> list[RoutedMessage]:
        """
        Route each of an optional list of messages
        """
        return [
            self.route_message(orig, new)
            for new in self.as_message_list(news)
            ]

    def route_message(self, orig: RoutedMessage | None,
            new: gm.Message | RoutedMessage) -> RoutedMessage:
        """
        Decide how to address a message

        Default is to set both routes to default objects, ignoring the original
        if any.

        Also accepts already routed messages and forward them as-is ; this
        allows `on_routed_message` overrides to set the route themselves on a
        per-message basis if needed.
        """
        del orig

        if isinstance(new, RoutedMessage):
            return new

        return RoutedMessage(
                incoming=Route(),
                forward=Route(),
                body=new
                )

    #  Recv methods
    # ==================

    @staticmethod
    def as_message_list(messages: Replies
            ) -> list[gm.Message | RoutedMessage]:
        """
        Canonicallize a list of messages
        """
        match messages:
            case None:
                return []
            case gm.Message() | RoutedMessage():
                return [messages]
        return messages

    def on_verb(self, route, msg_body: list[bytes]) -> list[RoutedMessage]:
        """Parse given message, calling callbacks as needed.

        Returns:
            Whatever the final handler for this message returned.
        """
        match msg_body:
            case [_verb, payload]:
                msg_obj, err = load_model(gm.AnyMessage, payload)
            case [_verb, payload, data]:
                msg_obj, err = load_model(gm.AnyMessage, payload, data=data)
            case _:
                self._validate(False, route, 'Wrong number of frames')

        if msg_obj is None:
            self._validate(False, route, err)

        incoming, forward = route
        rmsg = RoutedMessage(incoming=incoming, forward=forward, body=msg_obj)

        return self.route_messages(rmsg, self.on_routed_message(rmsg))

    def on_routed_message(self, msg: RoutedMessage) -> Replies:
        """
        Process a routed message by forwarding the body only to the on_ method
        of matching name
        """
        method_name = f'on_{msg.body.verb}'
        if not hasattr(self, method_name):
            return self.on_unhandled(msg.body)
        return getattr(self, method_name)(msg.body)
