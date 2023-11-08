"""
GALP protocol implementation
"""

import logging

from typing import TypeAlias, Iterable
from dataclasses import dataclass

import galp.messages as gm
from galp.lower_protocol import (
        LowerProtocol, Route, IllegalRequestError, Layer, Session
        )
from galp.serializer import dump_model, load_model, DeserializeError

# Errors and exceptions
# =====================

class ProtocolEndException(Exception):
    """
    Exception thrown by a handler to signal that no more messages are expected
    and the transport should be closed
    """

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

Replies: TypeAlias = gm.Message | RoutedMessage | Iterable[gm.Message | RoutedMessage] | None
"""
Allowed returned type of message handers
"""

@dataclass
class UpperSession:
    """
    Session encapsulating a galp message to be sent
    """
    lower_session: Session

    def write(self, message: gm.Message) -> list[bytes]:
        """
        Write a complete message from a galp message object.

        Route is specified through the lower_session attribute.
        """
        frames = [
                dump_model(message, exclude={'data'})
                ]
        if hasattr(message, 'data'):
            frames.append(message.data)
        return self.lower_session.write(frames)

class Protocol(LowerProtocol):
    """
    Helper class gathering methods solely concerned with parsing and building
    messages, but not with what to do with them.

    Methods named after a verb (`get`) build and send a message.
    Methods starting with `on_` and a verb (`on_get`) are called when such a
    message is received, and should usually be overriden unless the verb is to
    be ignored (with a warning).
    """
    def __init__(self, name, router): #, lower_session, upper_layer):
        """
        This should eventually be split into a layer object that receives the
        upper layer and a session object that receives the lower session
        """
        super().__init__(name, router)
        #lower_session = LowerProtocol(name, router)
        self.lower_session = self #lower_session
        self.upper_layer = self #upper_layer

        lower_base_session = self.new_session() # from LowerProtocol. This
        # should be given to the constructor by the stack building code instead.
        self.base_session = UpperSession(lower_base_session)

    # Default handlers
    # ================

    def on_unhandled(self, msg: gm.Message):
        """
        A message without an overriden callback was received.
        """
        logging.error("Unhandled GALP verb %s", msg.verb)

    # Send methods
    # ============

    def _dump_message(self, msg: RoutedMessage):
        """
        To be removed as this does not belong in the handler class.
        """
        route = (msg.incoming, msg.forward)
        frames = [
                dump_model(msg.body, exclude={'data'})
                ]
        if hasattr(msg.body, 'data'):
            frames.append(msg.body.data)
        return route, frames

    def write_message(self, msg: RoutedMessage) -> list[bytes]:
        """
        Serialize gm.Message objects, allowing them to be returned directly from
        handlers

        To be removed as this does not belong in the handler class.
        """
        if isinstance(msg, RoutedMessage):
            # Message still needs to be serialized. Ultimately we want that to
            # be done by handlers within session objects and do nothing here.
            self._log_message(msg, is_incoming=False)
            return self.lower_session.write_plain_message(self._dump_message(msg))
        if isinstance(msg, list) and msg and isinstance(msg[0], bytes):
            # Message has already been written. Ultimately we want all messages
            # here and then we can drop this function
            return msg
        if isinstance(msg, gm.BaseMessage):
            raise ValueError('Message must be routed (addressed) before being written out')
        raise TypeError(f'Invalid message type {type(msg)}')

    def route_messages(self, session: Session, orig: RoutedMessage | None, news: Replies
            ) -> list[RoutedMessage]:
        """
        Route each of an optional list of messages
        """
        return [
            self.route_message(orig, new)
            for new in self.as_message_list(news)
            ]

    def route_message(self, orig: None, new: gm.Message) -> list[bytes]:
        """
        Legacy default-addressing
        """
        assert orig is None, 'Deprecated use'
        assert isinstance(new, gm.BaseMessage), 'Deprecated use'

        return self.base_session.write(new)

    #  Recv methods
    # ==================

    @staticmethod
    def as_message_list(messages: Replies
            ) -> Iterable[gm.Message | RoutedMessage]:
        """
        Canonicallize a list of messages
        """
        match messages:
            case None:
                return []
            case gm.BaseMessage() | RoutedMessage():
                return [messages]
        return messages

    def on_verb(self, session: Session, route, msg_body: list[bytes]
            ) -> list[list[bytes]]:
        """Parse given message, calling callbacks as needed.

        Returns:
            Whatever the final handler for this message returned.
        """

        msg_obj: gm.Message
        try:
            match msg_body:
                case [payload]:
                    # pydantic magic, see
                    # https://github.com/python/mypy/issues/9773 for context
                    # about why it's hard to type this
                    msg_obj = load_model(gm.Message, payload) # type: ignore[arg-type]
                case [payload, data]:
                    msg_obj = load_model(gm.Message, payload, data=data) # type: ignore[arg-type]
                case _:
                    raise IllegalRequestError(route, 'Wrong number of frames')
        except DeserializeError as exc:
            raise IllegalRequestError(route, f'Bad message: {exc.args[0]}') from exc

        incoming, forward = route
        rmsg = RoutedMessage(incoming=incoming, forward=forward, body=msg_obj)

        self._log_message(rmsg, is_incoming=True)

        # We should not need to call write_message here.
        return [
                self.write_message(msg) for msg in
                    self.route_messages(session, rmsg, self.on_routed_message(rmsg))
                    ]

    def on_routed_message(self, msg: RoutedMessage) -> Replies:
        """
        Process a routed message by forwarding the body only to the on_ method
        of matching name
        """
        method_name = f'on_{msg.body.verb}'
        if not hasattr(self, method_name):
            return self.on_unhandled(msg.body)
        return getattr(self, method_name)(msg.body)

    # Logging

    def _log_message(self, msg: RoutedMessage, is_incoming: bool) -> None:

        # Addr is the one expected on any routed communication
        # Extra addr is either an additional forward segment when receiving, or
        # additional source segment when sending, and characterizes a forwarded
        # message
        if is_incoming:
            addr, extra_addr = msg.incoming, msg.forward
        else:
            addr, extra_addr = msg.forward, msg.incoming

        verb = msg.body.verb.upper()
        match msg.body:
            case gm.Submit() | gm.Found():
                arg = str(msg.body.task_def.name)
            case _:
                arg = getattr(msg.body, 'name', '')

        msg_log_str = (
            f"{self.proto_name +' ' if self.proto_name else ''}"
            f"[{addr[0].hex() if addr else ''}]"
            f" {verb} {arg}"
            )
        meta_log_str = f"hops {len(addr + extra_addr)}"

        pattern = '<- %s' if is_incoming else '-> %s'

        if extra_addr:
            logging.debug(pattern, msg_log_str)
        else:
            logging.info(pattern, msg_log_str)
        logging.debug(pattern, meta_log_str)
