"""
GALP protocol implementation
"""

import logging

from typing import TypeAlias, Iterable
from dataclasses import dataclass

import galp.messages as gm
from galp.lower_protocol import (
        LowerProtocol, Route, IllegalRequestError, Session,
        ForwardingSession,
        LegacyRouteWriter, InvalidMessageDispatcher
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

TransportMessage: TypeAlias = list[bytes]
"""
Type of messages expected by the transport
"""

@dataclass
class UpperSession:
    """
    Session encapsulating a galp message to be sent
    """
    lower_session: Session

    def write(self, message: gm.Message) -> TransportMessage:
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

@dataclass
class UpperForwardingSession:
    """
    Wrapper class for forwarding session that stacks galp serialization
    """
    lower: ForwardingSession

    def forward_from(self, origin: Route | None) -> UpperSession:
        """
        Wrapper around forward_from that wraps the result in a UpperSession
        """
        return UpperSession(self.lower.forward_from(origin))

    @property
    def uid(self):
        """
        Hashable identifier
        """
        return self.lower.uid

@dataclass
class Stack:
    """
    Handling side of a network stack
    """
    upper: 'Protocol'
    lib_upper: 'Protocol' # Only for legacy transport iface
    root: LowerProtocol
    base_session: UpperSession

    def write_local(self, msg: gm.Message) -> TransportMessage:
        """
        Write a locally generated, next-hop addressed, galp message

        This is a temporary interface to get the message writing out of the
        handling stack
        """
        return self.base_session.write(msg)

def make_stack(make_upper_protocol, name, router) -> Stack:
    """
    Factory function to assemble the handler stack

    Returns:
        The result of each app-provided class factory, and the final root of the
        stack to be given to the transport
    """
    # Handlers
    app_upper = make_upper_protocol(name, router)
    lib_upper = Protocol(name, router, app_upper)
    app_lower = lib_upper # Not yet exposed to app
    lib_lower = InvalidMessageDispatcher(
            LowerProtocol(router, app_lower)
            )

    # Writers
    _legacy_route_writer = LegacyRouteWriter(router)
    _lower_base_session = _legacy_route_writer.new_session()
    base_session = UpperSession(_lower_base_session)

    return Stack(app_upper, lib_upper, lib_lower, base_session)

class Protocol:
    """
    Helper class gathering methods solely concerned with parsing and building
    messages, but not with what to do with them.

    Methods named after a verb (`get`) build and send a message.
    Methods starting with `on_` and a verb (`on_get`) are called when such a
    message is received, and should usually be overriden unless the verb is to
    be ignored (with a warning).
    """
    def __init__(self, name, router, upper=None):
        """
        This should eventually be split into a layer object that receives the
        upper layer and a session object that receives the lower session
        """
        # To keep, this is the main way of accessing the application-defined
        # handlers, but this should be a parameter instead of inheritance
        self.upper = upper

        # To keep, for logging
        self.proto_name = name

        # To be removed, only used for legacy RoutedMessage objects
        self.legacy_route_writer = LegacyRouteWriter(router)

        # To be removed, this is used in route_message, which is out of this
        # class' responsibility
        lower_base_session = self.legacy_route_writer.new_session()
        self.base_session = UpperSession(lower_base_session)

    # Default handlers
    # ================

    # To be removed: send methods
    # ===========================

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

    def write_message(self, msg: RoutedMessage | TransportMessage) -> TransportMessage:
        """
        Serialize gm.Message objects, allowing them to be returned directly from
        handlers

        To be removed as this does not belong in the handler class. As a
        transition, allow already written messages and pass them as is
        """
        if isinstance(msg, RoutedMessage):
            # Message still needs to be serialized. Ultimately we want that to
            # be done by handlers within session objects and do nothing here.
            self._log_message(msg, is_incoming=False)
            return self.legacy_route_writer.write_plain_message(self._dump_message(msg))
        if isinstance(msg, list) and msg and isinstance(msg[0], bytes):
            # Message has already been written. Ultimately we want all messages
            # here and then we can drop this function
            return msg
        if isinstance(msg, gm.BaseMessage):
            raise ValueError('Message must be routed (addressed) before being written out')
        raise TypeError(f'Invalid message type {type(msg)}')

    def route_messages(self, orig: UpperForwardingSession, news: Replies
            ) -> list[RoutedMessage | TransportMessage]:
        """
        Route each of an optional list of messages. Legacy, handlers should
        generate new messages through contextual writers and pass only already
        written messages back.
        """
        return [
                orig.forward_from(None).write(new) if isinstance(new, gm.BaseMessage) else new
                for new in self.as_message_list(news)
                ]

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

    def on_message(self, session: ForwardingSession, route: tuple[Route, Route],
            msg_body: list[bytes]) -> Iterable[TransportMessage]:
        """
        Message handler that call's Protocol default handler
        and catches IllegalRequestError
        """
        # Wrap session
        upper_session = UpperForwardingSession(session)

        try:
            return self.on_message_unsafe(upper_session, route, msg_body)
        # Obsolete pathway
        except IllegalRequestError as exc:
            return [upper_session
                    .forward_from(None)
                    .write(gm.Illegal(reason=exc.reason))
                    ]

    def on_message_unsafe(self, session: UpperForwardingSession, route, msg_body: list[bytes]
            ) -> list[TransportMessage]:
        """Parse given message, calling callbacks as needed.

        Returns:
            Whatever the final handler for this message returned.
        """

        # Deserialize the payload
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
                    raise IllegalRequestError('Wrong number of frames')
        except DeserializeError as exc:
            raise IllegalRequestError(f'Bad message: {exc.args[0]}') from exc

        # Build legacy routed message object, to be removed
        incoming, forward = route
        rmsg = RoutedMessage(incoming=incoming, forward=forward, body=msg_obj)

        self._log_message(rmsg, is_incoming=True)

        # We should not need to call write_message here.
        return [
                self.write_message(msg) for msg in
                    self.route_messages(session,
                        self.upper.on_message(session, rmsg))
                    ]


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

@dataclass
class NameDispatcher:
    """
    Dispatches galp messages to methods named after a `on_<type>` template
    """
    def __init__(self, upper):
        self.upper = upper

    def on_message(self, session: Session, msg: RoutedMessage) -> Replies:
        """
        Process a routed message by forwarding the body only to the on_ method
        of matching name
        """
        method = getattr(self.upper, f'on_{msg.body.verb}', None)
        if not method:
            return self._on_unhandled(msg.body)
        return method(msg.body)

    def _on_unhandled(self, msg: gm.Message):
        """
        A message without an overriden callback was received.
        """
        logging.error("Unhandled GALP verb %s", msg.verb)
