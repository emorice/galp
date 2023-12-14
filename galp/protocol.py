"""
GALP protocol implementation
"""

import logging

from typing import TypeAlias, Iterable, TypeVar, Generic, Callable
from dataclasses import dataclass

import galp.messages as gm
from galp.lower_protocol import (
        LowerProtocol, IllegalRequestError, Session,
        ForwardingSession, InvalidMessageDispatcher,
        make_local_session
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
    forward: bool
    body: gm.Message

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

    def write(self, message: gm.BaseMessage) -> TransportMessage:
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

    def forward_from(self, origin: 'UpperForwardingSession | None') -> UpperSession:
        """
        Wrapper around forward_from that wraps the result in a UpperSession
        """
        return UpperSession(self.lower.forward_from(
            origin.lower if origin is not None else None)
            )

    @property
    def uid(self):
        """
        Hashable identifier
        """
        return self.lower.uid

ForwardingHandler: TypeAlias = Callable[
        [UpperForwardingSession, RoutedMessage], list[TransportMessage]
        ]
"""
Handler that has some awareness of forwarding
"""

@dataclass
class Stack:
    """
    Handling side of a network stack
    """
    upper: ForwardingHandler
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

def make_stack(app_handler: ForwardingHandler, name, router) -> Stack:
    """
    Factory function to assemble the handler stack

    Returns:
        The result of each app-provided class factory, and the final root of the
        stack to be given to the transport
    """
    # Handlers
    lib_upper = Protocol(name, app_handler)
    app_lower = lib_upper # Not yet exposed to app
    lib_lower = InvalidMessageDispatcher(
            LowerProtocol(router, app_lower)
            )

    # Writers
    _lower_base_session = make_local_session(router)
    base_session = UpperSession(_lower_base_session)

    return Stack(app_handler, lib_upper, lib_lower, base_session)


class Protocol:
    """
    Helper class gathering methods solely concerned with parsing and building
    messages, but not with what to do with them.

    Methods named after a verb (`get`) build and send a message.
    Methods starting with `on_` and a verb (`on_get`) are called when such a
    message is received, and should usually be overriden unless the verb is to
    be ignored (with a warning).
    """
    def __init__(self, name, upper: ForwardingHandler):
        """
        This should eventually be split into a layer object that receives the
        upper layer and a session object that receives the lower session
        """
        # Reference to application-defined handler
        self.upper = upper

        # For logging
        self.proto_name = name

    #  Recv methods
    # ==================

    def on_message(self, session: ForwardingSession, is_forward: bool, msg_body: list[bytes]
            ) -> Iterable[TransportMessage]:
        """
        Message handler that call's Protocol default handler
        and catches IllegalRequestError
        """
        # Wrap session
        upper_session = UpperForwardingSession(session)

        try:
            return self.on_message_unsafe(upper_session, is_forward, msg_body)
        # Obsolete pathway
        except IllegalRequestError as exc:
            return [upper_session
                    .forward_from(None)
                    .write(gm.Illegal(reason=exc.reason))
                    ]

    def on_message_unsafe(self, session: UpperForwardingSession, is_forward, msg_body: list[bytes]
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
        rmsg = RoutedMessage(forward=is_forward, body=msg_obj)

        self._log_message(rmsg, is_incoming=True)

        # We should not need to call write_message here.
        return self.upper(session, rmsg) or []

    # Logging

    def _log_message(self, msg: RoutedMessage, is_incoming: bool) -> None:

        # Addr is the one expected on any routed communication
        # Extra addr is either an additional forward segment when receiving, or
        # additional source segment when sending, and characterizes a forwarded
        # message
        verb = msg.body.verb.upper()
        match msg.body:
            case gm.Submit() | gm.Found():
                arg = str(msg.body.task_def.name)
            case _:
                arg = getattr(msg.body, 'name', '')

        msg_log_str = (
            f"{self.proto_name +' ' if self.proto_name else ''}"
            f" {verb} {arg}"
            )

        pattern = '<- %s' if is_incoming else '-> %s'

        if msg.forward:
            logging.debug(pattern, msg_log_str)
        else:
            logging.info(pattern, msg_log_str)


M = TypeVar('M', bound=gm.Message)

HandlerFunction: TypeAlias = Callable[[UpperSession, M], list[TransportMessage]]
"""
Type of function that handles a specific message M and generate replies
"""

DispatchFunction: TypeAlias = HandlerFunction[gm.Message]
"""
Type of function that can handle any of several messages
"""

def make_name_dispatcher(upper) -> DispatchFunction:
    """
    Create a handler that dispatches on name
    """
    def on_message(_session, msg: gm.Message) -> list[TransportMessage]:
        """
        Process a routed message by forwarding the body only to the on_ method
        of matching name
        """
        method = getattr(upper, f'on_{msg.verb}', None)
        if not method:
            #logging.error("Unhandled GALP verb %s", msg.verb)
            return []
        return method(msg)
    return on_message

def make_local_handler(dispatch: DispatchFunction) -> ForwardingHandler:
    """
    Wraps a Dispatcher accepting an UpperSession/Message
    into one accepting an UpperForwardingSession/RoutedMessage and discarding
    forwarding information
    """
    def on_message(session: UpperForwardingSession, msg: RoutedMessage
            ) -> list[TransportMessage]:
        """
        Process a routed message by forwarding the body only to the on_ method
        of matching name
        """
        # We do not forwarding, so only generate local messages and discard
        # forwarding information
        upper_session = session.forward_from(None)
        return dispatch(upper_session, msg.body)
    return on_message

@dataclass
class Handler(Generic[M]):
    """
    Wraps a callable message handler while exposing the type of messages
    intended to be handled
    """
    handles: type[M]
    handler: HandlerFunction[M]

def make_type_dispatcher(handlers: Iterable[Handler]) -> DispatchFunction:
    """
    Dispatches a message to a handler based on the type of the message
    """
    _handlers : dict[type, HandlerFunction] = {
        hdl.handles: hdl.handler
        for hdl in handlers
        }
    def on_message(session: UpperSession, msg: gm.BaseMessage
            ) -> list[TransportMessage]:
        """
        Dispatches
        """
        handler = _handlers.get(type(msg))
        if handler:
            return handler(session, msg)
        #logging.error('No handler for %s', msg)
        return []
    return on_message
