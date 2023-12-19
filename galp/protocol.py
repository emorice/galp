"""
GALP protocol implementation
"""

import logging

from typing import TypeAlias, Iterable, TypeVar, Generic, Callable
from dataclasses import dataclass

import galp.messages as gm
from galp.lower_protocol import (IllegalRequestError, LowerSession,
        ForwardingSession, make_local_session, TransportMessage, RoutedHandler,
        TransportHandler, handle_routing)
from galp.serializer import dump_model, load_model, DeserializeError

# Errors and exceptions
# =====================

class ProtocolEndException(Exception):
    """
    Exception thrown by a handler to signal that no more messages are expected
    and the transport should be closed
    """

# Core-layer writers
# ==================

@dataclass
class UpperSession:
    """
    Session encapsulating a galp message to be sent
    """
    lower_session: LowerSession

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

UpperForwardingSession: TypeAlias = ForwardingSession[UpperSession]
"""
A session, resulting from the injection of core serializing logic into the
routing layer, that exposes an interface for control of forwarding, which
results in sessions accepting core galp messages
"""

# Core-layer handlers
# ===================

ForwardingHandler: TypeAlias = Callable[
        [UpperForwardingSession, gm.Message], list[TransportMessage]
        ]
"""
Type of the next-layer ("application") handler that has some awareness of
forwarding and some control over it, in order to accomodate the needs of both
"end peers" (client, worker, pool) and broker.
"""

_UpperRoutedHandler: TypeAlias = Callable[
        [UpperForwardingSession, list[bytes]], list[TransportMessage]
        ]
"""
Internal intermediary hanlder type
"""
def _handle_illegal(upper: _UpperRoutedHandler) -> RoutedHandler:
    """
    Wraps a handler to catch IllegalRequestError and reply with a gm.Illegal
    message. Also wraps the session to accept galp message as replies
    """
    def on_message(session: UpperForwardingSession, msg_body: list[bytes]
            ) -> Iterable[TransportMessage]:
        # Wrap session
        upper_session = session

        try:
            return upper(upper_session, msg_body)
        # Obsolete pathway
        except IllegalRequestError as exc:
            return [upper_session
                    .reply_from(None)
                    .write(gm.Illegal(reason=exc.reason))
                    ]
    return on_message

def _log_message(msg: gm.Message, proto_name: str) -> None:
    # Extra addr is either an additional forward segment when receiving, or
    # additional source segment when sending, and characterizes a forwarded
    # message
    verb = msg.verb.upper()
    match msg:
        case gm.Submit() | gm.Found():
            arg = str(msg.task_def.name)
        case _:
            arg = getattr(msg, 'name', '')

    msg_log_str = (
        f"{proto_name +' ' if proto_name else ''}"
        f" {verb} {arg}"
        )

    pattern = '<- %s' #if is_incoming else '-> %s'

    logging.info(pattern, msg_log_str)

def _add_log_message(proto_name: str, upper: ForwardingHandler) -> ForwardingHandler:
    """
    Insert a logging routine in the handling stack
    """
    def on_message(session: UpperForwardingSession, msg: gm.Message,
            ) -> list[TransportMessage]:
        _log_message(msg, proto_name)
        return upper(session, msg)
    return on_message

def _parse_core_message(upper: ForwardingHandler) -> _UpperRoutedHandler:
    """
    Deserialize the core galp.message in the payload.

    Raises IllegalRequestError on deserialization or validation problems
    """
    def on_message(session: UpperForwardingSession, msg_body: list[bytes]
            ) -> list[TransportMessage]:
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

        return upper(session, msg_obj) or []
    return on_message

def handle_core(upper: ForwardingHandler, proto_name: str) -> RoutedHandler:
    """
    Chains the three parts of the core handlers:
     * Error handling on the outside
     * Parsing the core payload
     * Logging the message between the parsing and the application handler
    """
    return _handle_illegal(
            _parse_core_message(
                _add_log_message(proto_name,
                    upper
                    )
                )
            )

# Stack
# =====
# Utilities to bind layers together

@dataclass
class Stack:
    """
    Handling side of a network stack
    """
    handler: TransportHandler
    base_session: UpperSession

    def write_local(self, msg: gm.Message) -> TransportMessage:
        """
        Write a locally generated, next-hop addressed, galp message

        This is a temporary interface to get the message writing out of the
        handling stack
        """
        return self.base_session.write(msg)

def make_stack(app_handler: ForwardingHandler, name: str, router: bool,
        on_forward: ForwardingHandler | None = None
        ) -> Stack:
    """
    Factory function to assemble the handler stack

    Returns:
        The result of each app-provided class factory, and the final root of the
        stack to be given to the transport
    """
    # Handlers
    core_handler = handle_core(app_handler, name)
    if on_forward:
        forward_core_handler = handle_core(on_forward, name)
    else:
        forward_core_handler = None
    routing_handler = handle_routing(router,
            core_handler, forward_core_handler,
            UpperSession
            )

    # Writers
    _lower_base_session = make_local_session(router)
    base_session = UpperSession(_lower_base_session)

    return Stack(routing_handler, base_session)

# Dispatch-layer handlers
# =======================
# Functions to help applications combine modular handlers into a generic handler
# suitable for the core-layer

M = TypeVar('M', bound=gm.Message)

HandlerFunction: TypeAlias = Callable[[UpperSession, M], list[TransportMessage]]
"""
Type of function that handles a specific message M and generate replies
"""

DispatchFunction: TypeAlias = HandlerFunction[gm.Message]
"""
Type of function that can handle any of several messages, but differs from the
core-layer expected handler by being blind to forwarding
"""

def make_local_handler(dispatch: DispatchFunction) -> ForwardingHandler:
    """
    Wraps a Dispatcher accepting an UpperSession/Message
    into one accepting an UpperForwardingSession/Message and discarding
    forwarding information
    """
    def on_message(session: UpperForwardingSession, msg: gm.Message) -> list[TransportMessage]:
        """
        Process a routed message by forwarding the body only to the on_ method
        of matching name
        """
        # We do not forwarding, so only generate local messages and discard
        # forwarding information
        upper_session = session.reply_from(None)
        return dispatch(upper_session, msg)
    return on_message

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
