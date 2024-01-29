"""
GALP protocol implementation
"""

import logging

from typing import TypeAlias, Iterable, TypeVar, Generic, Callable
from dataclasses import dataclass

import galp.net.core.types as gm
from galp.net.core.load import parse_core_message, LoadError
from galp.net.core.dump import Writer, make_message_writer
from galp.writer import TransportMessage
from galp.lower_sessions import (make_local_session, ReplyFromSession,
        ForwardSessions)
from galp.lower_protocol import (RoutedHandler,
        AppSessionT, TransportHandler, handle_routing)

# Errors and exceptions
# =====================

class ProtocolEndException(Exception):
    """
    Exception thrown by a handler to signal that no more messages are expected
    and the transport should be closed
    """

# Core-layer handlers
# ===================

ForwardingHandler: TypeAlias = Callable[
        [AppSessionT, gm.Message], list[TransportMessage]
        ]
"""
Type of the next-layer ("application") handler that has some awareness of
forwarding and some control over it, in order to accomodate the needs of both
"end peers" (client, worker, pool) and broker.
"""

def _write_illegal(session: ReplyFromSession, error: LoadError) -> TransportMessage:
    """
    Wraps a LoadError and reply with a gm.Illegal message.
    """
    return session.reply_from(None)(gm.Illegal(reason=error.reason))

def _log_message(msg: gm.Message, proto_name: str) -> None:
    verb = msg.message_get_key()
    match msg:
        case gm.Submit():
            arg = str(msg.task_def.name)
        case _:
            arg = getattr(msg, 'name', '')

    msg_log_str = (
        f"{proto_name +' ' if proto_name else ''}"
        f" {verb!r} {arg}"
        )

    pattern = '<- %s' #if is_incoming else '-> %s'

    logging.info(pattern, msg_log_str)

def handle_core(upper: ForwardingHandler[AppSessionT], proto_name: str
        ) -> RoutedHandler[AppSessionT]:
    """
    Chains the three parts of the core handlers:
     * Parsing the core payload
     * Parse error handling
     * Logging the message between the parsing and the application handler
    """
    def on_message(session: ReplyFromSession, next_session: AppSessionT, msg: list[bytes]
                   ) -> Iterable[TransportMessage]:
        msg_obj = parse_core_message(msg)
        if isinstance(msg_obj, LoadError):
            return [_write_illegal(session, msg_obj)]
        _log_message(msg_obj, proto_name)
        return upper(next_session, msg_obj)
    return on_message

# Stack
# =====
# Utilities to bind layers together

@dataclass
class Stack:
    """
    Handling side of a network stack
    """
    handler: TransportHandler
    write_local: Writer[gm.Message]

def make_stack(app_handler: ForwardingHandler[ReplyFromSession], name: str, router: bool,
        on_forward: ForwardingHandler[ForwardSessions] | None = None
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
    routing_handler = handle_routing(router, core_handler, forward_core_handler)

    # Writers
    _lower_base_session = make_local_session(router)
    base_writer = make_message_writer(_lower_base_session.write)

    return Stack(routing_handler, base_writer)

# Dispatch-layer handlers
# =======================
# Functions to help applications combine modular handlers into a generic handler
# suitable for the core-layer

M = TypeVar('M', bound=gm.Message)

HandlerFunction: TypeAlias = Callable[[Writer[gm.Message], M], list[TransportMessage]]
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
    into one accepting an ReplyFromSession/Message and discarding
    forwarding information
    """
    def on_message(session: ReplyFromSession, msg: gm.Message) -> list[TransportMessage]:
        """
        Process a routed message by forwarding the body only to the on_ method
        of matching name
        """
        # We do not forwarding, so only generate local messages and discard
        # forwarding information
        return dispatch(session.reply_from(None), msg)
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
        method = getattr(upper, f'on_{msg.message_get_key().decode("ascii")}', None)
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
    def on_message(write: Writer[gm.Message], msg: gm.Message
            ) -> list[TransportMessage]:
        """
        Dispatches
        """
        handler = _handlers.get(type(msg))
        if handler:
            return handler(write, msg)
        #logging.error('No handler for %s', msg)
        return []
    return on_message
