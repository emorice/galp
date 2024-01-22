"""
GALP protocol implementation
"""

import logging

from typing import TypeAlias, Iterable, TypeVar, Generic, Callable, Protocol, Any
from dataclasses import dataclass

import galp.messages as gm
from galp.writer import TransportMessage
from galp.lower_sessions import (make_local_session, ReplyFromSession,
        ForwardSessions)
from galp.lower_protocol import (IllegalRequestError, RoutedHandler,
        AppSessionT, TransportHandler, handle_routing)
from galp.serializer import load_model, DeserializeError
from galp.upper_session import UpperSession

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

def _handle_illegal(session: ReplyFromSession,
                    upper: Callable[[], Iterable[TransportMessage]]
                    ) -> Iterable[TransportMessage]:
    """
    Wraps a handler to catch IllegalRequestError and reply with a gm.Illegal
    message.
    """
    try:
        return upper()
    except IllegalRequestError as exc:
        logging.exception('Bad request')
        return [session
                .reply_from(None)
                .write(gm.Illegal(reason=exc.reason))
                ]

def _log_message(msg: gm.Message, proto_name: str) -> None:
    verb = msg.message_get_key()
    match msg:
        case gm.Submit() | gm.Found():
            arg = str(msg.task_def.name)
        case _:
            arg = getattr(msg, 'name', '')

    msg_log_str = (
        f"{proto_name +' ' if proto_name else ''}"
        f" {verb!r} {arg}"
        )

    pattern = '<- %s' #if is_incoming else '-> %s'

    logging.info(pattern, msg_log_str)

T = TypeVar('T')

class DefaultLoader(Protocol): # pylint: disable=too-few-public-methods
    """Generic callable signature for a universal object loader"""
    def __call__(self, cls: type[T]) -> Callable[[list[bytes]], T]: ...

class Loaders:
    """
    Thin wrapper around a dictionary that maps types to a method to load them
    from buffers. This class is used because even if the functionality is a
    trivial defaultdict, the type signature is quite more elaborate.
    """
    def __init__(self, default_loader: DefaultLoader):
        self._loaders: dict[type, Callable[[list[bytes]], Any]] = {}
        self._default_loader = default_loader

    def __setitem__(self, cls: type[T], loader: Callable[[list[bytes]], T]
            ) -> None:
        self._loaders[cls] = loader

    def __getitem__(self, cls: type[T]) -> Callable[[list[bytes]], T]:
        loader = self._loaders.get(cls)
        if loader is None:
            return self._default_loader(cls)
        return loader

def _default_loader(cls: type[T]) -> Callable[[list[bytes]], T]:
    """
    Fallback to loading a message consisting of a single frame by using the
    general pydantic validating constructor for e.g. dataclasses
    """
    def _load(frames: list[bytes]) -> T:
        match frames:
            case [frame]:
                return load_model(cls, frame)
            case _:
                raise IllegalRequestError('Wrong number of frames')
    return _load

def _reply_value_loader(frames: list[bytes]) -> gm.BaseReplyValue:
    match frames:
        case [key, *value_frames]:
            vtype = gm.BaseReplyValue.message_get_type(key)
            if not vtype:
                raise IllegalRequestError(f'Bad message: {vtype!r}')
            return _default_loader(vtype)(value_frames)
        case _:
            raise IllegalRequestError('Wrong number of frames')

def _reply_loader(frames: list[bytes]) -> gm.Reply:
    """
    Constructs a Reply
    """
    match frames:
        case [core_frame, *value_frames]:
            return gm.Reply(
                request=load_model(gm.RequestId, core_frame),
                value=_reply_value_loader(value_frames)
                )
        case _:
            raise IllegalRequestError('Wrong number of frames')

_message_loader = Loaders(_default_loader)
_message_loader[gm.Reply] = _reply_loader

def parse_core_message(msg_body: list[bytes]) -> gm.Message:
    """
    Deserialize the core galp.message in the payload.

    Raises IllegalRequestError on deserialization or validation problems
    """
    match msg_body:
        case [verb, *frames]:
            cls = gm.Message.message_get_type(verb)
            if cls is None:
                raise IllegalRequestError(f'Bad message: {verb!r}')
            try:
                return _message_loader[cls](frames)
            except DeserializeError as exc:
                raise IllegalRequestError(f'Bad message: {exc.args[0]}') from exc
        case _:
            raise IllegalRequestError('Wrong number of frames')

def handle_core(upper: ForwardingHandler[AppSessionT], proto_name: str
        ) -> RoutedHandler[AppSessionT]:
    """
    Chains the three parts of the core handlers:
     * Error handling on the outside
     * Parsing the core payload
     * Logging the message between the parsing and the application handler
    """
    def on_message(session: ReplyFromSession, next_session: AppSessionT, msg: list[bytes]
                   ) -> Iterable[TransportMessage]:
        def _unsafe_handle():
            msg_obj = parse_core_message(msg)
            _log_message(msg_obj, proto_name)
            return upper(next_session, msg_obj)
        return _handle_illegal(session, _unsafe_handle)
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
    base_session: UpperSession

    def write_local(self, msg: gm.Message) -> TransportMessage:
        """
        Write a locally generated, next-hop addressed, galp message

        This is a temporary interface to get the message writing out of the
        handling stack
        """
        return self.base_session.write(msg)

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
    base_session = UpperSession(_lower_base_session.write)

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
    def on_message(session: UpperSession, msg: gm.Message
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
