"""
GALP protocol implementation
"""

import logging

from typing import TypeAlias, Iterable, Callable
from dataclasses import dataclass

import galp.net.core.types as gm
from galp.net.core.dump import Writer
from galp.net.routing.dump import (make_local_writer, ReplyFromSession,
        ForwardSessions)
from galp.net.routing.load import Routed, load_routed
from galp.writer import TransportMessage
from galp.lower_protocol import (TransportHandler, TransportReturn,
        handle_routing)
from galp.result import Error

# Errors and exceptions
# =====================

class ProtocolEndException(Exception):
    """
    Exception thrown by a handler to signal that no more messages are expected
    and the transport should be closed
    """

# Core-layer handlers
# ===================

def _log_message(routed: Routed, proto_name: str) -> Routed:
    msg = routed.body
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

    return routed

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

def make_forward_stack(app_handler: Callable[
    [ReplyFromSession | ForwardSessions, gm.Message],
    TransportReturn],
    name: str, router: bool = True) -> Stack:
    """
    Factory function to assemble the handler stack

    Returns:
        The result of each app-provided class factory, and the final root of the
        stack to be given to the transport
    """
    def on_message(writer: Writer[TransportMessage], msg: TransportMessage
            ) -> TransportReturn:
        return load_routed(msg).then(
                lambda routed: app_handler(
                    handle_routing(router, writer, _log_message(routed, name)),
                    routed.body
                    )
                )
    return Stack(on_message, make_local_writer(router))

DispatchFunction: TypeAlias = Callable[[Writer[gm.Message], gm.Message],
        Iterable[TransportMessage] | Error]
"""
Type of function that can handle any of several messages, but differs from the
core-layer expected handler by being blind to forwarding
"""

def make_stack(app_handler: DispatchFunction, name: str) -> Stack:
    """Shortcut stack maker for common end-peer stacks"""
    def _on_message(sessions: ReplyFromSession | ForwardSessions, msg: gm.Message):
        match sessions:
            case ReplyFromSession():
                return app_handler(sessions.reply_from(None), msg)
            case ForwardSessions():
                return Error('Unexpected forwarded message')
    return make_forward_stack(_on_message, name, router=False)
