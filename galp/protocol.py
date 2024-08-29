"""
GALP protocol implementation
"""

import logging

from typing import TypeAlias, Iterable, Callable

import galp.net.core.types as gm
from galp.net.core.dump import Writer, MessageTypeMap
from galp.net.routing.dump import ReplyFromSession, ForwardSessions
from galp.net.routing.dump import write_local # pylint: disable=unused-import
from galp.net.routing.load import Routed, load_routed
from galp.writer import TransportMessage
from galp.result import Error, Result

# Routing-layer handlers
# ======================

TransportReturn: TypeAlias = Iterable[TransportMessage] | Result[object]
TransportHandler: TypeAlias = Callable[
        [list[bytes]], TransportReturn]
"""
Type of handler implemented by this layer and intended to be passed to the
transport
"""

def _handle_routing(is_router: bool, msg: Routed) -> ReplyFromSession | ForwardSessions:
    """
    Creates one or two sessions: one associated with the original recipient
    (forward session), and one associated with the original sender (reply
    session).

    Args:
        is_router: True if sending function should move a routing id in the
            first routing segment.
    """

    reply = ReplyFromSession(is_router, msg.incoming)

    if msg.forward:
        forward = ReplyFromSession(is_router, msg.forward)
        return ForwardSessions(origin=reply, dest=forward)

    return reply

# Core-layer handlers
# ===================

def _log_message(routed: Routed, proto_name: str) -> Routed:
    msg = routed.body
    verb, _dumper = MessageTypeMap.get_key(msg)
    match msg:
        case gm.Submit() | gm.Upload():
            arg = str(msg.task_def.name)
        case _:
            arg = getattr(msg, 'name', '')

    msg_log_str = (
        f"{proto_name +' ' if proto_name else ''}"
        f" {verb!r} {arg}"
        )

    pattern = '<- %s' #if is_incoming else '-> %s'

    logging.debug(pattern, msg_log_str)

    return routed

# Stack
# =====
# Utilities to bind layers together

def make_forward_handler(app_handler: Callable[
    [ReplyFromSession | ForwardSessions, gm.Message],
    TransportReturn],
    name: str, router: bool = True) -> TransportHandler:
    """
    Factory function to assemble the handler stack

    Returns:
        The result of each app-provided class factory, and the final root of the
        stack to be given to the transport
    """
    def on_message(msg: TransportMessage) -> TransportReturn:
        return load_routed(msg).then(
                lambda routed: app_handler(
                    _handle_routing(router, _log_message(routed, name)),
                    routed.body
                    )
                )
    return on_message

DispatchFunction: TypeAlias = Callable[[Writer[gm.Message], gm.Message],
        TransportReturn]
"""
Type of function that can handle any of several messages, but differs from the
core-layer expected handler by being blind to forwarding
"""

def make_transport_handler(app_handler: DispatchFunction, name: str) -> TransportHandler:
    """Shortcut stack maker for common end-peer stacks"""
    def _on_message(sessions: ReplyFromSession | ForwardSessions, msg: gm.Message):
        match sessions:
            case ReplyFromSession():
                return app_handler(sessions.reply_from(None), msg)
            case ForwardSessions():
                return Error('Unexpected forwarded message')
    return make_forward_handler(_on_message, name, router=False)
