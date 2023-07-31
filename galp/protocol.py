"""
GALP protocol implementation
"""

import logging

from typing import NoReturn, TypeVar

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

    def _dump_message(self, msg: gm.Message):
        route = (msg.incoming, msg.forward)
        frames = [
                msg.verb.upper().encode('ascii'),
                dump_model(msg, exclude={'incoming', 'forward', 'data'})
                ]
        if hasattr(msg, 'data'):
            frames.append(msg.data)
        return route, frames

    def write_message(self, msg: PlainMessage | gm.Message):
        """
        Serialize gm.Message objects, allowing them to be returned directly from
        handlers
        """
        if isinstance(msg, gm.Message):
            plain = self._dump_message(msg)
        else:
            plain = msg
        return super().write_message(plain)

    #  Recv methods
    # ==================

    def on_verb(self, route, msg_body: list[bytes]):
        """Parse given message, calling callbacks as needed.

        Returns:
            Whatever the final handler for this message returned.
        """
        gmsg : gm.Message = self._load_message(gm.AnyMessage, route, msg_body)
        method_name = f'on_{gmsg.verb}'
        if not hasattr(self, method_name):
            return self.on_unhandled(gmsg)
        return getattr(self, method_name)(gmsg)

    T = TypeVar('T', bound=gm.Message)
    def _load_message(self, message_type: type[T],
            route: Route, msg: list[bytes]) -> T:

        routes = {'incoming': route[0], 'forward': route[1]}

        # This passes or not a `data=` parameter to model validation, which will
        # only pass if the presence or absence of the last frame matches the
        # presence or absence of a `data: bytes` field on the class.
        # Gotchas:
        #  * Because of pydantic defaults, this means that an unexpected third
        #  frame will be simply discarded.
        #  * Conversely, a missing third frame will produce an arguably cryptic
        #  missing field `data` error

        match msg:
            case [_verb, payload]:
                msg_obj, err = load_model(message_type, payload, **routes)
            case [_verb, payload, data]:
                msg_obj, err = load_model(message_type, payload, data=data, **routes)
            case _:
                self._validate(False, route, 'Wrong number of frames')

        if msg_obj is None:
            self._validate(False, route, err)

        return msg_obj
