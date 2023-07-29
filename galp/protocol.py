"""
GALP protocol implementation
"""

import logging

from typing import NoReturn, TypeVar

import galp.messages as gm
from galp.lower_protocol import LowerProtocol, Route, PlainMessage
from galp.eventnamespace import EventNamespace, NoHandlerError
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
    # FIXME: this is a temp disable, I'm not set on what's the correct way
    # pylint: disable=unused-argument
    # Callback methods
    # ================
    # The methods below are just placeholders that double as documentation.
    def on_doing(self, msg: gm.Doing):
        """A `DOING` request was received for resource `name`"""
        return self.on_unhandled(b'DOING')

    def on_done(self, msg: gm.Done):
        """A `DONE` request was received for resource `name`"""
        return self.on_unhandled(b'DONE')

    def on_exit(self, msg: gm.Exit):
        """An `EXIT` message was received"""
        return self.on_unhandled(b'EXIT')

    def on_exited(self, msg: gm.Exited):
        """An `EXITED` message was received"""
        return self.on_unhandled(b'EXITED')

    def on_failed(self, msg: gm.Failed):
        """A `FAILED` message was received"""
        return self.on_unhandled(b'FAILED')

    def on_get(self, msg: gm.Get):
        """A `GET` request was received for resource `name`"""
        return self.on_unhandled(b'GET')

    def on_illegal(self, msg: gm.Illegal):
        """An `ILLEGAL` message was received"""
        return self.on_unhandled(b'ILLEGAL')

    def on_not_found(self, msg: gm.NotFound):
        """A `NOTFOUND` message was received"""
        return self.on_unhandled(b'NOTFOUND')

    def on_put(self, msg: gm.Put):
        """A `PUT` message was received for resource `name`
        """
        return self.on_unhandled(b'PUT')

    def on_ready(self, msg: gm.Ready):
        """A `READY` message was received"""
        return self.on_unhandled(b'READY')

    def on_submit(self, msg: gm.Submit):
        """A `SUBMIT` message was received"""
        return self.on_unhandled(b'SUBMIT')

    def on_found(self, msg: gm.Found):
        """A `FOUND` message was received"""
        return self.on_unhandled(b'FOUND')

    def on_stat(self, msg: gm.Stat):
        """A `STAT` message was received"""
        return self.on_unhandled(b'STAT')

    def on_unhandled(self, verb: bytes):
        """
        A message without an overriden callback was received.
        """
        logging.error("Unhandled GALP verb %s", verb.decode('ascii'))

    # Default handlers
    # ================
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
        if isinstance(msg, gm.BaseMessage):
            plain = self._dump_message(msg)
        else:
            plain = msg
        return super().write_message(plain)

    # Main logic methods
    # ==================
    def on_verb(self, route, msg_body: list[bytes]):
        """Parse given message, calling callbacks as needed.

        Returns:
            Whatever the final handler for this message returned.
        """
        str_verb = str(msg_body[0], 'ascii')
        try:
            return self.handler(str_verb)(route, msg_body)
        except NoHandlerError:
            self._validate(False, route, f'No such verb "{str_verb}"')
        return None


    # Internal message handling methods
    event = EventNamespace()

    def handler(self, event_name: str):
        """Shortcut to call handler methods"""
        def _handler(*args, **kwargs):
            return self.event.handler(event_name)(self, *args, **kwargs)
        return _handler

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

    @event.on('EXIT')
    def _on_exit(self, route, msg):
        return self.on_exit(self._load_message(gm.Exit, route, msg))

    @event.on('EXITED')
    def _on_exited(self, route, msg):
        return self.on_exited(self._load_message(gm.Exited, route, msg))

    @event.on('ILLEGAL')
    def _on_illegal(self, route, msg):
        return self.on_illegal(self._load_message(gm.Illegal, route, msg))

    @event.on('GET')
    def _on_get(self, route, msg):
        return self.on_get(self._load_message(gm.Get, route, msg))

    @event.on('DOING')
    def _on_doing(self, route, msg):
        return self.on_doing(self._load_message(gm.Doing, route, msg))

    @event.on('DONE')
    def _on_done(self, route, msg):
        return self.on_done(self._load_message(gm.Done, route, msg))

    @event.on('FAILED')
    def _on_failed(self, route, msg: list[bytes]):
        return self.on_failed(self._load_message(gm.Failed, route, msg))

    @event.on('NOTFOUND')
    def _on_not_found(self, route, msg):
        return self.on_not_found(self._load_message(gm.NotFound, route, msg))

    @event.on('PUT')
    def _on_put(self, route, msg):
        return self.on_put(self._load_message(gm.Put, route, msg))

    @event.on('READY')
    def _on_ready(self, route, msg: list[bytes]):
        return self.on_ready(self._load_message(gm.Ready, route, msg))

    @event.on('SUBMIT')
    def _on_submit(self, route, msg: list[bytes]):
        return self.on_submit(self._load_message(gm.Submit, route, msg))

    @event.on('FOUND')
    def _on_found(self, route, msg):
        return self.on_found(self._load_message(gm.Found, route, msg))

    @event.on('STAT')
    def _on_stat(self, route, msg):
        return self.on_stat(self._load_message(gm.Stat, route, msg))
