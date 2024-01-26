"""
Request-reply pattern with multiplexing
"""

import logging
from typing import Callable, TypeAlias, TypeVar

from galp.protocol import (Handler, TransportMessage,
    UpperSession, HandlerFunction)
from galp.net.core.types import Reply, ReplyValue, Request
from galp.net.core.dump import add_request_id, Writer
from galp.commands import Script, PrimitiveProxy, InertCommand

M = TypeVar('M', bound=Request)

RequestHandler: TypeAlias = Callable[[Writer[ReplyValue], M], list[TransportMessage]]

def make_request_handler(handler: RequestHandler[M]) -> HandlerFunction:
    """
    Chain a handler returning a galp.Message with message writing back to
    original sender
    """
    def on_message(session: UpperSession, msg: M) -> list[TransportMessage]:
        return handler(add_request_id(session.write, msg), msg)
    return on_message


ReplyHandler: TypeAlias = Callable[[PrimitiveProxy, ReplyValue], list[InertCommand]]

def make_reply_handler(script: Script, handlers: dict[str, ReplyHandler],
        handle_new: Callable[[UpperSession, list[InertCommand]], list[TransportMessage]]):
    """
    Make handler for all replies
    """
    def _on_reply(session: UpperSession, msg: Reply) -> list[TransportMessage]:
        """
        Handle a reply
        """
        # Extract the promise
        verb = msg.request.verb.decode('ascii').upper()
        promise_id = (verb, msg.request.name)
        command = script.commands.get(promise_id)
        if not command:
            logging.error('Dropping answer to missing promise %s', promise_id)
            return []
        # Try dispatch based on request verb:
        if verb in handlers:
            news = handlers[verb](command, msg.value)
            return handle_new(session, news)
        logging.error('No handler for reply to %s', verb)
        return []
    return Handler(Reply, _on_reply)
