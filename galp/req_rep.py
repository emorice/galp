"""
Request-reply pattern with multiplexing
"""

import logging
from dataclasses import dataclass
from typing import Callable, TypeAlias, TypeVar

from galp.protocol import (DispatchFunction, Handler, TransportMessage,
    UpperSession, HandlerFunction)
from galp.messages import Reply, ReplyValue, Message
from galp.commands import Script, PrimitiveProxy, InertCommand

@dataclass
class ReplySession:
    """
    Add request info to reply
    """
    lower: UpperSession
    request: str

    def write(self, value: ReplyValue) -> TransportMessage:
        """
        Wrap and write message
        """
        return self.lower.write(Reply(
            request=self.request,
            value=value
            ))

M = TypeVar('M', bound=Message)

RequestHandler: TypeAlias = Callable[[ReplySession, M], list[TransportMessage]]

def make_request_handler(handler: RequestHandler[M]) -> HandlerFunction:
    """
    Chain a handler returning a galp.Message with message writing back to
    original sender
    """
    def on_message(session: UpperSession, msg: M) -> list[TransportMessage]:
        return handler(ReplySession(session, msg.verb), msg)
    return on_message


ReplyHandler: TypeAlias = Callable[[PrimitiveProxy, ReplyValue], list[InertCommand]]

def make_reply_handler(script: Script, dispatch: DispatchFunction | None,
        handlers: dict[str, ReplyHandler] | None = None,
        handle_new: Callable[[UpperSession, list[InertCommand]], list[TransportMessage]] | None = None
        ):
    """
    Make handler for all replies
    """
    def _on_reply(session: UpperSession, msg: Reply) -> list[TransportMessage]:
        """
        Handle a reply
        """
        # Extract the promise
        verb = msg.request.upper()
        promise_id = (verb, msg.value.name)
        command = script.commands.get(promise_id)
        if not command:
            logging.error('Dropping answer to missing promise %s', promise_id)
            return []
        # Try dispatch based on request verb:
        if handlers and verb in handlers:
            news = handlers[verb](command, msg.value)
            if not handle_new:
                raise TypeError('No handler for new primitives !')
            return handle_new(session, news)
        # Fallback to legacy value-type dispatching
        if dispatch:
            return dispatch(session, msg.value)
        raise TypeError('No handlers')
    return Handler(Reply, _on_reply)
