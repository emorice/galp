"""
Request-reply pattern with multiplexing
"""

import logging
from dataclasses import dataclass
from typing import Callable, TypeAlias, TypeVar

from galp.protocol import (DispatchFunction, Handler, TransportMessage,
    UpperSession, HandlerFunction)
from galp.messages import Reply, ReplyValue, Message
from galp.commands import Script, PrimitiveProxy

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


ReplyHandler: TypeAlias = Callable[[PrimitiveProxy, ReplyValue], list[TransportMessage]]

def make_reply_handler(script: Script, dispatch: DispatchFunction | None,
        handlers: dict[str, ReplyHandler] | None = None):
    """
    Make handler for all replies
    """
    def _on_reply(session: UpperSession, msg: Reply) -> list[TransportMessage]:
        """
        Handle a reply
        """
        # Here will be the code to generally extract the promise
        verb = msg.request.upper()
        promise_id = (verb, msg.value.name)
        command = script.commands.get(promise_id)
        if not command:
            logging.error('Dropping answer to missing promise %s', promise_id)
            return []
        # Try dispatch based on request verb:
        if handlers and verb in handlers:
            return handlers[verb](command, msg.value)
        # Fallback to legacy value-type dispatching
        if dispatch:
            return dispatch(session, msg.value)
    return Handler(Reply, _on_reply)
