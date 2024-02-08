"""
Request-reply pattern with multiplexing
"""

import logging
from typing import Callable, TypeAlias

from galp.protocol import Handler, TransportMessage
from galp.net.core.types import Reply, ReplyValue, Message
from galp.net.core.dump import Writer
from galp.commands import Script, PrimitiveProxy, InertCommand

ReplyHandler: TypeAlias = Callable[[PrimitiveProxy, ReplyValue], list[InertCommand]]

def handle_reply(msg: Reply, script: Script, handlers: dict[str, ReplyHandler]
        ) -> list[InertCommand]:
    """
    Handle a reply by fulfilling promise and calling callbacks, return new
    promises
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
        return handlers[verb](command, msg.value)
    logging.error('No handler for reply to %s', verb)
    return []

def make_reply_handler(script: Script, handlers: dict[str, ReplyHandler],
        handle_new: Callable[[Writer[Message], list[InertCommand]], list[TransportMessage]]):
    """
    Make handler for all replies
    """
    def _on_reply(write: Writer[Message], msg: Reply) -> list[TransportMessage]:
        news = handle_reply(msg, script, handlers)
        return handle_new(write, news)
    return Handler(Reply, _on_reply)
